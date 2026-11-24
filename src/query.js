import { buildWhereClause } from "./parser.js";
import { assertSchemaReady, hydrateRow } from "./schema.js";
import { resolveExecutionTarget } from "./transaction.js";

export async function executeGet(parsed, filters, directives, emitEvent) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);

  if (parsed.pk !== null && parsed.subResource) {
    return executeSubResourceGet(tableDef, parsed.pk, parsed.subResource, filters, directives, emitEvent);
  }

  if (directives.$with && hasExplicitSqlJoin(tableDef, directives.$with)) {
    return executeSqlJoinedGet(tableDef, parsed.pk, filters, directives, conn, runner, emitEvent);
  }

  const effectiveFilters = applySoftDeleteFilter(tableDef, { ...filters }, directives.$trash);
  if (parsed.pk !== null) {
    effectiveFilters[tableDef.primaryKey] = parsed.pk;
  }

  if (directives.$cursor !== undefined && directives.$cursor !== null) {
    effectiveFilters[tableDef.primaryKey] = { gt: directives.$cursor };
  }

  if (directives.$group || hasAggregationDirective(directives)) {
    return executeAggregationGet(tableDef, effectiveFilters, directives, conn, runner, emitEvent);
  }

  const { sql, params, countSql, countParams } = compileSelectSql(tableDef, effectiveFilters, directives, conn);
  const start = performance.now();
  const rawRows = await runner.all(sql, params);
  emitEvent("query", { action: `get:${parsed.table}`, sql, params, durationMs: performance.now() - start });

  let rows = rawRows.map((r) => hydrateRow(tableDef, r));

  if (directives.$with && rows.length > 0) {
    rows = await attachNestedRelations(tableDef, rows, directives.$with, directives, conn, runner, emitEvent);
  }

  if (parsed.pk !== null) {
    return rows[0] ?? null;
  }

  if (directives.$page !== undefined) {
    const page = Math.max(1, Number(directives.$page));
    const limit = Math.max(1, Number(directives.$limit ?? 20));
    const countRow = await runner.get(countSql, countParams);
    const total = Number(countRow?.total ?? 0);
    return {
      data: rows,
      meta: {
        page,
        limit,
        total,
        totalPages: Math.ceil(total / limit)
      }
    };
  }

  return rows;
}

function compileSelectSql(tableDef, filters, directives, conn) {
  const { dialect, schema } = conn;
  const q = dialect.quoteId;

  const selectCols = Array.isArray(directives.$fields) && directives.$fields.length > 0
    ? directives.$fields.map(q).join(", ")
    : "*";

  const where = buildWhereClause(filters, schema, tableDef.name, dialect);
  const whereSql = where.sql ? ` WHERE ${where.sql}` : "";

  let orderSql = "";
  if (directives.$sort) {
    const parts = String(directives.$sort).split(",").map((s) => {
      const [col, dir = "asc"] = s.trim().split(":");
      const normDir = dir.toLowerCase();
      if (normDir !== "asc" && normDir !== "desc") {
        throw new Error(`Arah pengurutan tidak valid: "${dir}". Gunakan "asc" atau "desc".`);
      }
      return `${q(col)} ${normDir.toUpperCase()}`;
    });
    orderSql = ` ORDER BY ${parts.join(", ")}`;
  } else if (directives.$cursor !== undefined) {
    orderSql = ` ORDER BY ${q(tableDef.primaryKey)} ASC`;
  }

  let limitSql = "";
  if (directives.$page !== undefined) {
    const page = Math.max(1, Number(directives.$page));
    const limit = Math.max(1, Number(directives.$limit ?? 20));
    const offset = (page - 1) * limit;
    limitSql = ` LIMIT ${limit} OFFSET ${offset}`;
  } else {
    if (directives.$limit !== undefined) limitSql += ` LIMIT ${Number(directives.$limit)}`;
    if (directives.$offset !== undefined) limitSql += ` OFFSET ${Number(directives.$offset)}`;
  }

  const lockSql = dialect.forUpdateSql(Boolean(directives.$lock));
  const sql = `SELECT ${selectCols} FROM ${q(tableDef.name)}${whereSql}${orderSql}${limitSql}${lockSql};`;
  const countSql = `SELECT COUNT(*) AS total FROM ${q(tableDef.name)}${whereSql};`;

  return { sql, params: where.params, countSql, countParams: where.params };
}

async function executeAggregationGet(tableDef, filters, directives, conn, runner, emitEvent) {
  const { dialect, schema } = conn;
  const q = dialect.quoteId;
  const selectParts = [];
  const groupCols = directives.$group
    ? (Array.isArray(directives.$group) ? directives.$group : [directives.$group])
    : [];

  for (const col of groupCols) {
    selectParts.push(q(col));
  }

  const aggMap = { $count: "COUNT", $sum: "SUM", $avg: "AVG", $min: "MIN", $max: "MAX" };
  for (const [dirKey, sqlFn] of Object.entries(aggMap)) {
    if (directives[dirKey]) {
      const cols = Array.isArray(directives[dirKey]) ? directives[dirKey] : [directives[dirKey]];
      for (const col of cols) {
        const alias = `${dirKey.slice(1)}_${col}`;
        selectParts.push(`${sqlFn}(${q(col)}) AS ${q(alias)}`);
      }
    }
  }

  const where = buildWhereClause(filters, schema, tableDef.name, dialect);
  const whereSql = where.sql ? ` WHERE ${where.sql}` : "";
  const groupSql = groupCols.length > 0 ? ` GROUP BY ${groupCols.map(q).join(", ")}` : "";
  const sql = `SELECT ${selectParts.join(", ")} FROM ${q(tableDef.name)}${whereSql}${groupSql};`;

  const start = performance.now();
  const rows = await runner.all(sql, where.params);
  emitEvent("query", { action: `get:${tableDef.name}:agg`, sql, params: where.params, durationMs: performance.now() - start });
  return rows;
}

function hasAggregationDirective(directives) {
  return ["$count", "$sum", "$avg", "$min", "$max"].some((k) => directives[k] !== undefined);
}

function hasExplicitSqlJoin(tableDef, withObj) {
  for (const [relName, cfg] of Object.entries(withObj)) {
    const rel = tableDef.relations.get(relName);
    if (!rel) return true;
    if (cfg && typeof cfg === "object") {
      if (cfg.$join && cfg.$join !== "left") return true;
      if (cfg.$on || cfg.$flat) return true;
    }
  }
  return false;
}

async function executeSqlJoinedGet(tableDef, pk, filters, directives, conn, runner, emitEvent) {
  const { dialect, schema } = conn;
  const q = dialect.quoteId;
  const baseAlias = "_base";
  const effectiveFilters = applySoftDeleteFilter(tableDef, { ...filters }, directives.$trash);
  if (pk !== null) effectiveFilters[tableDef.primaryKey] = pk;

  const selectCols = [];
  const baseFields = Array.isArray(directives.$fields)
    ? directives.$fields
    : [...tableDef.columns.keys()];

  for (const col of baseFields) {
    selectCols.push(`${q(baseAlias)}.${q(col)} AS ${q(`__base__${col}`)}`);
  }

  const joinClauses = [];
  const joinPlans = [];

  for (const [relName, rawCfg] of Object.entries(directives.$with)) {
    const cfg = rawCfg === true ? {} : rawCfg;
    const rel = tableDef.relations.get(relName);
    const targetTable = rel ? rel.targetTable : relName;
    const targetDef = assertSchemaReady(conn, targetTable);
    const relAlias = `_rel_${relName}`;

    const joinKeyword = mapJoinType(cfg.$join ?? "left");
    let onClause = "";

    if (joinKeyword !== "CROSS JOIN") {
      if (Array.isArray(cfg.$on) && cfg.$on.length === 2) {
        onClause = ` ON ${q(baseAlias)}.${q(cfg.$on[0])} = ${q(relAlias)}.${q(cfg.$on[1])}`;
      } else if (rel && (rel.kind === "many-to-one" || rel.kind === "one-to-one")) {
        onClause = ` ON ${q(baseAlias)}.${q(rel.localKey)} = ${q(relAlias)}.${q(rel.targetKey)}`;
      } else if (rel && (rel.kind === "one-to-many-owned" || rel.kind === "one-to-many-reverse")) {
        onClause = ` ON ${q(baseAlias)}.${q(rel.localKey)} = ${q(relAlias)}.${q(rel.foreignKey)}`;
      } else {
        throw new Error(`Tidak dapat menentukan kunci JOIN untuk "${relName}". Gunakan direktif $on: ["kolomAsal", "kolomTujuan"].`);
      }
    }

    joinClauses.push(`${joinKeyword} ${q(targetTable)} ${q(relAlias)}${onClause}`);

    const targetFields = Array.isArray(cfg.$fields) ? cfg.$fields : [...targetDef.columns.keys()];
    for (const col of targetFields) {
      selectCols.push(`${q(relAlias)}.${q(col)} AS ${q(`__${relName}__${col}`)}`);
    }

    joinPlans.push({ relName, targetDef, targetFields, flat: Boolean(cfg.$flat) });
  }

  const where = buildWhereClause(effectiveFilters, schema, tableDef.name, dialect, baseAlias);
  const whereSql = where.sql ? ` WHERE ${where.sql}` : "";
  const sql = `SELECT ${selectCols.join(", ")} FROM ${q(tableDef.name)} ${q(baseAlias)} ${joinClauses.join(" ")}${whereSql};`;

  const start = performance.now();
  const rawRows = await runner.all(sql, where.params);
  emitEvent("query", { action: `get:${tableDef.name}:join`, sql, params: where.params, durationMs: performance.now() - start });

  const results = rawRows.map((row) => {
    const rawBase = {};
    for (const col of baseFields) {
      rawBase[col] = row[`__base__${col}`];
    }
    const hydratedBase = hydrateRow(tableDef, rawBase);

    for (const plan of joinPlans) {
      const rawChild = {};
      let hasNonNull = false;
      for (const col of plan.targetFields) {
        const val = row[`__${plan.relName}__${col}`];
        if (val !== null && val !== undefined) hasNonNull = true;
        rawChild[col] = val;
      }

      if (plan.flat) {
        if (hasNonNull) {
          const hydratedChild = hydrateRow(plan.targetDef, rawChild);
          for (const [k, v] of Object.entries(hydratedChild)) {
            if (!(k in hydratedBase)) hydratedBase[k] = v;
          }
        }
      } else {
        hydratedBase[plan.relName] = hasNonNull ? hydrateRow(plan.targetDef, rawChild) : null;
      }
    }

    return hydratedBase;
  });

  return pk !== null ? (results[0] ?? null) : results;
}

function mapJoinType(type) {
  switch (String(type).toLowerCase()) {
    case "inner":
      return "INNER JOIN";
    case "right":
      return "RIGHT JOIN";
    case "full":
      return "FULL OUTER JOIN";
    case "cross":
      return "CROSS JOIN";
    case "left":
    default:
      return "LEFT JOIN";
  }
}

async function attachNestedRelations(tableDef, parentRows, withObj, directives, conn, runner, emitEvent) {
  const { dialect, schema } = conn;
  const q = dialect.quoteId;
  let currentRows = parentRows;

  for (const [relName, rawCfg] of Object.entries(withObj)) {
    const cfg = rawCfg === true ? {} : rawCfg;
    const rel = tableDef.relations.get(relName);
    if (!rel) {
      throw new Error(`Relasi "${relName}" tidak ditemukan pada tabel "${tableDef.name}".`);
    }

    const { $fields, $required, $join, ...nestedFiltersAndWith } = cfg;
    const nestedWith = {};
    const childFilters = {};
    for (const [k, v] of Object.entries(nestedFiltersAndWith)) {
      if (k === "$with") {
        Object.assign(nestedWith, v);
      } else if (!k.startsWith("$") && schema.tables.get(rel.targetTable)?.relations.has(k) && typeof v === "object" && v !== null) {
        nestedWith[k] = v;
      } else if (!k.startsWith("$")) {
        childFilters[k] = v;
      }
    }

    const isRequired = Boolean($required || $join === "inner");

    if (rel.kind === "many-to-one" || rel.kind === "one-to-one") {
      const targetDef = assertSchemaReady(conn, rel.targetTable);
      const fkValues = [...new Set(currentRows.map((r) => r[rel.localKey]).filter((v) => v !== null && v !== undefined))];

      if (fkValues.length === 0) {
        if (isRequired) return [];
        for (const r of currentRows) r[relName] = null;
        continue;
      }

      const queryFilters = applySoftDeleteFilter(targetDef, {
        ...childFilters,
        [rel.targetKey]: fkValues
      });
      const { sql, params } = compileSelectSql(targetDef, queryFilters, { $fields }, conn);
      let relatedRows = (await runner.all(sql, params)).map((r) => hydrateRow(targetDef, r));

      if (Object.keys(nestedWith).length > 0 && relatedRows.length > 0) {
        relatedRows = await attachNestedRelations(targetDef, relatedRows, nestedWith, directives, conn, runner, emitEvent);
      }

      const byKey = new Map(relatedRows.map((r) => [r[rel.targetKey], r]));
      currentRows = currentRows.filter((row) => {
        const match = byKey.get(row[rel.localKey]) ?? null;
        row[relName] = match;
        return !isRequired || match !== null;
      });
    } else if (rel.kind === "one-to-many-owned" || rel.kind === "one-to-many-reverse") {
      const targetDef = assertSchemaReady(conn, rel.targetTable);
      const pkValues = [...new Set(currentRows.map((r) => r[rel.localKey]))];

      const queryFilters = applySoftDeleteFilter(targetDef, {
        ...childFilters,
        [rel.foreignKey]: pkValues
      });
      const fieldsWithFk = Array.isArray($fields) ? [...new Set([...$fields, rel.foreignKey])] : undefined;
      const { sql, params } = compileSelectSql(targetDef, queryFilters, { $fields: fieldsWithFk }, conn);
      let childRows = (await runner.all(sql, params)).map((r) => hydrateRow(targetDef, r));

      if (Object.keys(nestedWith).length > 0 && childRows.length > 0) {
        childRows = await attachNestedRelations(targetDef, childRows, nestedWith, directives, conn, runner, emitEvent);
      }

      const grouped = new Map();
      for (const child of childRows) {
        const key = child[rel.foreignKey];
        if (!grouped.has(key)) grouped.set(key, []);
        if (Array.isArray($fields) && !$fields.includes(rel.foreignKey)) {
          delete child[rel.foreignKey];
        }
        grouped.get(key).push(child);
      }

      currentRows = currentRows.filter((row) => {
        const list = grouped.get(row[rel.localKey]) ?? [];
        row[relName] = list;
        return !isRequired || list.length > 0;
      });
    } else if (rel.kind === "many-to-many") {
      const targetDef = assertSchemaReady(conn, rel.targetTable);
      const pkValues = [...new Set(currentRows.map((r) => r[rel.localKey]))];
      const placeholders = pkValues.map((_, i) => dialect.param(i + 1)).join(", ");

      const junctionRows = await runner.all(
        `SELECT ${q(rel.junctionLocalKey)} AS _parent_id, ${q(rel.junctionTargetKey)} AS _target_id FROM ${q(rel.junctionTable)} WHERE ${q(rel.junctionLocalKey)} IN (${placeholders});`,
        pkValues.map((v) => dialect.serialize(v))
      );

      const targetIds = [...new Set(junctionRows.map((j) => j._target_id))];
      if (targetIds.length === 0) {
        if (isRequired) return [];
        for (const r of currentRows) r[relName] = [];
        continue;
      }

      const queryFilters = applySoftDeleteFilter(targetDef, {
        ...childFilters,
        [rel.targetKey]: targetIds
      });
      const { sql, params } = compileSelectSql(targetDef, queryFilters, { $fields }, conn);
      let targetRows = (await runner.all(sql, params)).map((r) => hydrateRow(targetDef, r));

      if (Object.keys(nestedWith).length > 0 && targetRows.length > 0) {
        targetRows = await attachNestedRelations(targetDef, targetRows, nestedWith, directives, conn, runner, emitEvent);
      }

      const targetById = new Map(targetRows.map((t) => [t[rel.targetKey], t]));
      const byParent = new Map();
      for (const j of junctionRows) {
        const item = targetById.get(j._target_id);
        if (!item) continue;
        if (!byParent.has(j._parent_id)) byParent.set(j._parent_id, []);
        byParent.get(j._parent_id).push(item);
      }

      currentRows = currentRows.filter((row) => {
        const list = byParent.get(row[rel.localKey]) ?? [];
        row[relName] = list;
        return !isRequired || list.length > 0;
      });
    }
  }

  return currentRows;
}

async function executeSubResourceGet(tableDef, pk, subResource, filters, directives, emitEvent) {
  const rel = tableDef.relations.get(subResource);
  if (!rel) {
    throw new Error(`Sub-relasi "${subResource}" tidak ditemukan pada tabel "${tableDef.name}".`);
  }

  if (rel.kind === "one-to-many-owned" || rel.kind === "one-to-many-reverse") {
    return executeGet(
      { action: "get", table: rel.targetTable, pk: null, subResource: null },
      { ...filters, [rel.foreignKey]: pk },
      directives,
      emitEvent
    );
  }

  if (rel.kind === "many-to-many") {
    const { conn, runner } = resolveExecutionTarget(directives);
    const q = conn.dialect.quoteId;
    const jRows = await runner.all(
      `SELECT ${q(rel.junctionTargetKey)} AS tid FROM ${q(rel.junctionTable)} WHERE ${q(rel.junctionLocalKey)} = ${conn.dialect.param(1)};`,
      [conn.dialect.serialize(pk)]
    );
    const targetIds = jRows.map((r) => r.tid);
    if (targetIds.length === 0) return [];
    return executeGet(
      { action: "get", table: rel.targetTable, pk: null, subResource: null },
      { ...filters, [rel.targetKey]: targetIds },
      directives,
      emitEvent
    );
  }

  const parent = await executeGet(
    { action: "get", table: tableDef.name, pk, subResource: null },
    {},
    { ...directives, $with: { [subResource]: true } },
    emitEvent
  );
  return parent ? parent[subResource] : null;
}

export function applySoftDeleteFilter(tableDef, filters, trashMode) {
  if (!tableDef.softDeletePatch) return filters;

  for (const [colName, deletedMarker] of Object.entries(tableDef.softDeletePatch)) {
    if (colName in filters) continue;
    const colDef = tableDef.columns.get(colName);
    const isNullMarker = colDef?.nullable && deletedMarker !== null;

    if (trashMode === "include") continue;
    if (trashMode === "only") {
      filters[colName] = isNullMarker ? { ne: null } : deletedMarker;
    } else {
      filters[colName] = isNullMarker ? null : (colDef?.defaultValue ?? false);
    }
  }

  return filters;
}

export async function executeExplain(parsed, filters, directives) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const effectiveFilters = applySoftDeleteFilter(tableDef, { ...filters }, directives.$trash);
  if (parsed.pk !== null) effectiveFilters[tableDef.primaryKey] = parsed.pk;

  const { sql, params } = compileSelectSql(tableDef, effectiveFilters, directives, conn);
  const planRows = await runner.all(conn.dialect.explainSql(sql), params);

  return { dialect: conn.dialect.name, sql, params, plan: planRows };
}

export async function executeDump(parsed, directives) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const rows = await runner.all(`SELECT * FROM ${conn.dialect.quoteId(tableDef.name)};`);
  return rows.map((r) => hydrateRow(tableDef, r));
}