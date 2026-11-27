import { buildWhereClause, splitDirectives } from "./parser.js";
import { assertSchemaReady, hydrateRow, validateAndPrepareRow } from "./schema.js";
import { beginTransaction, commitTransaction, resolveExecutionTarget, rollbackTransaction } from "./transaction.js";
import { applySoftDeleteFilter } from "./query.js";
import {
  DataOperationBlockedError,
  IMPACT_CAPTURE,
  validateContractChanges
} from "./contracts.js";
import { assertMutationLeaseAccess } from "./lease.js";
import {
  addAffectedRows,
  assertExpectedState,
  MAX_AFFECTED_STATE
} from "./operation-guards.js";

const DEFAULT_CHUNK_SIZE = 500;
const NESTED_CONTRACT_VALIDATION = Symbol("lumb.contract.nested");

export async function executePost(parsed, payload, directives, emitEvent) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const { dialect, schema } = conn;
  const q = dialect.quoteId;

  if (Array.isArray(payload)) {
    return executeBatchInsert(
      tableDef,
      payload,
      directives,
      conn,
      runner,
      emitEvent,
      directives.$timelineCapture
    );
  }

  const { ownedChildren, m2mValues } = extractNestedWriteData(tableDef, payload);
  const cleanRow = validateAndPrepareRow(tableDef, schema, payload, false);

  const cols = Object.keys(cleanRow);
  const vals = cols.map((c) => dialect.serialize(cleanRow[c]));
  const placeholders = cols.map((_, idx) => dialect.param(idx + 1)).join(", ");
  const hasAutoPk = tableDef.columns.get(tableDef.primaryKey)?.isAutoIncrement;

  const sql = dialect.insertSql(
    q(tableDef.name),
    cols.map(q).join(", "),
    placeholders,
    q(tableDef.primaryKey),
    Boolean(hasAutoPk)
  );
  const res = await runner.run(sql, vals);

  const pkValue = cleanRow[tableDef.primaryKey] !== undefined ? cleanRow[tableDef.primaryKey] : res.lastInsertId;
  const insertedRaw = await runner.get(
    `SELECT * FROM ${q(tableDef.name)} WHERE ${q(tableDef.primaryKey)} = ${dialect.param(1)};`,
    [dialect.serialize(pkValue)]
  );
  const hydrated = hydrateRow(tableDef, insertedRaw);

  for (const [fieldName, childList] of Object.entries(ownedChildren)) {
    const rel = tableDef.relations.get(fieldName);
    const savedChildren = [];
    for (const childItem of childList) {
      const saved = await executePost(
        { action: "post", table: rel.targetTable },
        { ...childItem, [rel.foreignKey]: pkValue },
        directives,
        emitEvent
      );
      savedChildren.push(saved);
      const childDef = assertSchemaReady(conn, rel.targetTable);
      const after = persistedColumns(childDef, saved);
      const nestedChange = {
        table: childDef.name,
        pk: after[childDef.primaryKey],
        operation: "insert",
        before: null,
        after,
        relationsBefore: {},
        relationsAfter: await relationValues(conn, runner, childDef, saved)
      };
      recordNestedImpact(directives, nestedChange);
      await validateContractChanges(conn, [nestedChange], "post");
    }
    hydrated[fieldName] = savedChildren;
  }

  for (const [fieldName, targetIds] of Object.entries(m2mValues)) {
    const rel = tableDef.relations.get(fieldName);
    const pivotSql = dialect.insertIgnorePivotSql(
      q(rel.junctionTable),
      q(rel.junctionLocalKey),
      q(rel.junctionTargetKey)
    );
    for (const tid of targetIds) {
      const resolvedTid = typeof tid === "object" && tid !== null ? tid[rel.targetKey] : tid;
      await runner.run(pivotSql, [dialect.serialize(pkValue), dialect.serialize(resolvedTid)]);
    }
    hydrated[fieldName] = targetIds;
  }

  emitEvent(`post:${tableDef.name}`, hydrated);
  return hydrated;
}

async function executeBatchInsert(tableDef, rows, directives, conn, runner, emitEvent, timelineCapture) {
  if (rows.length === 0) return { inserted: 0, total: 0 };

  const { dialect, schema } = conn;
  const q = dialect.quoteId;
  if (timelineCapture) {
    return executeTrackedBatchInsert(
      tableDef,
      rows,
      directives,
      conn,
      runner,
      emitEvent,
      timelineCapture
    );
  }
  const chunkSize = Math.max(1, Number(directives.$chunk ?? DEFAULT_CHUNK_SIZE));
  const conflictMode = directives.$onConflict;
  const preparedRows = rows.map((r) => validateAndPrepareRow(tableDef, schema, r, false));

  const cols = [...tableDef.columns.keys()].filter((col) =>
    preparedRows.some((row) => Object.prototype.hasOwnProperty.call(row, col))
  );
  const hasAutoPk = tableDef.columns.get(tableDef.primaryKey)?.isAutoIncrement;
  if (cols.length === 0 && hasAutoPk) cols.push(tableDef.primaryKey);
  const colListSql = cols.map(q).join(", ");
  const updateCols = cols.filter((c) => c !== tableDef.primaryKey);

  const runChunks = async (activeRunner) => {
    let totalInserted = 0;
    for (let i = 0; i < preparedRows.length; i += chunkSize) {
      const chunk = preparedRows.slice(i, i + chunkSize);
      const params = [];
      let paramIdx = 0;

      const rowPlaceholders = chunk.map((row) => {
        const singleRow = cols.map((c) => {
          if (row[c] === undefined && c === tableDef.primaryKey && hasAutoPk && dialect.name === "postgres") {
            return "DEFAULT";
          }
          paramIdx += 1;
          params.push(dialect.serialize(row[c]));
          return dialect.param(paramIdx);
        });
        return `(${singleRow.join(", ")})`;
      });

      const allPlaceholders = rowPlaceholders.join(", ");
      const sql = conflictMode
        ? dialect.upsertSql(q(tableDef.name), colListSql, allPlaceholders, q(tableDef.primaryKey), updateCols, conflictMode)
        : `INSERT INTO ${q(tableDef.name)} (${colListSql}) VALUES ${allPlaceholders};`;

      const res = await activeRunner.run(sql, params);
      totalInserted += res.changes;
    }
    return totalInserted;
  };

  let totalInserted = 0;
  if (!directives.$tx && preparedRows.length > chunkSize) {
    const autoTx = await beginTransaction(null, directives);
    try {
      const target = resolveExecutionTarget({ ...directives, $tx: autoTx });
      totalInserted = await runChunks(target.runner);
      await commitTransaction(autoTx);
    } catch (err) {
      await rollbackTransaction(autoTx);
      throw err;
    }
  } else {
    totalInserted = await runChunks(runner);
  }

  const summary = { inserted: totalInserted, total: rows.length };
  emitEvent(`post:${tableDef.name}`, summary);
  return summary;
}

async function executeTrackedBatchInsert(tableDef, rows, directives, conn, runner, emitEvent, timelineCapture) {
  const { dialect, schema } = conn;
  const q = dialect.quoteId;
  let inserted = 0;

  for (const input of rows) {
    const row = validateAndPrepareRow(tableDef, schema, input, false);
    const cols = Object.keys(row);
    const hasAutoPk = tableDef.columns.get(tableDef.primaryKey)?.isAutoIncrement;
    if (cols.length === 0 && hasAutoPk) cols.push(tableDef.primaryKey);
    const values = [];
    let parameterIndex = 0;
    const placeholders = cols.map((column) => {
      if (row[column] === undefined && column === tableDef.primaryKey && hasAutoPk && dialect.name === "postgres") {
        return "DEFAULT";
      }
      parameterIndex += 1;
      values.push(dialect.serialize(row[column]));
      return dialect.param(parameterIndex);
    }).join(", ");

    const sql = directives.$onConflict
      ? dialect.upsertSql(
        q(tableDef.name),
        cols.map(q).join(", "),
        `(${placeholders})`,
        q(tableDef.primaryKey),
        cols.filter((column) => column !== tableDef.primaryKey),
        directives.$onConflict
      )
      : dialect.insertSql(
        q(tableDef.name),
        cols.map(q).join(", "),
        placeholders,
        q(tableDef.primaryKey),
        Boolean(hasAutoPk)
      );

    const result = await runner.run(sql, values);
    inserted += result.changes;
    if (result.changes === 0) continue;

    let pkValue = row[tableDef.primaryKey] ?? result.lastInsertId;
    if (pkValue === undefined || pkValue === null) {
      const uniqueColumn = [...tableDef.columns.values()].find((column) =>
        column.unique && Object.prototype.hasOwnProperty.call(row, column.name)
      );
      if (uniqueColumn) {
        const found = await runner.get(
          `SELECT ${q(tableDef.primaryKey)} FROM ${q(tableDef.name)} WHERE ${q(uniqueColumn.name)} = ${dialect.param(1)};`,
          [dialect.serialize(row[uniqueColumn.name])]
        );
        pkValue = found?.[tableDef.primaryKey];
      }
    }

    if (pkValue !== undefined && pkValue !== null) {
      const saved = await runner.get(
        `SELECT * FROM ${q(tableDef.name)} WHERE ${q(tableDef.primaryKey)} = ${dialect.param(1)};`,
        [dialect.serialize(pkValue)]
      );
      if (saved) timelineCapture.rows.push(hydrateRow(tableDef, saved));
    }
  }

  const summary = { inserted, total: rows.length };
  emitEvent(`post:${tableDef.name}`, summary);
  return summary;
}

export async function executeSeed(parsed, payload, directives, emitEvent) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const { dialect, schema } = conn;
  const q = dialect.quoteId;

  const rows = Array.isArray(payload) ? payload : [payload];
  const syncMode = parsed.queryFilters?.sync === true;

  let matchKey = tableDef.primaryKey;
  if (tableDef.columns.get(matchKey)?.isAutoIncrement) {
    const uniqueCol = [...tableDef.columns.values()].find((c) => c.unique && !c.isPrimaryKey);
    if (uniqueCol) matchKey = uniqueCol.name;
  }

  const runSeedLoop = async (activeDirectives, activeRunner) => {
    let inserted = 0;
    let updated = 0;
    let skipped = 0;

    for (const rawItem of rows) {
      const prepared = validateAndPrepareRow(tableDef, schema, rawItem, false);
      const keyVal = prepared[matchKey];

      const existing = keyVal !== undefined && keyVal !== null
        ? await activeRunner.get(
          `SELECT * FROM ${q(tableDef.name)} WHERE ${q(matchKey)} = ${dialect.param(1)};`,
          [dialect.serialize(keyVal)]
        )
        : null;

      if (!existing) {
        await executePost({ action: "post", table: tableDef.name }, rawItem, activeDirectives, emitEvent);
        inserted++;
      } else if (syncMode) {
        await executePatch(
          { action: "patch", table: tableDef.name, pk: existing[tableDef.primaryKey] },
          rawItem,
          activeDirectives,
          emitEvent
        );
        updated++;
      } else {
        skipped++;
      }
    }

    return { seeded: true, table: tableDef.name, inserted, updated, skipped };
  };

  if (!directives.$tx && rows.length > 1) {
    const autoTx = await beginTransaction(null, directives);
    try {
      const txDirectives = { ...directives, $tx: autoTx };
      const target = resolveExecutionTarget(txDirectives);
      const summary = await runSeedLoop(txDirectives, target.runner);
      await commitTransaction(autoTx);
      return summary;
    } catch (err) {
      await rollbackTransaction(autoTx);
      throw err;
    }
  }

  return runSeedLoop(directives, runner);
}

export async function executePatch(parsed, payload, directives, emitEvent) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const { dialect, schema } = conn;
  const q = dialect.quoteId;

  if (Array.isArray(payload)) {
    const runBatchUpdate = async (activeDirectives) => {
      let updatedCount = 0;
      for (const item of payload) {
        const pkVal = item[tableDef.primaryKey];
        if (pkVal === undefined || pkVal === null) {
          throw new Error(`Setiap objek pada batch patch "${tableDef.name}" wajib menyertakan "${tableDef.primaryKey}".`);
        }
        const { data: rowChanges, directives: itemDir } = splitDirectives(item);
        delete rowChanges[tableDef.primaryKey];
        const res = await executePatch(
          { action: "patch", table: tableDef.name, pk: pkVal },
          rowChanges,
          { ...activeDirectives, ...itemDir },
          emitEvent
        );
        if (res) updatedCount++;
      }
      return { updated: updatedCount };
    };

    if (!directives.$tx && payload.length > 1) {
      const autoTx = await beginTransaction(null, directives);
      try {
        const summary = await runBatchUpdate({ ...directives, $tx: autoTx });
        await commitTransaction(autoTx);
        return summary;
      } catch (err) {
        await rollbackTransaction(autoTx);
        throw err;
      }
    }
    return runBatchUpdate(directives);
  }

  let targetFilters;
  if (parsed.pk !== null) {
    targetFilters = { ...(directives.$where ?? {}), [tableDef.primaryKey]: parsed.pk };
  } else if (directives.$where && typeof directives.$where === "object") {
    targetFilters = { ...directives.$where };
  } else {
    throw new Error(`Perintah "patch:${tableDef.name}" tanpa ID di URL wajib menyertakan direktif "$where".`);
  }

  if (directives.$expect !== undefined) {
    const expectedWhere = buildWhereClause(targetFilters, schema, tableDef.name, dialect);
    const expectedRows = await runner.all(
      `SELECT * FROM ${q(tableDef.name)}${expectedWhere.sql ? ` WHERE ${expectedWhere.sql}` : ""}` +
      `${dialect.forUpdateSql(true)};`,
      expectedWhere.params
    );
    assertExpectedState(
      tableDef,
      expectedRows.map((row) => hydrateRow(tableDef, row)),
      directives.$expect
    );
  }

  const { m2mValues } = extractNestedWriteData(tableDef, payload);
  const prepared = validateAndPrepareRow(tableDef, schema, payload, true);
  const setClauses = [];
  const setParams = [];
  const paramCounter = { count: 0 };

  const pushSetParam = (rawVal) => {
    paramCounter.count += 1;
    setParams.push(dialect.serialize(rawVal));
    return dialect.param(paramCounter.count);
  };

  for (const [col, val] of Object.entries(prepared)) {
    if (val && typeof val === "object" && !(val instanceof Date)) {
      if ("increment" in val) {
        const step = Number(val.increment);
        if (!Number.isFinite(step)) throw new Error(`Nilai increment pada "${col}" harus berupa angka.`);
        setClauses.push(`${q(col)} = ${q(col)} + ${pushSetParam(step)}`);
        continue;
      }
      if ("decrement" in val) {
        const step = Number(val.decrement);
        if (!Number.isFinite(step)) throw new Error(`Nilai decrement pada "${col}" harus berupa angka.`);
        setClauses.push(`${q(col)} = ${q(col)} - ${pushSetParam(step)}`);
        continue;
      }
    }
    setClauses.push(`${q(col)} = ${pushSetParam(val)}`);
  }

  const where = buildWhereClause(targetFilters, schema, tableDef.name, dialect, null, paramCounter);
  const whereSql = where.sql ? ` WHERE ${where.sql}` : "";

  if (setClauses.length > 0) {
    const sql = `UPDATE ${q(tableDef.name)} SET ${setClauses.join(", ")}${whereSql};`;
    const res = await runner.run(sql, [...setParams, ...where.params]);

    if (parsed.pk === null) {
      const summary = { updated: res.changes };
      emitEvent(`patch:${tableDef.name}`, summary);
      return summary;
    }
  }

  if (parsed.pk !== null) {
    for (const [fieldName, targetIds] of Object.entries(m2mValues)) {
      const rel = tableDef.relations.get(fieldName);
      await runner.run(
        `DELETE FROM ${q(rel.junctionTable)} WHERE ${q(rel.junctionLocalKey)} = ${dialect.param(1)};`,
        [dialect.serialize(parsed.pk)]
      );
      const pivotSql = dialect.insertIgnorePivotSql(
        q(rel.junctionTable),
        q(rel.junctionLocalKey),
        q(rel.junctionTargetKey)
      );
      for (const tid of targetIds) {
        const resolvedTid = typeof tid === "object" && tid !== null ? tid[rel.targetKey] : tid;
        await runner.run(pivotSql, [dialect.serialize(parsed.pk), dialect.serialize(resolvedTid)]);
      }
    }

    const checkWhere = buildWhereClause(targetFilters, schema, tableDef.name, dialect);
    const updatedRow = await runner.get(
      `SELECT * FROM ${q(tableDef.name)} WHERE ${checkWhere.sql};`,
      checkWhere.params
    );
    const hydrated = hydrateRow(tableDef, updatedRow);
    if (hydrated) emitEvent(`patch:${tableDef.name}`, hydrated);
    return hydrated;
  }

  return { updated: 0 };
}

export async function executeDelete(parsed, payload, directives, emitEvent) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const { dialect, schema } = conn;
  const q = dialect.quoteId;

  let targetFilters;
  if (parsed.pk !== null) {
    targetFilters = { ...(directives.$where ?? {}), [tableDef.primaryKey]: parsed.pk };
  } else if (Array.isArray(payload)) {
    targetFilters = { ...(directives.$where ?? {}), [tableDef.primaryKey]: payload };
  } else if (directives.$where && typeof directives.$where === "object") {
    targetFilters = { ...directives.$where };
  } else if (payload && typeof payload === "object" && Object.keys(payload).length > 0) {
    targetFilters = { ...payload };
  } else {
    throw new Error(`Perintah "delete:${tableDef.name}" tanpa ID wajib menyertakan filter atau "$where".`);
  }

  const effectiveFilters = applySoftDeleteFilter(tableDef, { ...targetFilters }, directives.$trash);
  const where = buildWhereClause(effectiveFilters, schema, tableDef.name, dialect);
  const whereSql = where.sql ? ` WHERE ${where.sql}` : "";

  const targetRows = await runner.all(
    `SELECT * FROM ${q(tableDef.name)}${whereSql}${dialect.forUpdateSql(true)};`,
    where.params
  );

  if (targetRows.length === 0) {
    return { deleted: 0 };
  }
  await assertMutationLeaseAccess(conn, runner, tableDef, targetRows, directives);
  if (directives[NESTED_CONTRACT_VALIDATION]) {
    addAffectedRows(
      directives[MAX_AFFECTED_STATE],
      targetRows.length,
      tableDef,
      targetRows
    );
  }
  if (directives.$expect !== undefined) {
    assertExpectedState(
      tableDef,
      targetRows.map((row) => hydrateRow(tableDef, row)),
      directives.$expect
    );
  }

  const targetIds = targetRows.map((r) => r[tableDef.primaryKey]);

  for (const rel of tableDef.relations.values()) {
    if (rel.kind === "one-to-many-owned") {
      await executeDelete(
        { action: "delete", table: rel.targetTable, pk: null },
        {},
        {
          ...directives,
          $where: { [rel.foreignKey]: targetIds },
          $hard: true,
          $expect: undefined,
          [NESTED_CONTRACT_VALIDATION]: true
        },
        emitEvent
      );
      continue;
    }

    if (rel.kind === "one-to-many-reverse") {
      const childDef = schema.tables.get(rel.targetTable);
      if (!childDef) continue;

      const childWhere = buildWhereClause(
        applySoftDeleteFilter(childDef, { [rel.foreignKey]: targetIds }),
        schema,
        childDef.name,
        dialect
      );
      const refCountRow = await runner.get(
        `SELECT COUNT(*) AS cnt FROM ${q(childDef.name)} WHERE ${childWhere.sql};`,
        childWhere.params
      );

      if (Number(refCountRow?.cnt ?? 0) > 0) {
        if (directives.$cascade) {
          await executeDelete(
            { action: "delete", table: childDef.name, pk: null },
            {},
            {
              ...directives,
              $where: { [rel.foreignKey]: targetIds },
              $expect: undefined,
              [NESTED_CONTRACT_VALIDATION]: true
            },
            emitEvent
          );
        } else {
          const message =
            `Data pada "${tableDef.name}" tidak dapat dihapus karena masih digunakan oleh tabel "${childDef.name}". Gunakan { $cascade: true } untuk menghapus berantai.`;
          throw new DataOperationBlockedError([{
            type: "relation",
            table: tableDef.name,
            relatedTable: childDef.name,
            message
          }]);
        }
      }
    }
  }

  if (tableDef.softDeletePatch && !directives.$hard) {
    const liveChanges = {};
    const proxy = new Proxy(
      {},
      {
        set(_, prop, value) {
          liveChanges[prop] = value;
          return true;
        }
      }
    );
    tableDef.removeFn.call(proxy);
    if (directives[NESTED_CONTRACT_VALIDATION]) {
      const relationStates = await Promise.all(targetRows.map(async (raw) => {
        const row = hydrateRow(tableDef, raw);
        return relationValues(conn, runner, tableDef, row);
      }));
      const contractChanges = targetRows.map((raw, index) => {
        const before = hydrateRow(tableDef, raw);
        const after = { ...before, ...liveChanges };
        const relationState = relationStates[index];
        return {
          table: tableDef.name,
          pk: before[tableDef.primaryKey],
          operation: "delete",
          before: persistedColumns(tableDef, before),
          after: persistedColumns(tableDef, after),
          relationsBefore: relationState,
          relationsAfter: relationState
        };
      });
      recordNestedImpact(directives, contractChanges);
      await validateContractChanges(conn, contractChanges, "delete");
    }

    const setParts = [];
    const setParams = [];
    const paramCounter = { count: 0 };
    for (const [col, val] of Object.entries(liveChanges)) {
      paramCounter.count += 1;
      setParts.push(`${q(col)} = ${dialect.param(paramCounter.count)}`);
      setParams.push(dialect.serialize(val));
    }

    const updateWhere = buildWhereClause(effectiveFilters, schema, tableDef.name, dialect, null, paramCounter);
    const sql = `UPDATE ${q(tableDef.name)} SET ${setParts.join(", ")} WHERE ${updateWhere.sql};`;
    const res = await runner.run(sql, [...setParams, ...updateWhere.params]);
    const summary = { deleted: res.changes, softDeleted: true };
    emitEvent(`delete:${tableDef.name}`, summary);
    return summary;
  }

  let nestedRelationValues = [];
  if (directives[NESTED_CONTRACT_VALIDATION]) {
    nestedRelationValues = await Promise.all(targetRows.map(async (raw) => {
      const row = hydrateRow(tableDef, raw);
      return relationValues(conn, runner, tableDef, row);
    }));
    const contractChanges = targetRows.map((raw) => {
      const before = hydrateRow(tableDef, raw);
      return {
        table: tableDef.name,
        pk: before[tableDef.primaryKey],
        operation: "delete",
        before: persistedColumns(tableDef, before),
        after: null,
        relationsBefore: {},
        relationsAfter: {}
      };
    });
    recordNestedImpact(directives, contractChanges, nestedRelationValues);
    await validateContractChanges(conn, contractChanges, "delete");
  }

  for (const rel of tableDef.relations.values()) {
    if (rel.kind === "many-to-many") {
      const phs = targetIds.map((_, i) => dialect.param(i + 1)).join(", ");
      await runner.run(
        `DELETE FROM ${q(rel.junctionTable)} WHERE ${q(rel.junctionLocalKey)} IN (${phs});`,
        targetIds.map((id) => dialect.serialize(id))
      );
    }
  }

  const sql = `DELETE FROM ${q(tableDef.name)}${whereSql};`;
  const res = await runner.run(sql, where.params);
  const summary = { deleted: res.changes, softDeleted: false };
  emitEvent(`delete:${tableDef.name}`, summary);
  return summary;
}

function extractNestedWriteData(tableDef, payload) {
  const ownedChildren = {};
  const m2mValues = {};

  for (const [fieldName, rel] of tableDef.relations.entries()) {
    if (Array.isArray(payload[fieldName])) {
      if (rel.kind === "one-to-many-owned") {
        ownedChildren[fieldName] = payload[fieldName];
      } else if (rel.kind === "many-to-many") {
        m2mValues[fieldName] = payload[fieldName];
      }
    }
  }

  return { ownedChildren, m2mValues };
}

function persistedColumns(tableDef, row) {
  const result = {};
  for (const column of tableDef.columns.keys()) {
    if (Object.prototype.hasOwnProperty.call(row ?? {}, column)) result[column] = row[column];
  }
  return result;
}

async function relationValues(conn, runner, tableDef, row) {
  const values = {};
  const q = conn.dialect.quoteId;
  for (const [name, relation] of tableDef.relations) {
    if (relation.kind !== "many-to-many") continue;
    const links = await runner.all(
      `SELECT ${q(relation.junctionTargetKey)} AS target_id FROM ${q(relation.junctionTable)} WHERE ${q(relation.junctionLocalKey)} = ${conn.dialect.param(1)};`,
      [conn.dialect.serialize(row[relation.localKey])]
    );
    values[name] = links.map((item) => item.target_id);
  }
  return values;
}

function recordNestedImpact(directives, changes, relationsBefore = []) {
  const capture = directives[IMPACT_CAPTURE];
  if (!capture) return;
  if (!Array.isArray(capture.nestedChanges)) capture.nestedChanges = [];
  const items = Array.isArray(changes) ? changes : [changes];
  items.forEach((change, index) => {
    capture.nestedChanges.push({
      ...change,
      relationsBefore: relationsBefore[index] ?? change.relationsBefore ?? {},
      relationsAfter: change.relationsAfter ?? {}
    });
  });
}