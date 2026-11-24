export function parseCommand(command) {
  if (typeof command !== "string" || !command.includes(":")) {
    throw new Error(`invalid: "${command}"`);
  }

  const colonIdx = command.indexOf(":");
  const action = command.slice(0, colonIdx).trim().toLowerCase();
  const rest = command.slice(colonIdx + 1).trim();

  if (action === "raw") {
    return { action, rawSql: rest, table: null, pk: null, subResource: null, queryFilters: {} };
  }

  const [pathPart, queryPart] = rest.split("?");
  const segments = pathPart ? pathPart.split("/").filter(Boolean) : [];

  const table = segments[0] ?? null;
  const pk = segments[1] !== undefined ? coerceQueryValue(decodeURIComponent(segments[1])) : null;
  const subResource = segments[2] !== undefined ? decodeURIComponent(segments[2]) : null;

  const queryFilters = {};
  if (queryPart) {
    const params = new URLSearchParams(queryPart);
    for (const [key, val] of params.entries()) {
      queryFilters[key] = coerceQueryValue(val);
    }
  }

  return { action, table, pk, subResource, queryFilters };
}

function coerceQueryValue(val) {
  if (val === "true") return true;
  if (val === "false") return false;
  if (val === "null") return null;
  if (/^-?(?:0|[1-9]\d*)(?:\.\d+)?$/.test(val)) return Number(val);
  return val;
}

export function splitDirectives(obj = {}) {
  const data = {};
  const directives = {};

  if (!obj || typeof obj !== "object" || Array.isArray(obj)) {
    return { data, directives };
  }

  for (const key of Object.keys(obj)) {
    if (key.startsWith("$")) {
      directives[key] = obj[key];
    } else {
      data[key] = obj[key];
    }
  }

  return { data, directives };
}

export function buildWhereClause(
  filters,
  schema,
  tableName,
  dialect,
  alias = null,
  paramCounter = { count: 0 }
) {
  const clauses = [];
  const params = [];
  const prefix = alias ? `${dialect.quoteId(alias)}.` : "";
  const tableDef = schema.tables.get(tableName);

  const nextParam = (rawVal) => {
    paramCounter.count += 1;
    params.push(dialect.serialize(rawVal));
    return dialect.param(paramCounter.count);
  };

  for (const [field, condition] of Object.entries(filters)) {
    if (condition === undefined) continue;

    if (field === "$or" && Array.isArray(condition)) {
      const orParts = [];
      for (const branch of condition) {
        const sub = buildWhereClause(branch, schema, tableName, dialect, alias, paramCounter);
        if (sub.sql) {
          orParts.push(`(${sub.sql})`);
          params.push(...sub.params);
        }
      }
      if (orParts.length > 0) {
        clauses.push(`(${orParts.join(" OR ")})`);
      }
      continue;
    }

    if (field === "$not" && condition && typeof condition === "object") {
      const sub = buildWhereClause(condition, schema, tableName, dialect, alias, paramCounter);
      if (sub.sql) {
        clauses.push(`NOT (${sub.sql})`);
        params.push(...sub.params);
      }
      continue;
    }

    if (tableDef && tableDef.relations.has(field)) {
      const rel = tableDef.relations.get(field);
      const isAntiJoin = condition === null || condition?.$exists === false;
      const isSemiJoin = condition?.$exists === true;

      if (isAntiJoin || isSemiJoin) {
        const existsClause = buildRelationExistsSql(rel, prefix, dialect);
        clauses.push(isAntiJoin ? `NOT EXISTS (${existsClause})` : `EXISTS (${existsClause})`);
        continue;
      }
    }

    const col = `${prefix}${dialect.quoteId(field)}`;

    if (condition === null) {
      clauses.push(`${col} IS NULL`);
      continue;
    }

    if (Array.isArray(condition)) {
      if (condition.length === 0) {
        clauses.push("1 = 0");
      } else {
        const placeholders = condition.map((v) => nextParam(v)).join(", ");
        clauses.push(`${col} IN (${placeholders})`);
      }
      continue;
    }

    if (typeof condition === "object" && !(condition instanceof Date)) {
      for (const [op, val] of Object.entries(condition)) {
        if (val === undefined) continue;
        switch (op) {
          case "eq":
            clauses.push(val === null ? `${col} IS NULL` : `${col} = ${nextParam(val)}`);
            break;
          case "ne":
            clauses.push(val === null ? `${col} IS NOT NULL` : `${col} != ${nextParam(val)}`);
            break;
          case "gt":
            clauses.push(`${col} > ${nextParam(val)}`);
            break;
          case "gte":
            clauses.push(`${col} >= ${nextParam(val)}`);
            break;
          case "lt":
            clauses.push(`${col} < ${nextParam(val)}`);
            break;
          case "lte":
            clauses.push(`${col} <= ${nextParam(val)}`);
            break;
          case "includes":
            clauses.push(`${col} LIKE ${nextParam(`%${val}%`)}`);
            break;
          case "startsWith":
            clauses.push(`${col} LIKE ${nextParam(`${val}%`)}`);
            break;
          case "endsWith":
            clauses.push(`${col} LIKE ${nextParam(`%${val}`)}`);
            break;
          case "in":
            if (Array.isArray(val) && val.length > 0) {
              const phs = val.map((v) => nextParam(v)).join(", ");
              clauses.push(`${col} IN (${phs})`);
            } else {
              clauses.push("1 = 0");
            }
            break;
          default:
            throw new Error(`Operator filter tidak dikenal: "${op}" pada kolom "${field}".`);
        }
      }
      continue;
    }

    clauses.push(`${col} = ${nextParam(condition)}`);
  }

  return {
    sql: clauses.join(" AND "),
    params
  };
}

function buildRelationExistsSql(rel, parentPrefix, dialect) {
  const q = dialect.quoteId;
  const parentPk = `${parentPrefix}${q(rel.localKey)}`;
  if (rel.kind === "one-to-many-owned" || rel.kind === "one-to-many-reverse") {
    return `SELECT 1 FROM ${q(rel.targetTable)} _sub WHERE _sub.${q(rel.foreignKey)} = ${parentPk}`;
  }
  if (rel.kind === "many-to-many") {
    return `SELECT 1 FROM ${q(rel.junctionTable)} _sub WHERE _sub.${q(rel.junctionLocalKey)} = ${parentPk}`;
  }
  return `SELECT 1 FROM ${q(rel.targetTable)} _sub WHERE _sub.${q(rel.targetKey)} = ${parentPrefix}${q(rel.localKey)}`;
}