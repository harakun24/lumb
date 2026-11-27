import crypto from "node:crypto";
import { assertSchemaReady } from "./schema.js";
import { resolveExecutionTarget } from "./transaction.js";

const TABLE_NAME = "_lumb_access";

export async function ensureAccessStore(conn) {
  const q = conn.dialect.quoteId;
  const textType = conn.dialect.name === "mysql" ? "LONGTEXT" : "TEXT";
  const shortType = conn.dialect.name === "mysql" ? "VARCHAR(255)" : "TEXT";
  await conn.adapter.exec(
    `CREATE TABLE IF NOT EXISTS ${q(TABLE_NAME)} (` +
    `${q("id")} ${shortType} PRIMARY KEY, ` +
    `${q("created_at")} ${shortType} NOT NULL, ` +
    `${q("actor_id")} ${shortType} NOT NULL, ` +
    `${q("purpose")} ${shortType} NOT NULL, ` +
    `${q("action")} ${shortType} NOT NULL, ` +
    `${q("table_name")} ${shortType} NOT NULL, ` +
    `${q("connection_name")} ${shortType} NULL, ` +
    `${q("fields")} ${textType} NOT NULL, ` +
    `${q("relations")} ${textType} NOT NULL, ` +
    `${q("filter_fields")} ${textType} NOT NULL, ` +
    `${q("record_count")} BIGINT NOT NULL);`
  );
}

export function normalizeAccessRequest(parsed, directives, filters, conn) {
  if (directives.$purpose === undefined) return null;
  if (!["get", "dump"].includes(parsed.action)) {
    throw new Error('Direktif "$purpose" hanya mendukung pembacaan melalui "get:" dan "dump:".');
  }
  if (directives.$tx !== undefined) {
    throw new Error('"$purpose" tidak dapat digunakan di dalam transaksi; receipt akses harus tetap tercatat setelah rollback.');
  }
  if (directives.$stream !== undefined) {
    throw new Error('"$purpose" belum mendukung "$stream"; konsumsi stream parsial tidak menghasilkan receipt lengkap.');
  }
  if (parsed.subResource) {
    throw new Error('"$purpose" belum mendukung pembacaan sub-relasi karena tabel targetnya tidak eksplisit.');
  }
  if (
    directives.$group !== undefined ||
    ["$count", "$sum", "$avg", "$min", "$max"].some((key) => directives[key] !== undefined)
  ) {
    throw new Error('"$purpose" belum mendukung agregasi karena kolom sumbernya tidak dapat dicatat dengan akurat.');
  }
  if (typeof directives.$purpose !== "string" ||
      directives.$purpose.trim().length === 0 ||
      directives.$purpose.length > 255) {
    throw new Error('"$purpose" wajib berupa string tidak kosong dengan panjang maksimal 255 karakter.');
  }

  const actorId = directives.$actor?.id;
  if ((typeof actorId !== "string" && typeof actorId !== "number") ||
      String(actorId).trim().length === 0) {
    throw new Error('"$purpose" mewajibkan "$actor.id" berupa string atau angka yang tidak kosong.');
  }
  if (!parsed.table) throw new Error('"$purpose" memerlukan nama tabel.');

  const tableDef = assertSchemaReady(conn, parsed.table);
  if (
    directives.$fields !== undefined &&
    (!Array.isArray(directives.$fields) ||
      directives.$fields.some((field) => typeof field !== "string" || field.trim().length === 0))
  ) {
    throw new Error('Direktif "$fields" bersama "$purpose" wajib berupa Array nama kolom string.');
  }
  const requestedFields = parsed.action === "dump" ||
      directives.$fields === undefined ||
      directives.$fields.length === 0
    ? [...tableDef.columns.keys()]
    : [...new Set(directives.$fields)];
  const relations = parsed.action === "get"
    ? normalizeRelations(tableDef, directives.$with, conn.schema, directives.$trash)
    : [];
  const filterFields = new Set();
  collectFilterFields(parsed.queryFilters, tableDef, filterFields);
  collectFilterFields(filters, tableDef, filterFields);
  if (parsed.pk !== null) filterFields.add(tableDef.primaryKey);
  if (parsed.action === "get" && directives.$cursor !== undefined) {
    filterFields.add(tableDef.primaryKey);
  }
  if (parsed.action === "get" && directives.$trash !== "include" && tableDef.softDeletePatch) {
    for (const field of Object.keys(tableDef.softDeletePatch)) filterFields.add(field);
  }

  return {
    actorId: String(actorId),
    purpose: directives.$purpose.trim(),
    action: parsed.action,
    table: parsed.table,
    connectionName: directives.$connection ?? null,
    fields: requestedFields,
    relations,
    filterFields: [...filterFields].sort()
  };
}

export async function recordAccess(conn, request, result) {
  const q = conn.dialect.quoteId;
  const recordCount = Array.isArray(result)
    ? result.length
    : Array.isArray(result?.data)
      ? result.data.length
      : result === null || result === undefined
        ? 0
        : 1;
  await conn.adapter.run(
    `INSERT INTO ${q(TABLE_NAME)} (` +
    `${q("id")}, ${q("created_at")}, ${q("actor_id")}, ${q("purpose")}, ` +
    `${q("action")}, ${q("table_name")}, ${q("connection_name")}, ${q("fields")}, ` +
    `${q("relations")}, ${q("filter_fields")}, ${q("record_count")}) VALUES (` +
    `${conn.dialect.param(1)}, ${conn.dialect.param(2)}, ${conn.dialect.param(3)}, ` +
    `${conn.dialect.param(4)}, ${conn.dialect.param(5)}, ${conn.dialect.param(6)}, ` +
    `${conn.dialect.param(7)}, ${conn.dialect.param(8)}, ${conn.dialect.param(9)}, ` +
    `${conn.dialect.param(10)}, ${conn.dialect.param(11)});`,
    [
      crypto.randomUUID(),
      new Date().toISOString(),
      request.actorId,
      request.purpose,
      request.action,
      request.table,
      request.connectionName,
      JSON.stringify(request.fields),
      JSON.stringify(request.relations),
      JSON.stringify(request.filterFields),
      recordCount
    ].map(conn.dialect.serialize)
  );
}

export async function executeAccess(parsed, directives) {
  if (!parsed.table) {
    throw new Error('Gunakan "access:history" atau "access:<tabel>" untuk membaca receipt akses.');
  }
  const { conn, runner } = resolveExecutionTarget(directives);
  await ensureAccessStore(conn);
  const q = conn.dialect.quoteId;
  const conditions = [];
  const params = [];
  const add = (column, value) => {
    params.push(conn.dialect.serialize(value));
    conditions.push(`${q(column)} = ${conn.dialect.param(params.length)}`);
  };

  if (parsed.table && parsed.table !== "history") add("table_name", parsed.table);
  if (directives.$actor !== undefined) {
    const actorId = typeof directives.$actor === "object"
      ? directives.$actor?.id
      : directives.$actor;
    if ((typeof actorId !== "string" && typeof actorId !== "number") ||
        String(actorId).trim().length === 0) {
      throw new Error('"$actor" pada "access:" wajib berupa ID string atau angka.');
    }
    add("actor_id", String(actorId));
  }
  if (directives.$purpose !== undefined) {
    if (typeof directives.$purpose !== "string" || directives.$purpose.trim().length === 0) {
      throw new Error('"$purpose" filter wajib berupa string tidak kosong.');
    }
    add("purpose", directives.$purpose.trim());
  }
  if (directives.$since !== undefined) {
    const since = directives.$since instanceof Date
      ? directives.$since
      : typeof directives.$since === "string"
        ? new Date(directives.$since)
        : null;
    if (!since || Number.isNaN(since.getTime())) {
      throw new Error('"$since" pada "access:" wajib berupa Date atau timestamp string yang valid.');
    }
    params.push(conn.dialect.serialize(since.toISOString()));
    conditions.push(`${q("created_at")} >= ${conn.dialect.param(params.length)}`);
  }

  const limit = directives.$limit === undefined ? 100 : Number(directives.$limit);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
    throw new Error('"$limit" pada "access:" wajib berupa integer antara 1 dan 1000.');
  }
  params.push(conn.dialect.serialize(limit));
  const whereSql = conditions.length > 0 ? ` WHERE ${conditions.join(" AND ")}` : "";
  const rows = await runner.all(
    `SELECT ${q("id")}, ${q("created_at")}, ${q("actor_id")}, ${q("purpose")}, ` +
    `${q("action")}, ${q("table_name")}, ${q("connection_name")}, ${q("fields")}, ` +
    `${q("relations")}, ${q("filter_fields")}, ${q("record_count")} ` +
    `FROM ${q(TABLE_NAME)}${whereSql} ORDER BY ${q("created_at")} DESC ` +
    `LIMIT ${conn.dialect.param(params.length)};`,
    params
  );
  return rows.map((row) => ({
    id: row.id,
    createdAt: row.created_at,
    actorId: row.actor_id,
    purpose: row.purpose,
    action: row.action,
    table: row.table_name,
    connection: row.connection_name,
    fields: JSON.parse(row.fields),
    relations: JSON.parse(row.relations),
    filterFields: JSON.parse(row.filter_fields),
    recordCount: Number(row.record_count)
  }));
}

function collectFilterFields(value, tableDef, fields) {
  if (!value || typeof value !== "object") return;
  if (Array.isArray(value)) {
    for (const item of value) collectFilterFields(item, tableDef, fields);
    return;
  }
  for (const [key, nested] of Object.entries(value)) {
    if (tableDef.columns.has(key) || tableDef.relations.has(key)) fields.add(key);
    collectFilterFields(nested, tableDef, fields);
  }
}

function normalizeRelations(tableDef, withDirective, schema, trashMode) {
  if (!withDirective || typeof withDirective !== "object" || Array.isArray(withDirective)) {
    return {};
  }
  const relations = {};
  for (const [name, config] of Object.entries(withDirective)) {
    const relation = tableDef.relations.get(name);
    const target = schema.tables.get(relation?.targetTable ?? name);
    if (!target) {
      throw new Error(`Tabel target untuk relasi "${name}" belum didefinisikan.`);
    }
    const options = config && typeof config === "object" && !Array.isArray(config)
      ? config
      : {};
    if (
      options.$fields !== undefined &&
      (!Array.isArray(options.$fields) ||
        options.$fields.some((field) => typeof field !== "string" || field.trim().length === 0))
    ) {
      throw new Error(`"$fields" pada relasi "${name}" bersama "$purpose" wajib berupa Array nama kolom string.`);
    }
    if (
      options.$with !== undefined &&
      (!options.$with || typeof options.$with !== "object" || Array.isArray(options.$with))
    ) {
      throw new Error(`Direktif "$with" pada relasi "${name}" bersama "$purpose" wajib berupa objek.`);
    }
    const childFilters = {};
    const nestedWith = { ...(options.$with ?? {}) };
    for (const [key, value] of Object.entries(options)) {
      if (["$fields", "$required", "$join", "$with"].includes(key)) continue;
      if (target.relations.has(key)) nestedWith[key] = value;
      else childFilters[key] = value;
    }
    const filterFields = new Set();
    collectFilterFields(childFilters, target, filterFields);
    if (trashMode !== "include" && target.softDeletePatch) {
      for (const field of Object.keys(target.softDeletePatch)) filterFields.add(field);
    }
    const relationFields = options.$fields === undefined || options.$fields.length === 0
      ? [...target.columns.keys()]
      : [...new Set(options.$fields)];
    relations[name] = {
      table: target.name,
      fields: relationFields,
      filterFields: [...filterFields].sort(),
      relations: normalizeRelations(target, nestedWith, schema, trashMode)
    };
  }
  return relations;
}
