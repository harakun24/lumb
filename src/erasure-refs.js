import crypto from "node:crypto";

const TABLE_NAME = "_lumb_forget_refs";
const initializedConnections = new WeakSet();

export async function ensureErasureRefStore(conn) {
  if (initializedConnections.has(conn)) return;
  const q = conn.dialect.quoteId;
  const textType = conn.dialect.name === "mysql" ? "LONGTEXT" : "TEXT";
  const shortType = conn.dialect.name === "mysql" ? "VARCHAR(255)" : "TEXT";
  await conn.adapter.exec(
    `CREATE TABLE IF NOT EXISTS ${q(TABLE_NAME)} (` +
    `${q("id")} ${shortType} PRIMARY KEY, ` +
    `${q("store_name")} ${shortType} NOT NULL, ` +
    `${q("store_key")} ${shortType} NOT NULL, ` +
    `${q("target_table")} ${shortType} NOT NULL, ` +
    `${q("target_pk")} ${textType} NOT NULL);`
  );
  initializedConnections.add(conn);
}

export async function registerErasureReferences(conn, runner, storeName, storeKey, subjects) {
  if (!Array.isArray(subjects) || subjects.length === 0) return;
  const q = conn.dialect.quoteId;
  const uniqueSubjects = new Map();
  for (const subject of subjects) {
    if (!subject || typeof subject.table !== "string" ||
        subject.pk === null || subject.pk === undefined) continue;
    uniqueSubjects.set(
      `${subject.table}\u0000${JSON.stringify(subject.pk)}`,
      subject
    );
  }

  for (const subject of uniqueSubjects.values()) {
    await runner.run(
      `INSERT INTO ${q(TABLE_NAME)} (` +
      `${q("id")}, ${q("store_name")}, ${q("store_key")}, ${q("target_table")}, ${q("target_pk")}) ` +
      `VALUES (${conn.dialect.param(1)}, ${conn.dialect.param(2)}, ${conn.dialect.param(3)}, ` +
      `${conn.dialect.param(4)}, ${conn.dialect.param(5)});`,
      [
        crypto.randomUUID(),
        storeName,
        storeKey,
        subject.table,
        JSON.stringify(subject.pk)
      ].map(conn.dialect.serialize)
    );
  }
}

export async function findErasureReferences(conn, runner, tableName, primaryKey) {
  const q = conn.dialect.quoteId;
  return runner.all(
    `SELECT ${q("store_name")}, ${q("store_key")} FROM ${q(TABLE_NAME)} ` +
    `WHERE ${q("target_table")} = ${conn.dialect.param(1)} AND ${q("target_pk")} = ${conn.dialect.param(2)};`,
    [tableName, JSON.stringify(primaryKey)].map(conn.dialect.serialize)
  );
}

export async function deleteErasureReferencesForStore(conn, runner, storeName, storeKey) {
  const q = conn.dialect.quoteId;
  await runner.run(
    `DELETE FROM ${q(TABLE_NAME)} WHERE ${q("store_name")} = ${conn.dialect.param(1)} ` +
    `AND ${q("store_key")} = ${conn.dialect.param(2)};`,
    [storeName, storeKey].map(conn.dialect.serialize)
  );
}

export async function deleteErasureReferencesForTarget(conn, runner, tableName, primaryKey) {
  const q = conn.dialect.quoteId;
  await runner.run(
    `DELETE FROM ${q(TABLE_NAME)} WHERE ${q("target_table")} = ${conn.dialect.param(1)} ` +
    `AND ${q("target_pk")} = ${conn.dialect.param(2)};`,
    [tableName, JSON.stringify(primaryKey)].map(conn.dialect.serialize)
  );
}
