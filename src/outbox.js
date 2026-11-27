import crypto from "node:crypto";
import {
  beginTransaction,
  commitTransaction,
  resolveExecutionTarget,
  rollbackTransaction
} from "./transaction.js";
import { registerErasureReferences } from "./erasure-refs.js";

const TABLE_NAME = "_lumb_outbox";
const initializedConnections = new WeakSet();

export async function ensureOutboxStore(conn) {
  if (initializedConnections.has(conn)) return;
  const q = conn.dialect.quoteId;
  const textType = conn.dialect.name === "mysql" ? "LONGTEXT" : "TEXT";
  const shortType = conn.dialect.name === "mysql" ? "VARCHAR(255)" : "TEXT";
  await conn.adapter.exec(
    `CREATE TABLE IF NOT EXISTS ${q(TABLE_NAME)} (` +
    `${q("id")} ${shortType} PRIMARY KEY, ` +
    `${q("event_type")} ${shortType} NOT NULL, ` +
    `${q("payload")} ${textType} NOT NULL, ` +
    `${q("created_at")} ${shortType} NOT NULL, ` +
    `${q("available_at")} ${shortType} NOT NULL, ` +
    `${q("status")} ${shortType} NOT NULL, ` +
    `${q("claimed_by")} ${shortType} NULL, ` +
    `${q("claim_token")} ${shortType} NULL, ` +
    `${q("claimed_until")} ${shortType} NULL, ` +
    `${q("attempts")} INTEGER NOT NULL, ` +
    `${q("last_error")} ${textType} NULL);`
  );
  initializedConnections.add(conn);
}

export async function enqueueOutbox(conn, runner, directive, subjects = []) {
  const events = Array.isArray(directive) ? directive : [directive];
  if (events.length === 0) return [];
  const q = conn.dialect.quoteId;
  const createdAt = new Date().toISOString();
  const inserted = [];

  for (const event of events) {
    if (!event || typeof event !== "object" || Array.isArray(event) ||
        typeof event.type !== "string" || event.type.trim().length === 0 ||
        event.type.length > 255) {
      throw new Error('Setiap event "$outbox" wajib berupa objek dengan "type" string (1-255 karakter).');
    }
    const id = crypto.randomUUID();
    const payload = JSON.stringify(event.payload ?? null);
    if (payload === undefined) {
      throw new Error(`Payload event outbox "${event.type}" tidak dapat diserialisasi sebagai JSON.`);
    }
    const values = [
      id,
      event.type,
      payload,
      createdAt,
      createdAt,
      "queued",
      null,
      null,
      null,
      0,
      null
    ].map(conn.dialect.serialize);
    const placeholders = values.map((_, index) => conn.dialect.param(index + 1)).join(", ");
    await runner.run(
      `INSERT INTO ${q(TABLE_NAME)} (` +
      `${q("id")}, ${q("event_type")}, ${q("payload")}, ${q("created_at")}, ` +
      `${q("available_at")}, ${q("status")}, ${q("claimed_by")}, ${q("claim_token")}, ` +
      `${q("claimed_until")}, ${q("attempts")}, ${q("last_error")}) ` +
      `VALUES (${placeholders});`,
      values
    );
    inserted.push({ id, type: event.type });
    await registerErasureReferences(conn, runner, "outbox", id, subjects);
  }
  return inserted;
}

export async function executeOutbox(parsed, directives) {
  const { conn } = resolveExecutionTarget(directives);
  await ensureOutboxStore(conn);
  const txId = await beginTransaction(directives.$tx ?? null, directives);

  try {
    const txDirectives = { ...directives, $tx: txId };
    const { runner } = resolveExecutionTarget(txDirectives);
    let result;
    if (parsed.table === "claim" && parsed.pk === null) {
      result = await claimOutbox(conn, runner, directives);
    } else if (parsed.table === "ack" && parsed.pk !== null) {
      result = await settleOutbox(conn, runner, parsed.pk, directives, "delivered");
    } else if (parsed.table === "fail" && parsed.pk !== null) {
      result = await settleOutbox(conn, runner, parsed.pk, directives, "queued");
    } else {
      throw new Error('Gunakan "outbox:claim", "outbox:ack/<id>", atau "outbox:fail/<id>".');
    }
    await commitTransaction(txId);
    return result;
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

async function claimOutbox(conn, runner, directives) {
  const worker = requireString(directives.$worker, '"$worker"');
  const limit = directives.$limit === undefined ? 10 : Number(directives.$limit);
  const ttl = directives.$ttl === undefined ? 30000 : Number(directives.$ttl);
  if (!Number.isSafeInteger(limit) || limit < 1 || limit > 1000) {
    throw new Error('"$limit" outbox wajib berupa integer antara 1 dan 1000.');
  }
  if (!Number.isSafeInteger(ttl) || ttl < 1000 || ttl > 86400000) {
    throw new Error('"$ttl" outbox wajib berupa integer antara 1000 ms dan 24 jam.');
  }

  const now = new Date();
  const nowIso = now.toISOString();
  const expiresAt = new Date(now.getTime() + ttl).toISOString();
  const q = conn.dialect.quoteId;
  const rows = await runner.all(
    `SELECT ${q("id")}, ${q("event_type")}, ${q("payload")}, ${q("created_at")}, ${q("attempts")} ` +
    `FROM ${q(TABLE_NAME)} WHERE (` +
    `(${q("status")} = ${conn.dialect.param(1)} AND ${q("available_at")} <= ${conn.dialect.param(2)}) OR ` +
    `(${q("status")} = ${conn.dialect.param(3)} AND ${q("claimed_until")} <= ${conn.dialect.param(4)})` +
    `) ORDER BY ${q("created_at")} ASC LIMIT ${conn.dialect.param(5)}` +
    `${conn.dialect.forUpdateSql(true)};`,
    ["queued", nowIso, "claimed", nowIso, limit].map(conn.dialect.serialize)
  );

  const claimed = [];
  for (const row of rows) {
    const claimToken = crypto.randomUUID();
    await runner.run(
      `UPDATE ${q(TABLE_NAME)} SET ${q("status")} = ${conn.dialect.param(1)}, ` +
      `${q("claimed_by")} = ${conn.dialect.param(2)}, ${q("claim_token")} = ${conn.dialect.param(3)}, ` +
      `${q("claimed_until")} = ${conn.dialect.param(4)}, ${q("attempts")} = ${q("attempts")} + 1 ` +
      `WHERE ${q("id")} = ${conn.dialect.param(5)};`,
      ["claimed", worker, claimToken, expiresAt, row.id].map(conn.dialect.serialize)
    );
    claimed.push({
      id: row.id,
      type: row.event_type,
      payload: JSON.parse(row.payload),
      createdAt: row.created_at,
      attempts: Number(row.attempts) + 1,
      claimToken,
      claimedUntil: expiresAt
    });
  }
  return claimed;
}

async function settleOutbox(conn, runner, id, directives, nextStatus) {
  const claimToken = requireString(directives.$claim, '"$claim"');
  const q = conn.dialect.quoteId;
  let sql;
  let params;
  if (nextStatus === "delivered") {
    sql = `UPDATE ${q(TABLE_NAME)} SET ${q("status")} = ${conn.dialect.param(1)}, ` +
      `${q("claimed_by")} = NULL, ${q("claim_token")} = NULL, ${q("claimed_until")} = NULL ` +
      `WHERE ${q("id")} = ${conn.dialect.param(2)} AND ${q("status")} = ${conn.dialect.param(3)} ` +
      `AND ${q("claim_token")} = ${conn.dialect.param(4)};`;
    params = ["delivered", id, "claimed", claimToken];
  } else {
    const retryAfter = directives.$retryAfter === undefined ? 0 : Number(directives.$retryAfter);
    if (!Number.isSafeInteger(retryAfter) || retryAfter < 0 || retryAfter > 86400000) {
      throw new Error('"$retryAfter" wajib berupa integer antara 0 ms dan 24 jam.');
    }
    const availableAt = new Date(Date.now() + retryAfter).toISOString();
    const lastError = directives.$error === undefined ? null : String(directives.$error);
    sql = `UPDATE ${q(TABLE_NAME)} SET ${q("status")} = ${conn.dialect.param(1)}, ` +
      `${q("available_at")} = ${conn.dialect.param(2)}, ${q("claimed_by")} = NULL, ` +
      `${q("claim_token")} = NULL, ${q("claimed_until")} = NULL, ${q("last_error")} = ${conn.dialect.param(3)} ` +
      `WHERE ${q("id")} = ${conn.dialect.param(4)} AND ${q("status")} = ${conn.dialect.param(5)} ` +
      `AND ${q("claim_token")} = ${conn.dialect.param(6)};`;
    params = ["queued", availableAt, lastError, id, "claimed", claimToken];
  }

  const result = await runner.run(sql, params.map(conn.dialect.serialize));
  return result.changes > 0
    ? { settled: true, status: nextStatus }
    : { settled: false, reason: "claim-changed" };
}

function requireString(value, name) {
  if (typeof value !== "string" || value.trim().length === 0) {
    throw new Error(`${name} outbox wajib berupa string tidak kosong.`);
  }
  return value;
}
