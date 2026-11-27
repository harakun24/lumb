import crypto from "node:crypto";
import { DataOperationBlockedError } from "./contracts.js";
import { registerErasureReferences } from "./erasure-refs.js";

const TABLE_NAME = "_lumb_once";
const initializedConnections = new WeakSet();

export async function ensureOnceStore(conn) {
  if (initializedConnections.has(conn)) return;
  const q = conn.dialect.quoteId;
  const textType = conn.dialect.name === "mysql" ? "LONGTEXT" : "TEXT";
  const keyType = conn.dialect.name === "mysql" ? "VARCHAR(255)" : "TEXT";
  await conn.adapter.exec(
    `CREATE TABLE IF NOT EXISTS ${q(TABLE_NAME)} (` +
    `${q("key")} ${keyType} PRIMARY KEY, ` +
    `${q("fingerprint")} ${textType} NOT NULL, ` +
    `${q("result")} ${textType} NULL);`
  );
  initializedConnections.add(conn);
}

export async function reserveOnce(conn, runner, key, fingerprint) {
  const q = conn.dialect.quoteId;
  const params = [
    conn.dialect.serialize(key),
    conn.dialect.serialize(fingerprint),
    conn.dialect.serialize(null)
  ];
  const insert = conn.dialect.name === "mysql"
    ? `INSERT INTO ${q(TABLE_NAME)} (${q("key")}, ${q("fingerprint")}, ${q("result")}) ` +
      `VALUES (${conn.dialect.param(1)}, ${conn.dialect.param(2)}, ${conn.dialect.param(3)}) ` +
      `ON DUPLICATE KEY UPDATE ${q("key")} = ${q("key")};`
    : `INSERT INTO ${q(TABLE_NAME)} (${q("key")}, ${q("fingerprint")}, ${q("result")}) ` +
      `VALUES (${conn.dialect.param(1)}, ${conn.dialect.param(2)}, ${conn.dialect.param(3)}) ` +
      `ON CONFLICT (${q("key")}) DO NOTHING;`;
  await runner.run(insert, params);

  const receipt = await runner.get(
    `SELECT ${q("fingerprint")}, ${q("result")} FROM ${q(TABLE_NAME)} ` +
    `WHERE ${q("key")} = ${conn.dialect.param(1)}${conn.dialect.forUpdateSql(true)};`,
    [conn.dialect.serialize(key)]
  );
  if (!receipt) throw new Error(`Receipt "$once" "${key}" gagal dibuat atau dibaca.`);
  if (receipt.fingerprint !== fingerprint) {
    throw new DataOperationBlockedError([{
      type: "once-conflict",
      key,
      message: `Kunci "$once" "${key}" sudah digunakan untuk perintah atau payload yang berbeda.`
    }]);
  }
  if (receipt.result !== null && receipt.result !== undefined) {
    return { replayed: true, result: decodeValue(JSON.parse(receipt.result)) };
  }
  return { replayed: false };
}

export async function completeOnce(conn, runner, key, result, subjects = []) {
  const q = conn.dialect.quoteId;
  await runner.run(
    `UPDATE ${q(TABLE_NAME)} SET ${q("result")} = ${conn.dialect.param(1)} ` +
    `WHERE ${q("key")} = ${conn.dialect.param(2)};`,
    [encodeOnceResult(result), conn.dialect.serialize(key)]
  );
  await registerErasureReferences(conn, runner, "once", key, subjects);
}

export function onceFingerprint(parsed, payload, directives) {
  const key = directives.$once;
  if (typeof key !== "string" || key.trim().length === 0 || key.length > 255) {
    throw new Error('Direktif "$once" wajib berupa string tidak kosong dengan panjang maksimal 255 karakter.');
  }
  const request = {
    action: parsed.action,
    table: parsed.table,
    pk: parsed.pk,
    payload,
    directives: Object.fromEntries(
      Object.entries(directives)
        .filter(([name]) => name !== "$once" && name !== "$tx")
        .sort(([left], [right]) => left.localeCompare(right))
    )
  };
  return {
    key,
    fingerprint: crypto.createHash("sha256").update(JSON.stringify(encodeValue(request, true))).digest("hex")
  };
}

function encodeValue(value, sortKeys = false, stack = new WeakSet()) {
  if (value === undefined) return { type: "undefined" };
  if (value === null || typeof value === "string" || typeof value === "boolean" ||
      typeof value === "number") {
    return { type: "value", value };
  }
  if (typeof value === "bigint") return { type: "bigint", value: String(value) };
  if (value instanceof Date) return { type: "date", value: value.toISOString() };
  if (typeof value !== "object") {
    throw new Error(`"$once" tidak dapat menyimpan nilai bertipe ${typeof value}.`);
  }
  if (stack.has(value)) throw new Error('Payload "$once" tidak boleh memiliki circular reference.');
  stack.add(value);
  let encoded;
  if (Array.isArray(value)) {
    encoded = { type: "array", value: value.map((item) => encodeValue(item, sortKeys, stack)) };
  } else {
    const entries = Object.entries(value);
    if (sortKeys) entries.sort(([left], [right]) => left.localeCompare(right));
    encoded = {
      type: "object",
      value: entries.map(([key, item]) => [key, encodeValue(item, sortKeys, stack)])
    };
  }
  stack.delete(value);
  return encoded;
}

export function encodeOnceResult(value) {
  return JSON.stringify(encodeValue(value));
}

function decodeValue(encoded) {
  switch (encoded.type) {
    case "undefined":
      return undefined;
    case "value":
      return encoded.value;
    case "bigint":
      return BigInt(encoded.value);
    case "date":
      return new Date(encoded.value);
    case "array":
      return encoded.value.map(decodeValue);
    case "object":
      return Object.fromEntries(encoded.value.map(([key, value]) => [key, decodeValue(value)]));
    default:
      throw new Error(`Tipe nilai "$once" "${encoded.type}" tidak dikenal.`);
  }
}
