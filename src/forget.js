import { assertSchemaReady } from "./schema.js";
import {
  beginTransaction,
  commitTransaction,
  resolveExecutionTarget,
  rollbackTransaction
} from "./transaction.js";
import {
  deleteErasureReferencesForStore,
  deleteErasureReferencesForTarget,
  ensureErasureRefStore,
  findErasureReferences
} from "./erasure-refs.js";
import { encodeOnceResult, ensureOnceStore } from "./once.js";
import { ensureOutboxStore } from "./outbox.js";
import { ensureTimelineStore, scrubTimelineData } from "./timeline.js";

const ONCE_TABLE = "_lumb_once";
const OUTBOX_TABLE = "_lumb_outbox";

export async function executeForget(parsed, directives) {
  if (!parsed.table || parsed.pk === null) {
    throw new Error('Gunakan "forget:<tabel>/<pk>" untuk melupakan data satu record.');
  }
  const { conn } = resolveExecutionTarget(directives);
  if (directives.$tx !== undefined) {
    throw new Error('"forget:" tidak dapat dijalankan di dalam transaksi yang dikelola pemanggil.');
  }
  const tableDef = assertSchemaReady(conn, parsed.table);
  const fields = directives.$fields;
  if (
    !Array.isArray(fields) ||
    fields.length === 0 ||
    fields.some((field) => typeof field !== "string" || field.trim().length === 0) ||
    new Set(fields).size !== fields.length
  ) {
    throw new Error('"$fields" pada "forget:" wajib berupa Array tidak kosong berisi nama kolom unik.');
  }
  for (const field of fields) {
    const column = tableDef.columns.get(field);
    if (!column) throw new Error(`Kolom "${field}" tidak ditemukan pada tabel "${parsed.table}".`);
    if (field === tableDef.primaryKey || column.isPrimaryKey) {
      throw new Error(`Primary key "${field}" tidak dapat dilupakan.`);
    }
    if (!column.nullable) {
      throw new Error(`Kolom "${field}" tidak nullable dan tidak dapat dilupakan.`);
    }
  }

  await ensureTimelineStore(conn);
  await ensureOnceStore(conn);
  await ensureOutboxStore(conn);
  await ensureErasureRefStore(conn);

  const txId = await beginTransaction(null, directives);
  const txDirectives = { ...directives, $tx: txId };
  try {
    const { runner } = resolveExecutionTarget(txDirectives);
    const q = conn.dialect.quoteId;
    const record = await runner.get(
      `SELECT ${q(tableDef.primaryKey)} FROM ${q(tableDef.name)} ` +
      `WHERE ${q(tableDef.primaryKey)} = ${conn.dialect.param(1)}` +
      `${conn.dialect.forUpdateSql(true)};`,
      [conn.dialect.serialize(parsed.pk)]
    );
    if (!record) {
      throw new Error(`Record "${parsed.table}/${parsed.pk}" tidak ditemukan.`);
    }

    const assignments = fields.map((field) => `${q(field)} = NULL`).join(", ");
    await runner.run(
      `UPDATE ${q(tableDef.name)} SET ${assignments} ` +
      `WHERE ${q(tableDef.primaryKey)} = ${conn.dialect.param(1)};`,
      [conn.dialect.serialize(parsed.pk)]
    );

    const references = await findErasureReferences(
      conn,
      runner,
      parsed.table,
      parsed.pk
    );
    for (const reference of references) {
      if (reference.store_name === "once") {
        await runner.run(
          `UPDATE ${q(ONCE_TABLE)} SET ${q("fingerprint")} = ${conn.dialect.param(1)}, ` +
          `${q("result")} = ${conn.dialect.param(2)} WHERE ${q("key")} = ${conn.dialect.param(3)};`,
          [
            "forgotten",
            encodeOnceResult(null),
            reference.store_key
          ].map(conn.dialect.serialize)
        );
      } else if (reference.store_name === "outbox") {
        await runner.run(
          `UPDATE ${q(OUTBOX_TABLE)} SET ${q("event_type")} = ${conn.dialect.param(1)}, ` +
          `${q("payload")} = ${conn.dialect.param(2)}, ${q("status")} = ${conn.dialect.param(3)}, ` +
          `${q("claimed_by")} = NULL, ${q("claim_token")} = NULL, ${q("claimed_until")} = NULL, ` +
          `${q("last_error")} = NULL WHERE ${q("id")} = ${conn.dialect.param(4)};`,
          ["forgotten", "null", "forgotten", reference.store_key]
            .map(conn.dialect.serialize)
        );
      }
      await deleteErasureReferencesForStore(
        conn,
        runner,
        reference.store_name,
        reference.store_key
      );
    }

    await scrubTimelineData(
      conn,
      runner,
      parsed.table,
      parsed.pk,
      fields
    );
    await deleteErasureReferencesForTarget(conn, runner, parsed.table, parsed.pk);
    await commitTransaction(txId);
    return {
      forgotten: true,
      table: parsed.table,
      fields: [...fields],
      referenceCount: references.length
    };
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}
