import crypto from "node:crypto";
import { assertSchemaReady } from "./schema.js";
import { applySoftDeleteFilter } from "./query.js";
import { buildWhereClause } from "./parser.js";
import {
  beginTransaction,
  commitTransaction,
  resolveExecutionTarget,
  rollbackTransaction
} from "./transaction.js";
import { DataOperationBlockedError } from "./contracts.js";

const TABLE_NAME = "_lumb_leases";
const initializedConnections = new WeakSet();
const DEFAULT_TTL_MS = 10 * 60 * 1000;
const MAX_TTL_MS = 24 * 60 * 60 * 1000;
const KEY_CHUNK_SIZE = 400;

export async function ensureLeaseStore(conn) {
  if (initializedConnections.has(conn)) return;
  const q = conn.dialect.quoteId;
  const textType = conn.dialect.name === "mysql" ? "LONGTEXT" : "TEXT";
  await conn.adapter.exec(
    `CREATE TABLE IF NOT EXISTS ${q(TABLE_NAME)} (` +
    `${q("resource_key")} CHAR(64) PRIMARY KEY, ` +
    `${q("table_name")} VARCHAR(255) NOT NULL, ` +
    `${q("row_key")} ${textType} NOT NULL, ` +
    `${q("owner_id")} VARCHAR(255), ` +
    `${q("owner_json")} ${textType}, ` +
    `${q("token")} VARCHAR(36), ` +
    `${q("expires_at")} BIGINT NOT NULL, ` +
    `${q("generation")} BIGINT NOT NULL, ` +
    `${q("updated_at")} VARCHAR(40) NOT NULL);`
  );
  initializedConnections.add(conn);
}

export async function executeClaim(parsed, directives) {
  getResource(parsed, directives);
  const owner = getOwner(directives.$actor);
  const ttl = getTtl(directives.$ttl);
  const { conn } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  await ensureLeaseStore(conn);

  const key = makeResourceKey(parsed.table, parsed.pk);
  const token = crypto.randomUUID();
  const now = Date.now();
  const txId = await beginLeaseTransaction(directives);

  try {
    const txDirectives = { ...directives, $tx: txId };
    const { runner } = resolveExecutionTarget(txDirectives);
    await assertResourceExists(conn, runner, tableDef, parsed.pk, directives);

    const existing = await selectLease(conn, runner, key, true);
    if (existing && Number(existing.expires_at) > now && existing.token) {
      await commitTransaction(txId);
      return { claimed: false, lease: publicLease(existing) };
    }

    const expiresAt = now + ttl;
    const updatedAt = new Date(now).toISOString();
    if (existing) {
      await runner.run(
        `UPDATE ${conn.dialect.quoteId(TABLE_NAME)} SET ` +
        `${conn.dialect.quoteId("table_name")} = ${conn.dialect.param(1)}, ` +
        `${conn.dialect.quoteId("row_key")} = ${conn.dialect.param(2)}, ` +
        `${conn.dialect.quoteId("owner_id")} = ${conn.dialect.param(3)}, ` +
        `${conn.dialect.quoteId("owner_json")} = ${conn.dialect.param(4)}, ` +
        `${conn.dialect.quoteId("token")} = ${conn.dialect.param(5)}, ` +
        `${conn.dialect.quoteId("expires_at")} = ${conn.dialect.param(6)}, ` +
        `${conn.dialect.quoteId("generation")} = ${conn.dialect.quoteId("generation")} + 1, ` +
        `${conn.dialect.quoteId("updated_at")} = ${conn.dialect.param(7)} ` +
        `WHERE ${conn.dialect.quoteId("resource_key")} = ${conn.dialect.param(8)};`,
        [
          parsed.table,
          JSON.stringify(parsed.pk),
          owner.id,
          JSON.stringify(owner),
          token,
          expiresAt,
          updatedAt,
          key
        ]
      );
    } else {
      await runner.run(
        `INSERT INTO ${conn.dialect.quoteId(TABLE_NAME)} (` +
        `${conn.dialect.quoteId("resource_key")}, ${conn.dialect.quoteId("table_name")}, ` +
        `${conn.dialect.quoteId("row_key")}, ${conn.dialect.quoteId("owner_id")}, ` +
        `${conn.dialect.quoteId("owner_json")}, ${conn.dialect.quoteId("token")}, ` +
        `${conn.dialect.quoteId("expires_at")}, ${conn.dialect.quoteId("generation")}, ` +
        `${conn.dialect.quoteId("updated_at")}) VALUES (` +
        `${conn.dialect.param(1)}, ${conn.dialect.param(2)}, ${conn.dialect.param(3)}, ` +
        `${conn.dialect.param(4)}, ${conn.dialect.param(5)}, ${conn.dialect.param(6)}, ` +
        `${conn.dialect.param(7)}, ${conn.dialect.param(8)}, ${conn.dialect.param(9)});`,
        [
          key,
          parsed.table,
          JSON.stringify(parsed.pk),
          owner.id,
          JSON.stringify(owner),
          token,
          expiresAt,
          1,
          updatedAt
        ]
      );
    }

    await commitTransaction(txId);
    return {
      claimed: true,
      token,
      owner,
      expiresAt: new Date(expiresAt).toISOString(),
      generation: Number(existing?.generation ?? 0) + 1
    };
  } catch (error) {
    await rollbackTransaction(txId);
    if (isUniqueConstraintError(error)) {
      const current = await readLease(conn, key);
      if (current && Number(current.expires_at) > Date.now() && current.token) {
        return { claimed: false, lease: publicLease(current) };
      }
    }
    throw error;
  }
}

export async function executeRenew(parsed, directives) {
  getResource(parsed, directives);
  const owner = getOwner(directives.$actor);
  const token = getLeaseToken(directives.$lease);
  const ttl = getTtl(directives.$ttl);
  const { conn } = resolveExecutionTarget(directives);
  assertSchemaReady(conn, parsed.table);
  await ensureLeaseStore(conn);

  const key = makeResourceKey(parsed.table, parsed.pk);
  const txId = await beginLeaseTransaction(directives);
  try {
    const { runner } = resolveExecutionTarget({ ...directives, $tx: txId });
    const tableDef = assertSchemaReady(conn, parsed.table);
    await assertResourceExists(conn, runner, tableDef, parsed.pk, directives);
    const existing = await selectLease(conn, runner, key, true);
    assertCurrentLease(existing, token, owner, parsed.table, parsed.pk);

    const expiresAt = Date.now() + ttl;
    await runner.run(
      `UPDATE ${conn.dialect.quoteId(TABLE_NAME)} SET ` +
      `${conn.dialect.quoteId("expires_at")} = ${conn.dialect.param(1)}, ` +
      `${conn.dialect.quoteId("updated_at")} = ${conn.dialect.param(2)} ` +
      `WHERE ${conn.dialect.quoteId("resource_key")} = ${conn.dialect.param(3)} ` +
      `AND ${conn.dialect.quoteId("token")} = ${conn.dialect.param(4)};`,
      [expiresAt, new Date().toISOString(), key, token]
    );
    await commitTransaction(txId);
    return {
      renewed: true,
      token,
      owner,
      expiresAt: new Date(expiresAt).toISOString(),
      generation: Number(existing.generation)
    };
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

export async function executeRelease(parsed, directives) {
  getResource(parsed, directives);
  const owner = getOwner(directives.$actor);
  const token = getLeaseToken(directives.$lease);
  const { conn } = resolveExecutionTarget(directives);
  assertSchemaReady(conn, parsed.table);
  await ensureLeaseStore(conn);

  const key = makeResourceKey(parsed.table, parsed.pk);
  const txId = await beginLeaseTransaction(directives);
  try {
    const { runner } = resolveExecutionTarget({ ...directives, $tx: txId });
    const tableDef = assertSchemaReady(conn, parsed.table);
    await assertResourceExists(conn, runner, tableDef, parsed.pk, directives);
    const existing = await selectLease(conn, runner, key, true);
    if (!existing || existing.token !== token || existing.owner_id !== owner.id) {
      await commitTransaction(txId);
      return { released: false, reason: "lease-changed" };
    }

    await runner.run(
      `UPDATE ${conn.dialect.quoteId(TABLE_NAME)} SET ` +
      `${conn.dialect.quoteId("owner_id")} = NULL, ` +
      `${conn.dialect.quoteId("owner_json")} = NULL, ` +
      `${conn.dialect.quoteId("token")} = NULL, ` +
      `${conn.dialect.quoteId("expires_at")} = 0, ` +
      `${conn.dialect.quoteId("updated_at")} = ${conn.dialect.param(1)} ` +
      `WHERE ${conn.dialect.quoteId("resource_key")} = ${conn.dialect.param(2)} ` +
      `AND ${conn.dialect.quoteId("token")} = ${conn.dialect.param(3)};`,
      [new Date().toISOString(), key, token]
    );
    await commitTransaction(txId);
    return { released: true, generation: Number(existing.generation) };
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

export async function executeLeaseStatus(parsed, directives) {
  getResource(parsed, directives);
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  await ensureLeaseStore(conn);
  await assertResourceExists(conn, runner, tableDef, parsed.pk, directives, false);
  const lease = await readLease(conn, makeResourceKey(parsed.table, parsed.pk), runner);
  if (!lease || Number(lease.expires_at) <= Date.now() || !lease.token) {
    return {
      held: false,
      generation: Number(lease?.generation ?? 0)
    };
  }
  return { held: true, ...publicLease(lease) };
}

export async function assertMutationLeaseAccess(conn, runner, tableDef, rows, directives) {
  if (rows.length === 0) return;
  await ensureLeaseStore(conn);

  const resources = new Map();
  for (const row of rows) {
    const pk = row[tableDef.primaryKey];
    resources.set(makeResourceKey(tableDef.name, pk), pk);
  }

  const leases = new Map();
  const keys = [...resources.keys()];
  const q = conn.dialect.quoteId;
  for (let start = 0; start < keys.length; start += KEY_CHUNK_SIZE) {
    const chunk = keys.slice(start, start + KEY_CHUNK_SIZE);
    const placeholders = chunk.map((_, index) => conn.dialect.param(index + 1));
    const found = await runner.all(
      `SELECT * FROM ${q(TABLE_NAME)} WHERE ${q("resource_key")} IN (${placeholders.join(", ")});`,
      chunk
    );
    for (const lease of found) leases.set(lease.resource_key, lease);
  }

  const token = directives.$lease;
  const actorId = directives.$actor?.id == null ? null : String(directives.$actor.id);
  const now = Date.now();
  const blockers = [];
  for (const [key, pk] of resources) {
    const lease = leases.get(key);
    const active = lease && lease.token && Number(lease.expires_at) > now;
    const tokenMatches = lease && token === lease.token;
    const actorMatches = lease && actorId !== null && actorId === lease.owner_id;
    if (active && tokenMatches && actorMatches) continue;
    if (!active && token === undefined) continue;

    const owner = lease?.owner_json ? JSON.parse(lease.owner_json) : null;
    const expired = lease && Number(lease.expires_at) <= now;
    const message = active
      ? `Record "${tableDef.name}/${pk}" sedang dipegang oleh "${lease.owner_id}" sampai ${new Date(Number(lease.expires_at)).toISOString()}.`
      : `Lease untuk "${tableDef.name}/${pk}" sudah kedaluwarsa atau token tidak valid.`;
    blockers.push({
      type: "lease",
      table: tableDef.name,
      pk,
      owner,
      expiresAt: lease?.expires_at ? new Date(Number(lease.expires_at)).toISOString() : null,
      expired: Boolean(expired),
      message
    });
  }

  if (blockers.length > 0) throw new DataOperationBlockedError(blockers);
}

async function assertResourceExists(conn, runner, tableDef, pk, directives, lock = true) {
  const filters = applySoftDeleteFilter(
    tableDef,
    { ...(directives.$where ?? {}), [tableDef.primaryKey]: pk },
    directives.$trash
  );
  const where = buildWhereClause(filters, conn.schema, tableDef.name, conn.dialect);
  const row = await runner.get(
    `SELECT ${conn.dialect.quoteId(tableDef.primaryKey)} FROM ${conn.dialect.quoteId(tableDef.name)} ` +
    `WHERE ${where.sql}${conn.dialect.forUpdateSql(lock)};`,
    where.params
  );
  if (!row) {
    throw new Error(`Record "${tableDef.name}/${pk}" tidak ditemukan atau tidak aktif.`);
  }
}

async function beginLeaseTransaction(directives) {
  if (directives.$tx) {
    throw new Error('Perintah lease tidak dapat dijalankan di dalam direktif "$tx".');
  }
  if (directives.$dryRun) {
    throw new Error("Perintah lease tidak mendukung direktif \"$dryRun\".");
  }
  return beginTransaction(null, directives);
}

async function selectLease(conn, runner, key, lock = false) {
  const q = conn.dialect.quoteId;
  return runner.get(
    `SELECT * FROM ${q(TABLE_NAME)} WHERE ${q("resource_key")} = ${conn.dialect.param(1)}` +
    `${conn.dialect.forUpdateSql(lock)};`,
    [key]
  );
}

async function readLease(conn, key, runner = conn.adapter) {
  const q = conn.dialect.quoteId;
  return runner.get(
    `SELECT * FROM ${q(TABLE_NAME)} WHERE ${q("resource_key")} = ${conn.dialect.param(1)};`,
    [key]
  );
}

function getResource(parsed, directives) {
  if (!parsed.table || parsed.pk === null) {
    throw new Error('Perintah lease wajib menggunakan format "<aksi>:<tabel>/<pk>".');
  }
  if (directives.$branch) {
    throw new Error("Perintah lease tidak dapat digunakan pada draft branch.");
  }
  return { table: parsed.table, pk: parsed.pk };
}

function getOwner(actor) {
  if (!actor || (typeof actor.id !== "string" && typeof actor.id !== "number") ||
      String(actor.id).trim().length === 0 || String(actor.id).length > 255) {
    throw new Error('Perintah lease memerlukan "$actor.id" berupa string atau angka (maksimal 255 karakter).');
  }
  const owner = { id: String(actor.id) };
  if (typeof actor.name === "string") owner.name = actor.name;
  return owner;
}

function getLeaseToken(token) {
  if (typeof token !== "string" || token.length === 0) {
    throw new Error('Perintah "renew:" dan "release:" memerlukan direktif "$lease" yang valid.');
  }
  return token;
}

function getTtl(value) {
  const ttl = value === undefined ? DEFAULT_TTL_MS : Number(value);
  if (!Number.isSafeInteger(ttl) || ttl < 1000 || ttl > MAX_TTL_MS) {
    throw new Error(`"$ttl" wajib berupa integer antara 1000 dan ${MAX_TTL_MS} milidetik.`);
  }
  return ttl;
}

function makeResourceKey(table, pk) {
  return crypto
    .createHash("sha256")
    .update(JSON.stringify([table, typeof pk, pk]))
    .digest("hex");
}

function publicLease(row) {
  return {
    owner: row.owner_json ? JSON.parse(row.owner_json) : null,
    expiresAt: new Date(Number(row.expires_at)).toISOString(),
    generation: Number(row.generation)
  };
}

function assertCurrentLease(lease, token, owner, table, pk) {
  if (!lease || !lease.token || lease.token !== token ||
      lease.owner_id !== owner.id || Number(lease.expires_at) <= Date.now()) {
    throw new Error(`Lease "${table}/${pk}" tidak valid, sudah kedaluwarsa, atau bukan milik operator ini.`);
  }
}

function isUniqueConstraintError(error) {
  const code = String(error?.code ?? "");
  return code === "23505" ||
    code === "ER_DUP_ENTRY" ||
    code.includes("SQLITE_CONSTRAINT_PRIMARYKEY") ||
    code.includes("SQLITE_CONSTRAINT_UNIQUE");
}
