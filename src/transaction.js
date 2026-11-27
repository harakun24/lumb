import crypto from "node:crypto";
import { getConnection } from "./connection.js";

const activeTransactions = new Map();

function closeDescendantSavepoints(parentTxId, status) {
  for (const session of [...activeTransactions.values()]) {
    if (session.parentTxId !== parentTxId) continue;
    closeDescendantSavepoints(session.id, status);
    session.status = status;
    activeTransactions.delete(session.id);
  }
}

export async function beginTransaction(parentTxId, directives = {}) {
  if (parentTxId) {
    const parent = activeTransactions.get(parentTxId);
    if (!parent || parent.status !== "active") {
      throw new Error(`Main Transaction: "${parentTxId}" inactive / closed.`);
    }

    const spId = `sp_${crypto.randomBytes(4).toString("hex")}`;
    await parent.client.exec(`SAVEPOINT ${parent.conn.dialect.quoteId(spId)};`);

    activeTransactions.set(spId, {
      id: spId,
      isSavepoint: true,
      parentTxId,
      conn: parent.conn,
      client: parent.client,
      status: "active"
    });

    return spId;
  }

  const conn = getConnection(directives.$connection);
  const timeoutMs = Number(directives.$timeout ?? 15000);
  const txId = `tx_${crypto.randomBytes(4).toString("hex")}`;

  let releaseLock = () => { };
  if (conn.adapter.isSingleConnection) {
    const lockPromise = new Promise((resolve) => {
      releaseLock = resolve;
    });
    const prevLock = conn.txLockQueue;
    conn.txLockQueue = prevLock.then(() => lockPromise);
    await prevLock;
  }

  let client;
  try {
    client = await conn.adapter.acquireDedicated();
    await client.exec(conn.dialect.beginTxSql());
  } catch (err) {
    if (client) await client.release().catch(() => { });
    releaseLock();
    throw err;
  }

  const timer = setTimeout(async () => {
    const session = activeTransactions.get(txId);
    if (session && session.status === "active") {
      await rollbackTransaction(txId);
    }
  }, timeoutMs);
  if (typeof timer.unref === "function") timer.unref();

  activeTransactions.set(txId, {
    id: txId,
    isSavepoint: false,
    conn,
    client,
    status: "active",
    timer,
    releaseLock
  });

  return txId;
}

export async function commitTransaction(txId) {
  const session = activeTransactions.get(txId);
  if (!session || session.status !== "active") {
    throw new Error(`Sesi transaksi "${txId}" tidak valid atau sudah ditutup.`);
  }

  const q = session.conn.dialect.quoteId;

  if (session.isSavepoint) {
    await session.client.exec(`RELEASE SAVEPOINT ${q(txId)};`);
    closeDescendantSavepoints(txId, "committed");
    session.status = "committed";
    activeTransactions.delete(txId);
    return { committed: true, id: txId, savepoint: true };
  }

  clearTimeout(session.timer);
  try {
    await session.client.exec("COMMIT;");
    session.status = "committed";
  } finally {
    closeDescendantSavepoints(txId, session.status ?? "closed");
    activeTransactions.delete(txId);
    await session.client.release().catch(() => { });
    session.releaseLock();
  }

  return { committed: true, id: txId };
}

export async function rollbackTransaction(txId) {
  const session = activeTransactions.get(txId);
  if (!session || session.status !== "active") {
    return { rolledBack: true, id: txId, alreadyClosed: true };
  }

  const q = session.conn.dialect.quoteId;

  if (session.isSavepoint) {
    await session.client.exec(`ROLLBACK TO SAVEPOINT ${q(txId)};`);
    await session.client.exec(`RELEASE SAVEPOINT ${q(txId)};`);
    closeDescendantSavepoints(txId, "rolled_back");
    session.status = "rolled_back";
    activeTransactions.delete(txId);
    return { rolledBack: true, id: txId, savepoint: true };
  }

  clearTimeout(session.timer);
  try {
    await session.client.exec("ROLLBACK;");
    session.status = "rolled_back";
  } finally {
    closeDescendantSavepoints(txId, session.status ?? "closed");
    activeTransactions.delete(txId);
    await session.client.release().catch(() => { });
    session.releaseLock();
  }

  return { rolledBack: true, id: txId };
}

export function resolveExecutionTarget(directives = {}) {
  if (directives.$tx) {
    const session = activeTransactions.get(directives.$tx);
    if (!session || session.status !== "active") {
      throw new Error(`Sesi transaksi "${directives.$tx}" sudah kedaluwarsa atau ditutup.`);
    }
    return { conn: session.conn, runner: session.client };
  }
  const conn = getConnection(directives.$connection);
  return { conn, runner: conn.adapter };
}

export async function executeCrossBatch(commands, dbExecutor, directives = {}) {
  if (!Array.isArray(commands)) {
    throw new Error('Perintah "batch:" wajib menerima Array berisi daftar operasi.');
  }

  const txId = await beginTransaction(null, directives);
  try {
    const results = [];
    for (const entry of commands) {
      const [cmd, payload = {}, opts = {}] = entry;
      if (Array.isArray(payload)) {
        results.push(await dbExecutor(cmd, payload, { ...opts, $tx: txId }));
      } else {
        results.push(await dbExecutor(cmd, payload, { ...opts, $tx: txId }));
      }
    }
    await commitTransaction(txId);
    return results;
  } catch (err) {
    await rollbackTransaction(txId);
    throw err;
  }
}