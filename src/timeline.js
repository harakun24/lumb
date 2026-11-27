import crypto from "node:crypto";
import { executeGet } from "./query.js";
import { assertSchemaReady, hydrateRow } from "./schema.js";
import {
  beginTransaction,
  commitTransaction,
  resolveExecutionTarget,
  rollbackTransaction
} from "./transaction.js";
import { buildWhereClause } from "./parser.js";
import { validateContractChanges } from "./contracts.js";
import { assertMutationLeaseAccess, ensureLeaseStore } from "./lease.js";
import { createAffectedState, MAX_AFFECTED_STATE } from "./operation-guards.js";
import { completeOnce, ensureOnceStore, onceFingerprint, reserveOnce } from "./once.js";
import { enqueueOutbox, ensureOutboxStore } from "./outbox.js";
import { ensureErasureRefStore } from "./erasure-refs.js";

const TABLE_NAME = "_lumb_timeline";
const initializedConnections = new WeakSet();
export const TIMELINE_INTERNAL = Symbol("lumb.timeline.internal");
let lastCreatedAtMs = 0;

export async function ensureTimelineStore(conn) {
  await ensureLeaseStore(conn);
  if (initializedConnections.has(conn)) return;
  const q = conn.dialect.quoteId;
  const payloadType = conn.dialect.name === "mysql" ? "LONGTEXT" : "TEXT";
  const idType = conn.dialect.name === "mysql" ? "VARCHAR(36)" : "TEXT";
  const kindType = conn.dialect.name === "mysql" ? "VARCHAR(32)" : "TEXT";
  const timestampType = conn.dialect.name === "mysql" ? "VARCHAR(40)" : "TEXT";
  await conn.adapter.exec(
    `CREATE TABLE IF NOT EXISTS ${q(TABLE_NAME)} (` +
    `${q("id")} ${idType} PRIMARY KEY, ` +
    `${q("kind")} ${kindType} NOT NULL, ` +
    `${q("created_at")} ${timestampType} NOT NULL, ` +
    `${q("payload")} ${payloadType} NOT NULL);`
  );
  initializedConnections.add(conn);
}

function relationValues(tableDef, row) {
  const values = {};
  if (!row) return values;
  for (const [name, relation] of tableDef.relations) {
    if (relation.kind !== "many-to-many" || !Array.isArray(row[name])) continue;
    values[name] = row[name].map((item) =>
      typeof item === "object" && item !== null ? item[relation.targetKey] : item
    );
  }
  return values;
}

export async function appendTimelineEntry(conn, runner, kind, payload) {
  await ensureTimelineStore(conn);
  const id = crypto.randomUUID();
  const createdAtMs = Math.max(Date.now(), lastCreatedAtMs + 1);
  lastCreatedAtMs = createdAtMs;
  const createdAt = new Date(createdAtMs).toISOString();
  const q = conn.dialect.quoteId;
  await runner.run(
    `INSERT INTO ${q(TABLE_NAME)} (${q("id")}, ${q("kind")}, ${q("created_at")}, ${q("payload")}) VALUES (` +
    `${conn.dialect.param(1)}, ${conn.dialect.param(2)}, ${conn.dialect.param(3)}, ${conn.dialect.param(4)});`,
    [id, kind, createdAt, JSON.stringify(payload)]
  );
  return { id, kind, createdAt, ...payload };
}

export async function executeTrackedMutation(
  parsed,
  payload,
  directives,
  execute,
  emitEvent,
  record = true,
  impactCapture = null
) {
  const { conn } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const onceRequest = directives.$once === undefined
    ? null
    : onceFingerprint(parsed, payload, directives);
  if (onceRequest) await ensureOnceStore(conn);
  if (directives.$outbox !== undefined) await ensureOutboxStore(conn);
  if (onceRequest || directives.$outbox !== undefined) {
    await ensureErasureRefStore(conn);
  }
  await ensureTimelineStore(conn);

  const txId = await beginTransaction(directives.$tx ?? null, directives);
  const txDirectives = { ...directives, $tx: txId };
  let mutationCompleted = false;

  try {
    const { runner } = resolveExecutionTarget(txDirectives);
    if (onceRequest) {
      const reservation = await reserveOnce(
        conn,
        runner,
        onceRequest.key,
        onceRequest.fingerprint
      );
      if (reservation.replayed) {
        await commitTransaction(txId);
        return reservation.result;
      }
    }
    const beforeRows = await selectMutationRows(parsed, payload, txDirectives, conn, runner, tableDef);
    await assertMutationLeaseAccess(conn, runner, tableDef, beforeRows, txDirectives);
    txDirectives[MAX_AFFECTED_STATE] = createAffectedState(
      directives,
      parsed.action,
      beforeRows.length,
      tableDef,
      beforeRows
    );
    const beforeStates = new Map();
    for (const row of beforeRows) {
      beforeStates.set(
        stableJson(row[tableDef.primaryKey]),
        await rowState(conn, runner, tableDef, row)
      );
    }
    const capture = { rows: [] };
    const result = await execute({ ...txDirectives, $timelineCapture: capture });
    mutationCompleted = true;
    const changes = await buildMutationChanges(
      parsed,
      payload,
      result,
      beforeRows,
      beforeStates,
      capture.rows,
      txDirectives,
      conn,
      runner,
      tableDef
    );
    const subjects = mutationSubjects(
      tableDef,
      beforeRows,
      changes,
      result
    );

    if (impactCapture) {
      impactCapture.result = result;
      impactCapture.changes = [...changes, ...(impactCapture.nestedChanges ?? [])];
    }
    await validateContractChanges(conn, changes, parsed.action);
    if (record) {
      await appendTimelineEntry(conn, runner, "change", {
        action: parsed.action,
        table: parsed.table,
        actor: directives.$actor ?? null,
        reason: directives.$reason ?? null,
        source: directives.$source ?? null,
        changes
      });
    }
    if (directives.$outbox !== undefined && changes.length > 0) {
      await enqueueOutbox(conn, runner, directives.$outbox, subjects);
    }
    if (onceRequest) {
      await completeOnce(conn, runner, onceRequest.key, result, subjects);
    }

    await commitTransaction(txId);
    return result;
  } catch (error) {
    if (impactCapture && impactCapture.changes.length === 0) {
      impactCapture.changes = impactCapture.nestedChanges ?? [];
    }
    if (impactCapture && !mutationCompleted) {
      impactCapture.complete = false;
      impactCapture.warnings.push("Eksekusi berhenti sebelum seluruh dampak mutasi dapat dihitung.");
    }
    await rollbackTransaction(txId);
    throw error;
  }
}

function mutationSubjects(tableDef, beforeRows, changes, result) {
  const subjects = new Map();
  const add = (table, pk) => {
    if (typeof table !== "string" || pk === null || pk === undefined) return;
    subjects.set(`${table}\u0000${JSON.stringify(pk)}`, { table, pk });
  };
  for (const row of beforeRows) add(tableDef.name, row[tableDef.primaryKey]);
  for (const change of changes) add(change.table, change.pk);
  const results = Array.isArray(result) ? result : [result];
  for (const row of results) {
    if (row && typeof row === "object") {
      add(tableDef.name, row[tableDef.primaryKey]);
    }
  }
  return [...subjects.values()];
}

async function selectMutationRows(parsed, payload, directives, conn, runner, tableDef) {
  let filters = {};
  if (parsed.action === "patch") {
    if (Array.isArray(payload)) {
      filters[tableDef.primaryKey] = payload.map((row) => row[tableDef.primaryKey]);
    } else if (parsed.pk !== null) {
      filters[tableDef.primaryKey] = parsed.pk;
    } else {
      filters = { ...(directives.$where ?? {}) };
    }
  } else if (parsed.action === "delete") {
    if (parsed.pk !== null) {
      filters[tableDef.primaryKey] = parsed.pk;
    } else if (Array.isArray(payload)) {
      filters[tableDef.primaryKey] = payload;
    } else {
      filters = { ...(directives.$where ?? {}), ...(payload ?? {}) };
    }
    filters = softDeleteFilters(tableDef, filters, directives.$trash);
  } else if (parsed.action === "seed") {
    if (parsed.queryFilters?.sync !== true) return [];
    const rows = Array.isArray(payload) ? payload : [payload];
    filters = seedFilters(tableDef, rows);
    if (Object.keys(filters).length === 0) return [];
  } else if (
    parsed.action === "post" &&
    directives.$onConflict === "update" &&
    Array.isArray(payload)
  ) {
    filters = postConflictFilters(tableDef, payload);
    if (Object.keys(filters).length === 0) return [];
  } else {
    return [];
  }

  if (Object.keys(filters).length === 0) return [];
  return selectRows(conn, runner, tableDef, filters, true);
}

function softDeleteFilters(tableDef, filters, trashMode) {
  if (!tableDef.softDeletePatch) return filters;
  for (const [column, marker] of Object.entries(tableDef.softDeletePatch)) {
    if (column in filters || trashMode === "include") continue;
    const definition = tableDef.columns.get(column);
    const nullMarker = definition?.nullable && marker !== null;
    filters[column] = trashMode === "only"
      ? (nullMarker ? { ne: null } : marker)
      : (nullMarker ? null : (definition?.defaultValue ?? false));
  }
  return filters;
}

async function selectRows(conn, runner, tableDef, filters, lock = false) {
  const q = conn.dialect.quoteId;
  const where = buildWhereClause(filters, conn.schema, tableDef.name, conn.dialect);
  const rawRows = await runner.all(
    `SELECT * FROM ${q(tableDef.name)}${where.sql ? ` WHERE ${where.sql}` : ""}` +
    `${conn.dialect.forUpdateSql(lock)};`,
    where.params
  );
  return rawRows.map((row) => hydrateRow(tableDef, row));
}

async function buildMutationChanges(
  parsed,
  payload,
  result,
  beforeRows,
  beforeStates,
  capturedRows,
  directives,
  conn,
  runner,
  tableDef
) {
  let afterRows = capturedRows;
  if (parsed.action === "post" && Array.isArray(payload) && directives.$onConflict) {
    const filters = postConflictFilters(tableDef, payload);
    const matchedRows = Object.keys(filters).length > 0
      ? await selectRows(conn, runner, tableDef, filters)
      : [];
    afterRows = [...capturedRows, ...matchedRows];
  } else if (parsed.action === "seed") {
    const seedRows = Array.isArray(payload) ? payload : [payload];
    const filters = seedFilters(tableDef, seedRows);
    afterRows = Object.keys(filters).length > 0
      ? await selectRows(conn, runner, tableDef, filters)
      : [];
  } else if (parsed.action === "patch" || parsed.action === "delete") {
    const ids = beforeRows.map((row) => row[tableDef.primaryKey]);
    if (ids.length > 0) {
      afterRows = await selectRows(conn, runner, tableDef, { [tableDef.primaryKey]: ids });
    }
  } else if (parsed.action === "post" && !Array.isArray(payload) && result && typeof result === "object") {
    afterRows = [result];
  }

  const beforeById = new Map(beforeRows.map((row) => [stableJson(row[tableDef.primaryKey]), row]));
  const afterById = new Map(afterRows.map((row) => [stableJson(row[tableDef.primaryKey]), row]));
  const changes = [];

  for (const [key, before] of beforeById) {
    const after = afterById.get(key) ?? null;
    const beforeState = beforeStates.get(key) ?? await rowState(conn, runner, tableDef, before);
    const afterState = after ? await rowState(conn, runner, tableDef, after) : null;
    if (stableJson(beforeState) === stableJson(afterState)) continue;
    changes.push({
      table: tableDef.name,
      pk: before[tableDef.primaryKey],
      operation: after ? "patch" : "delete",
      before: beforeState.row,
      after: afterState?.row ?? null,
      relationsBefore: beforeState.relations,
      relationsAfter: afterState?.relations ?? {}
    });
  }

  for (const [key, after] of afterById) {
    if (beforeById.has(key)) continue;
    const afterState = await rowState(conn, runner, tableDef, after);
    changes.push({
      table: tableDef.name,
      pk: after[tableDef.primaryKey],
      operation: "insert",
      before: null,
      after: afterState.row,
      relationsBefore: {},
      relationsAfter: afterState.relations
    });
  }

  if (parsed.action === "post" && !Array.isArray(payload) && changes.length === 0 && result) {
    const afterState = await rowState(conn, runner, tableDef, result);
    changes.push({
      table: tableDef.name,
      pk: result[tableDef.primaryKey],
      operation: "insert",
      before: null,
      after: afterState.row,
      relationsBefore: {},
      relationsAfter: afterState.relations
    });
  }
  return changes;
}

function seedFilters(tableDef, rows) {
  const matchKey = tableDef.columns.get(tableDef.primaryKey)?.isAutoIncrement
    ? [...tableDef.columns.values()].find((column) => column.unique && !column.isPrimaryKey)?.name ?? tableDef.primaryKey
    : tableDef.primaryKey;
  const values = rows.map((row) => row?.[matchKey]).filter((value) => value !== undefined && value !== null);
  return values.length > 0 ? { [matchKey]: values } : {};
}

function postConflictFilters(tableDef, rows) {
  const matchKey = tableDef.columns.get(tableDef.primaryKey)?.isAutoIncrement
    ? [...tableDef.columns.values()].find((column) => column.unique && !column.isPrimaryKey)?.name ?? tableDef.primaryKey
    : tableDef.primaryKey;
  const values = rows.map((row) => row?.[matchKey]).filter((value) => value !== undefined && value !== null);
  return values.length === rows.length && values.length > 0
    ? { [matchKey]: values }
    : {};
}

async function rowState(conn, runner, tableDef, row) {
  const relations = {};
  for (const [name, relation] of tableDef.relations) {
    if (relation.kind !== "many-to-many") continue;
    const q = conn.dialect.quoteId;
    const rows = await runner.all(
      `SELECT ${q(relation.junctionTargetKey)} AS target_id FROM ${q(relation.junctionTable)} WHERE ${q(relation.junctionLocalKey)} = ${conn.dialect.param(1)};`,
      [conn.dialect.serialize(row[relation.localKey])]
    );
    relations[name] = rows.map((item) => item.target_id);
  }
  return { row: persistedColumns(tableDef, row), relations };
}

export async function getTimelineEntry(conn, runner, id) {
  await ensureTimelineStore(conn);
  const q = conn.dialect.quoteId;
  const row = await runner.get(
    `SELECT ${q("id")}, ${q("kind")}, ${q("created_at")}, ${q("payload")} FROM ${q(TABLE_NAME)} WHERE ${q("id")} = ${conn.dialect.param(1)};`,
    [id]
  );
  return row ? decodeEntry(row) : null;
}

export async function listTimelineEntries(conn, runner, kind) {
  await ensureTimelineStore(conn);
  const q = conn.dialect.quoteId;
  const rows = await runner.all(
    `SELECT ${q("id")}, ${q("kind")}, ${q("created_at")}, ${q("payload")} FROM ${q(TABLE_NAME)} WHERE ${q("kind")} = ${conn.dialect.param(1)} ORDER BY ${q("created_at")} DESC;`,
    [kind]
  );
  return rows.map(decodeEntry);
}

export async function scrubTimelineData(conn, runner, tableName, primaryKey, fields) {
  await ensureTimelineStore(conn);
  const q = conn.dialect.quoteId;
  const rows = await runner.all(
    `SELECT ${q("id")}, ${q("kind")}, ${q("created_at")}, ${q("payload")} ` +
    `FROM ${q(TABLE_NAME)};`
  );
  const targetKey = stableJson(primaryKey);

  for (const row of rows) {
    const entry = decodeEntry(row);
    let changed = false;
    if (entry.kind === "change") {
      for (const change of entry.changes ?? []) {
        if (change.table !== tableName || stableJson(change.pk) !== targetKey) continue;
        changed = scrubRecordFields(change.before, fields) || changed;
        changed = scrubRecordFields(change.after, fields) || changed;
      }
    } else if (entry.kind === "snapshot" && entry.table && entry.rows) {
      const snapshotState = { unidentifiedTarget: false };
      changed = scrubSnapshotRows(
        entry.rows,
        conn.schema,
        conn.schema.tables.get(entry.table),
        tableName,
        targetKey,
        fields,
        snapshotState
      );
      if (entry.table === tableName) {
        changed = removeFilterFields(entry.filters, fields) || changed;
      }
      changed = scrubSnapshotWithFilters(
        entry.with,
        conn.schema,
        conn.schema.tables.get(entry.table),
        tableName,
        fields
      ) || changed;
      if (
        snapshotState.unidentifiedTarget ||
        snapshotWithContainsFlatTarget(
          entry.with,
          conn.schema,
          conn.schema.tables.get(entry.table),
          tableName
        )
      ) {
        entry.rows = [];
        entry.status = "forgotten";
        changed = true;
      }
    } else if (
      entry.kind === "branch" &&
      entry.table === tableName &&
      stableJson(entry.pk) === targetKey
    ) {
      entry.base = null;
      entry.baseRelations = {};
      entry.commands = [];
      entry.status = "forgotten";
      changed = true;
    }

    if (changed) {
      await updateTimelineEntry(conn, runner, entry.id, entry.kind, entry);
    }
  }
}

async function updateTimelineEntry(conn, runner, id, kind, payload) {
  const q = conn.dialect.quoteId;
  await runner.run(
    `UPDATE ${q(TABLE_NAME)} SET ${q("payload")} = ${conn.dialect.param(1)} WHERE ${q("id")} = ${conn.dialect.param(2)} AND ${q("kind")} = ${conn.dialect.param(3)};`,
    [JSON.stringify(payload), id, kind]
  );
}

function scrubRecordFields(row, fields) {
  if (!row || typeof row !== "object") return false;
  let changed = false;
  for (const field of fields) {
    if (Object.prototype.hasOwnProperty.call(row, field) && row[field] !== null) {
      row[field] = null;
      changed = true;
    }
  }
  return changed;
}

function scrubSnapshotRows(
  rows,
  schema,
  tableDef,
  targetTable,
  targetKey,
  fields,
  snapshotState
) {
  if (!Array.isArray(rows) || !tableDef) return false;
  let changed = false;
  for (const row of rows) {
    if (!row || typeof row !== "object") continue;
    if (tableDef.name === targetTable) {
      if (row[tableDef.primaryKey] === undefined || row[tableDef.primaryKey] === null) {
        snapshotState.unidentifiedTarget = true;
      } else if (stableJson(row[tableDef.primaryKey]) === targetKey) {
        changed = scrubRecordFields(row, fields) || changed;
      }
    }
    for (const relationName of Object.keys(row)) {
      const relation = tableDef.relations.get(relationName);
      const relatedTable = schema.tables.get(
        relation?.targetTable ?? (schema.tables.has(relationName) ? relationName : null)
      );
      if (!relatedTable || row[relationName] === undefined) continue;
      const relatedRows = Array.isArray(row[relationName])
        ? row[relationName]
        : [row[relationName]];
      changed = scrubSnapshotRows(
        relatedRows,
        schema,
        relatedTable,
        targetTable,
        targetKey,
        fields,
        snapshotState
      ) || changed;
    }
  }
  return changed;
}

function snapshotWithContainsFlatTarget(withConfig, schema, tableDef, targetTable) {
  if (
    !withConfig ||
    typeof withConfig !== "object" ||
    Array.isArray(withConfig) ||
    !tableDef
  ) {
    return false;
  }
  for (const [name, rawConfig] of Object.entries(withConfig)) {
    const relation = tableDef.relations.get(name);
    const relatedTable = schema.tables.get(relation?.targetTable ?? name);
    if (!relatedTable) continue;
    const config = rawConfig && typeof rawConfig === "object" && !Array.isArray(rawConfig)
      ? rawConfig
      : {};
    if (relatedTable.name === targetTable && config.$flat) return true;
    const nestedWith = config.$with &&
        typeof config.$with === "object" &&
        !Array.isArray(config.$with)
      ? config.$with
      : {};
    for (const [key, value] of Object.entries(config)) {
      if (relatedTable.relations.has(key)) nestedWith[key] = value;
    }
    if (snapshotWithContainsFlatTarget(nestedWith, schema, relatedTable, targetTable)) {
      return true;
    }
  }
  return false;
}

function scrubSnapshotWithFilters(withConfig, schema, tableDef, targetTable, fields) {
  if (
    !withConfig ||
    typeof withConfig !== "object" ||
    Array.isArray(withConfig) ||
    !tableDef
  ) {
    return false;
  }
  let changed = false;
  for (const [name, rawConfig] of Object.entries(withConfig)) {
    const relation = tableDef.relations.get(name);
    const relatedTable = schema.tables.get(relation?.targetTable ?? name);
    if (!relatedTable) continue;
    const config = rawConfig && typeof rawConfig === "object" && !Array.isArray(rawConfig)
      ? rawConfig
      : null;
    if (!config) continue;

    if (relatedTable.name === targetTable) {
      for (const field of fields) {
        if (
          relatedTable.columns.has(field) &&
          !relatedTable.relations.has(field) &&
          Object.prototype.hasOwnProperty.call(config, field)
        ) {
          delete config[field];
          changed = true;
        }
      }
    }

    const nestedWith = config.$with &&
        typeof config.$with === "object" &&
        !Array.isArray(config.$with)
      ? config.$with
      : {};
    for (const [key, value] of Object.entries(config)) {
      if (relatedTable.relations.has(key)) nestedWith[key] = value;
    }
    changed = scrubSnapshotWithFilters(
      nestedWith,
      schema,
      relatedTable,
      targetTable,
      fields
    ) || changed;
  }
  return changed;
}

function removeFilterFields(filters, fields) {
  if (!filters || typeof filters !== "object") return false;
  let changed = false;
  for (const key of Object.keys(filters)) {
    if (fields.includes(key)) {
      delete filters[key];
      changed = true;
    } else {
      changed = removeFilterFields(filters[key], fields) || changed;
    }
  }
  return changed;
}

export async function executeCreateBranch(parsed, directives) {
  if (!parsed.table || parsed.pk === null) {
    throw new Error('Gunakan "branch:<tabel>/<pk>" untuk membuat branch dari satu record.');
  }
  const { conn } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  await ensureTimelineStore(conn);
  const txId = await beginTransaction(directives.$tx ?? null, directives);
  const txDirectives = { ...directives, $tx: txId };
  try {
    const { runner } = resolveExecutionTarget(txDirectives);
    const row = await readRow(conn, txDirectives, tableDef, parsed.pk);
    if (!row) throw new Error(`Record "${parsed.table}/${parsed.pk}" tidak ditemukan.`);
    const baseState = await rowState(conn, runner, tableDef, row);

    const branch = await appendTimelineEntry(conn, runner, "branch", {
      table: parsed.table,
      pk: parsed.pk,
      base: baseState.row,
      baseRelations: baseState.relations,
      label: directives.$label ?? null,
      status: "open",
      commands: []
    });
    await commitTransaction(txId);
    return { id: branch.id, table: branch.table, pk: branch.pk, label: branch.label };
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

export async function stageBranchMutation(parsed, payload, directives) {
  const branchId = directives.$branch;
  const { conn } = resolveExecutionTarget(directives);
  await ensureTimelineStore(conn);
  const txId = await beginTransaction(directives.$tx ?? null, directives);
  const txDirectives = { ...directives, $tx: txId };
  try {
    const { runner } = resolveExecutionTarget(txDirectives);
    const branch = await getTimelineEntry(conn, runner, branchId);
    if (!branch || branch.kind !== "branch" || branch.status !== "open") {
      throw new Error(`Branch "${branchId}" tidak ditemukan atau sudah ditutup.`);
    }
    if (parsed.table !== branch.table) {
      throw new Error(`Branch "${branchId}" hanya menerima operasi pada tabel "${branch.table}".`);
    }
    if ((parsed.action === "patch" || parsed.action === "delete") &&
        String(parsed.pk) !== String(branch.pk)) {
      throw new Error("Branch record hanya dapat mengubah atau menghapus record yang menjadi dasarnya.");
    }
    if (!["post", "patch", "delete"].includes(parsed.action)) {
      throw new Error(`Operasi "${parsed.action}" belum didukung di dalam branch.`);
    }

    const command = `${parsed.action}:${parsed.table}${parsed.pk === null ? "" : `/${encodeURIComponent(parsed.pk)}`}`;
    const storedDirectives = { ...directives };
    delete storedDirectives.$branch;
    delete storedDirectives.$tx;
    delete storedDirectives.$timeline;
    delete storedDirectives.$timelineCapture;
    const commands = [
      ...branch.commands,
      { command, payload, directives: storedDirectives }
    ];
    await updateTimelineEntry(conn, runner, branchId, "branch", {
      ...branch,
      commands
    });
    await commitTransaction(txId);
    return { branch: branchId, staged: commands.length };
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

export async function executeBranchPreview(parsed, filters, directives, dbExecutor, emitEvent) {
  const branchId = directives.$branch;
  const { conn } = resolveExecutionTarget(directives);
  const txId = await beginTransaction(directives.$tx ?? null, directives);
  try {
    const txDirectives = { ...directives, $tx: txId };
    const { runner } = resolveExecutionTarget(txDirectives);
    const branch = await getTimelineEntry(conn, runner, branchId);
    if (!branch || branch.kind !== "branch" || branch.status !== "open") {
      throw new Error(`Branch "${branchId}" tidak ditemukan atau sudah ditutup.`);
    }
    if (parsed.table !== branch.table) {
      throw new Error(`Branch "${branchId}" hanya dapat membaca tabel "${branch.table}".`);
    }
    const tableDef = assertSchemaReady(conn, branch.table);
    const current = await readRowState(conn, txDirectives, tableDef, branch.pk);
    if (stableJson(current?.row ?? null) !== stableJson(branch.base) ||
        stableJson(current?.relations ?? {}) !== stableJson(branch.baseRelations ?? {})) {
      throw new Error(`Branch "${branchId}" konflik karena data dasarnya telah berubah.`);
    }

    await replayBranch(branch, txId, dbExecutor, true);
    const readDirectives = { ...txDirectives };
    delete readDirectives.$branch;
    delete readDirectives.$purpose;
    const result = await dbExecutor(
      `get:${parsed.table}${parsed.pk === null ? "" : `/${encodeURIComponent(parsed.pk)}`}`,
      filters,
      readDirectives
    );
    await rollbackTransaction(txId);
    emitEvent("branch:preview", { id: branchId });
    return result;
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

export async function executeMergeBranch(parsed, directives, dbExecutor, emitEvent) {
  const branchId = parsed.table;
  const { conn } = resolveExecutionTarget(directives);
  const txId = await beginTransaction(directives.$tx ?? null, directives);
  try {
    const txDirectives = { ...directives, $tx: txId };
    const { runner } = resolveExecutionTarget(txDirectives);
    const branch = await getTimelineEntry(conn, runner, branchId);
    if (!branch || branch.kind !== "branch" || branch.status !== "open") {
      throw new Error(`Branch "${branchId}" tidak ditemukan atau sudah ditutup.`);
    }
    const tableDef = assertSchemaReady(conn, branch.table);
    const current = await readRowState(conn, txDirectives, tableDef, branch.pk);
    const conflict = stableJson(current?.row ?? null) !== stableJson(branch.base) ||
      stableJson(current?.relations ?? {}) !== stableJson(branch.baseRelations ?? {});
    if (conflict) {
      await rollbackTransaction(txId);
      return { branch: branchId, merged: false, conflicts: [{ table: branch.table, pk: branch.pk }] };
    }

    const results = await replayBranch(branch, txId, dbExecutor, false);
    if (directives.$dryRun) {
      await rollbackTransaction(txId);
      return { branch: branchId, dryRun: true, merged: false, results };
    }

    const txTarget = resolveExecutionTarget(txDirectives);
    await updateTimelineEntry(conn, txTarget.runner, branchId, "branch", {
      ...branch,
      status: "merged"
    });
    await commitTransaction(txId);
    emitEvent("branch:merge", { id: branchId });
    return { branch: branchId, merged: true, results };
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

export async function discardBranch(parsed, directives) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const branch = await getTimelineEntry(conn, runner, parsed.table);
  if (!branch || branch.kind !== "branch" || branch.status !== "open") {
    throw new Error(`Branch "${parsed.table}" tidak ditemukan atau sudah ditutup.`);
  }
  await updateTimelineEntry(conn, runner, branch.id, "branch", {
    ...branch,
    status: "discarded"
  });
  return { branch: branch.id, discarded: true };
}

async function replayBranch(branch, txId, dbExecutor, internal) {
  const results = [];
  for (const item of branch.commands) {
    const options = { ...item.directives, $tx: txId };
    if (internal) options[TIMELINE_INTERNAL] = true;
    results.push(await dbExecutor(item.command, item.payload, options));
  }
  return results;
}

function decodeEntry(row) {
  const payload = JSON.parse(row.payload);
  return {
    id: row.id,
    kind: row.kind,
    createdAt: row.created_at,
    ...payload
  };
}

export function stableJson(value) {
  return JSON.stringify(value, (_, item) => {
    if (typeof item === "bigint") return item.toString();
    return item;
  });
}

export async function executeHistory(parsed, directives) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const entries = await listTimelineEntries(conn, runner, "change");
  return entries.filter((entry) => {
    if (parsed.table && !entry.changes.some((change) => change.table === parsed.table)) return false;
    if (parsed.pk !== null && !entry.changes.some((change) => String(change.pk) === String(parsed.pk))) return false;
    if (directives.$actor && stableJson(entry.actor) !== stableJson(directives.$actor)) return false;
    return true;
  });
}

export async function executeAt(parsed, directives, emitEvent) {
  if (parsed.pk === null) {
    throw new Error('Direktif "$at" hanya mendukung pembacaan satu record, gunakan "get:<tabel>/<pk>".');
  }
  if (directives.$with !== undefined) {
    throw new Error('Direktif "$with" belum didukung bersama "$at"; relasi historis tidak direkonstruksi.');
  }
  if (Object.keys(parsed.queryFilters ?? {}).length > 0) {
    throw new Error('Filter URL tambahan belum didukung bersama "$at"; targetkan record dengan primary key.');
  }
  if (!(directives.$at instanceof Date) && typeof directives.$at !== "string") {
    throw new Error('"$at" wajib berupa Date atau timestamp string yang valid.');
  }
  const timestamp = directives.$at instanceof Date
    ? directives.$at
    : new Date(directives.$at);
  if (Number.isNaN(timestamp.getTime())) {
    throw new Error('"$at" wajib berupa Date atau timestamp yang valid.');
  }

  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const entries = await listTimelineEntries(conn, runner, "change");
  const targetChanges = [];
  for (const entry of entries) {
    for (const change of entry.changes ?? []) {
      if (change.table === parsed.table && String(change.pk) === String(parsed.pk)) {
        targetChanges.push({ createdAt: entry.createdAt, change });
      }
    }
  }
  if (targetChanges.length === 0) {
    throw new Error(
      `Timeline tidak memiliki riwayat yang cukup untuk membaca "${parsed.table}/${parsed.pk}" pada waktu tersebut.`
    );
  }

  const atIso = timestamp.toISOString();
  const latestBefore = targetChanges.find((item) => item.createdAt <= atIso);
  const state = latestBefore
    ? latestBefore.change.after
    : targetChanges[targetChanges.length - 1].change.before;
  const row = state ? hydrateRow(tableDef, state) : null;
  let result = row;
  if (row && directives.$fields !== undefined) {
    if (!Array.isArray(directives.$fields) || directives.$fields.length === 0) {
      throw new Error('Direktif "$fields" bersama "$at" wajib berupa Array tidak kosong.');
    }
    for (const field of directives.$fields) {
      if (!tableDef.columns.has(field) && !tableDef.getters.has(field)) {
        throw new Error(`Kolom "${field}" tidak ditemukan pada tabel "${parsed.table}".`);
      }
    }
    result = Object.fromEntries(
      directives.$fields.filter((field) => Object.prototype.hasOwnProperty.call(row, field))
        .map((field) => [field, row[field]])
    );
  }
  emitEvent("query", {
    action: `get:${parsed.table}:at`,
    at: timestamp.toISOString(),
    durationMs: 0
  });
  return result;
}

export async function executeWhy(parsed, directives) {
  const field = parsed.subResource ?? parsed.queryFilters?.field ?? directives.$field;
  if (!parsed.table || parsed.pk === null || !field) {
    throw new Error('Gunakan "why:<tabel>/<pk>/<field>" untuk menelusuri asal nilai.');
  }
  const { conn, runner } = resolveExecutionTarget(directives);
  const entries = await listTimelineEntries(conn, runner, "change");
  const history = [];

  for (const entry of entries) {
    for (const change of entry.changes) {
      if (change.table !== parsed.table || String(change.pk) !== String(parsed.pk)) continue;
      if (!hasFieldChanged(change, field)) continue;
      history.push({
        changeId: entry.id,
        createdAt: entry.createdAt,
        actor: entry.actor ?? null,
        reason: entry.reason ?? null,
        source: entry.source ?? null,
        operation: change.operation,
        before: change.before?.[field] ?? null,
        after: change.after?.[field] ?? null
      });
    }
  }

  return history;
}

function hasFieldChanged(change, field) {
  if (!change.before || !change.after) {
    return Boolean(change.before?.[field] !== undefined || change.after?.[field] !== undefined);
  }
  return stableJson(change.before[field]) !== stableJson(change.after[field]);
}

export async function executeSnapshot(parsed, directives, emitEvent) {
  if (!parsed.table) throw new Error('Perintah "snapshot:" wajib menyebutkan tabel.');
  const { conn, runner } = resolveExecutionTarget(directives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  const filters = { ...parsed.queryFilters, ...(directives.$where ?? {}) };
  if (parsed.pk !== null) filters[tableDef.primaryKey] = parsed.pk;
  const snapshotDirectives = {
    $connection: directives.$connection,
    $tx: directives.$tx,
    $with: directives.$with,
    $trash: directives.$trash
  };
  const rows = await executeGet(
    { action: "get", table: parsed.table, pk: null, subResource: null },
    filters,
    snapshotDirectives,
    emitEvent
  );
  const snapshot = await appendTimelineEntry(conn, runner, "snapshot", {
    table: parsed.table,
    primaryKey: tableDef.primaryKey,
    filters,
    with: directives.$with ?? null,
    trash: directives.$trash ?? null,
    label: directives.$label ?? null,
    rows
  });
  return {
    id: snapshot.id,
    table: snapshot.table,
    label: snapshot.label,
    createdAt: snapshot.createdAt,
    rowCount: rows.length
  };
}

export async function executeDiff(parsed, directives, emitEvent) {
  const snapshotId = directives.$from ?? directives.$snapshot;
  if (!snapshotId) throw new Error('Perintah "diff:" wajib menyertakan "$from" atau "$snapshot".');
  const { conn, runner } = resolveExecutionTarget(directives);
  const snapshot = await getTimelineEntry(conn, runner, snapshotId);
  if (!snapshot || snapshot.kind !== "snapshot") {
    throw new Error(`Snapshot "${snapshotId}" tidak ditemukan.`);
  }
  if (snapshot.status === "forgotten") {
    throw new Error(`Snapshot "${snapshotId}" tidak dapat digunakan karena data di dalamnya telah dilupakan.`);
  }
  if (parsed.table && parsed.table !== snapshot.table) {
    throw new Error(`Snapshot "${snapshotId}" berisi tabel "${snapshot.table}", bukan "${parsed.table}".`);
  }

  const tableDef = assertSchemaReady(conn, snapshot.table);
  const currentRows = await executeGet(
    { action: "get", table: snapshot.table, pk: null, subResource: null },
    snapshot.filters,
    {
      $connection: directives.$connection,
      $tx: directives.$tx,
      $with: snapshot.with ?? undefined,
      $trash: snapshot.trash
    },
    emitEvent
  );
  const keyOf = (row) => stableJson(row[tableDef.primaryKey]);
  const before = new Map(snapshot.rows.map((row) => [keyOf(row), row]));
  const after = new Map(currentRows.map((row) => [keyOf(row), row]));
  const changes = [];

  for (const [key, oldRow] of before) {
    const newRow = after.get(key);
    if (!newRow) {
      changes.push({ operation: "missing", pk: oldRow[tableDef.primaryKey], before: oldRow, after: null });
    } else if (stableJson(oldRow) !== stableJson(newRow)) {
      changes.push({ operation: "changed", pk: oldRow[tableDef.primaryKey], before: oldRow, after: newRow });
    }
  }
  for (const [key, newRow] of after) {
    if (!before.has(key)) {
      changes.push({ operation: "added", pk: newRow[tableDef.primaryKey], before: null, after: newRow });
    }
  }
  return { snapshot: snapshot.id, table: snapshot.table, changes };
}

export async function executeUndo(parsed, directives, dbExecutor, emitEvent) {
  const { conn, runner } = resolveExecutionTarget(directives);
  const entry = await getTimelineEntry(conn, runner, parsed.table);
  if (!entry || entry.kind !== "change") {
    throw new Error(`Perubahan "${parsed.table}" tidak ditemukan atau tidak dapat dibatalkan.`);
  }
  const inverse = entry.changes.map((change) => ({
    ...change,
    operation: change.after === null
      ? "insert"
      : change.before === null
        ? "delete"
        : "patch",
    before: change.after,
    after: change.before,
    relationsBefore: change.relationsAfter,
    relationsAfter: change.relationsBefore
  }));
  if (directives.$dryRun) {
    const conflicts = [];
    for (const change of inverse) {
      const tableDef = assertSchemaReady(conn, change.table);
      const current = await readRow(conn, directives, tableDef, change.pk);
      const currentRow = current ? persistedColumns(tableDef, current) : null;
      const currentRelations = current
        ? (await readRowState(conn, directives, tableDef, change.pk))?.relations
        : {};
      if (stableJson(currentRow) !== stableJson(change.before ?? null) ||
          (Object.keys(change.relationsBefore ?? {}).length > 0 &&
           stableJson(currentRelations) !== stableJson(change.relationsBefore))) {
        conflicts.push({ table: change.table, pk: change.pk });
      }
    }
    return { changeId: entry.id, dryRun: true, conflicts, changes: inverse };
  }
  return applyChangeRows(inverse, entry, directives, conn, dbExecutor, emitEvent, "undo");
}

export async function executeRestore(parsed, directives, dbExecutor, emitEvent) {
  const snapshotId = directives.$from ?? directives.$snapshot;
  if (!snapshotId) throw new Error('Perintah "restore:" wajib menyertakan "$from" atau "$snapshot".');
  const { conn } = resolveExecutionTarget(directives);
  const txId = await beginTransaction(directives.$tx ?? null, directives);
  const txDirectives = { ...directives, $tx: txId };
  try {
    const { runner } = resolveExecutionTarget(txDirectives);
    const snapshot = await getTimelineEntry(conn, runner, snapshotId);
    if (!snapshot || snapshot.kind !== "snapshot") {
      throw new Error(`Snapshot "${snapshotId}" tidak ditemukan.`);
    }
    if (snapshot.status === "forgotten") {
      throw new Error(`Snapshot "${snapshotId}" tidak dapat digunakan karena data di dalamnya telah dilupakan.`);
    }
    if (parsed.table && parsed.table !== snapshot.table) {
      throw new Error(`Snapshot "${snapshotId}" berisi tabel "${snapshot.table}", bukan "${parsed.table}".`);
    }
    const tableDef = assertSchemaReady(conn, snapshot.table);

    const diff = await executeDiff(
      { action: "diff", table: snapshot.table, pk: null, queryFilters: {} },
      { ...txDirectives, $from: snapshotId },
      emitEvent
    );
    const changes = [];
    for (const item of diff.changes) {
      if (item.operation === "added" && directives.$mode !== "replace") continue;
      const relationsBefore = relationValues(tableDef, item.after);
      const relationsAfter = relationValues(tableDef, item.before);
      const rowChanged = stableJson(persistedColumns(tableDef, item.after)) !==
        stableJson(persistedColumns(tableDef, item.before));
      const relationsChanged = stableJson(relationsBefore) !== stableJson(relationsAfter);
      if (!rowChanged && !relationsChanged) continue;
      changes.push({
        table: snapshot.table,
        pk: item.pk,
        before: item.after ? persistedColumns(tableDef, item.after) : null,
        after: item.before ? persistedColumns(tableDef, item.before) : null,
        relationsBefore,
        relationsAfter,
        operation: item.before === null ? "delete" : item.after === null ? "insert" : "patch"
      });
    }

    if (directives.$dryRun) {
      await rollbackTransaction(txId);
      return { snapshot: snapshot.id, table: snapshot.table, changes };
    }
    const result = await applyChangeRows(changes, {
      table: snapshot.table,
      actor: directives.$actor,
      reason: directives.$reason ?? `Restore snapshot ${snapshot.id}`,
      source: directives.$source ?? null
    }, txDirectives, conn, dbExecutor, emitEvent, "restore");
    await commitTransaction(txId);
    return result;
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

async function applyChangeRows(changes, sourceEntry, directives, conn, dbExecutor, emitEvent, kind) {
  const txId = await beginTransaction(directives.$tx ?? null, directives);
  const txDirectives = { ...directives, $tx: txId };
  const internalOptions = { ...txDirectives, [TIMELINE_INTERNAL]: true };
  const applied = [];
  try {
    for (const change of changes) {
      const tableDef = assertSchemaReady(conn, change.table);
      const before = await readRowState(conn, txDirectives, tableDef, change.pk);
      if (stableJson(before?.row ?? null) !== stableJson(change.before ?? null) ||
          (Object.keys(change.relationsBefore ?? {}).length > 0 &&
           stableJson(before?.relations ?? {}) !== stableJson(change.relationsBefore))) {
        throw new Error(`Konflik pada "${change.table}/${change.pk}": data berubah sejak rencana dibuat.`);
      }
      if (change.operation === "delete") {
        await dbExecutor(`delete:${change.table}/${encodeURIComponent(change.pk)}`, {}, { ...internalOptions, $hard: true });
      } else if (change.operation === "insert") {
        const insertPayload = {
          ...persistedColumns(tableDef, change.after),
          ...change.relationsAfter
        };
        await dbExecutor(`post:${change.table}`, insertPayload, internalOptions);
      } else {
        const updates = persistedColumns(tableDef, change.after);
        delete updates[tableDef.primaryKey];
        Object.assign(updates, change.relationsAfter ?? {});
        await dbExecutor(`patch:${change.table}/${encodeURIComponent(change.pk)}`, updates, internalOptions);
      }
      const after = await readRowState(conn, txDirectives, tableDef, change.pk);
      applied.push({
        table: change.table,
        pk: change.pk,
        before: before?.row ?? null,
        after: after?.row ?? null,
        relationsBefore: before?.relations ?? {},
        relationsAfter: after?.relations ?? {},
        operation: before === null ? "insert" : after === null ? "delete" : "patch"
      });
    }
    const { runner } = resolveExecutionTarget(txDirectives);
    const result = await appendTimelineEntry(conn, runner, "change", {
      action: kind,
      table: sourceEntry.table ?? null,
      actor: directives.$actor ?? sourceEntry.actor ?? null,
      reason: directives.$reason ?? sourceEntry.reason ?? null,
      source: directives.$source ?? sourceEntry.source ?? null,
      changes: applied
    });
    await commitTransaction(txId);
    emitEvent(`${kind}:${result.id}`, result);
    return { applied: applied.length, changeId: result.id, changes: applied };
  } catch (error) {
    await rollbackTransaction(txId);
    throw error;
  }
}

async function readRow(conn, directives, tableDef, pk) {
  const { runner } = resolveExecutionTarget(directives);
  const q = conn.dialect.quoteId;
  const raw = await runner.get(
    `SELECT * FROM ${q(tableDef.name)} WHERE ${q(tableDef.primaryKey)} = ${conn.dialect.param(1)};`,
    [conn.dialect.serialize(pk)]
  );
  return raw ? hydrateRow(tableDef, raw) : null;
}

async function readRowState(conn, directives, tableDef, pk) {
  const row = await readRow(conn, directives, tableDef, pk);
  if (!row) return null;
  const { runner } = resolveExecutionTarget(directives);
  return rowState(conn, runner, tableDef, row);
}

function persistedColumns(tableDef, row) {
  const result = {};
  for (const column of tableDef.columns.keys()) {
    if (Object.prototype.hasOwnProperty.call(row ?? {}, column)) result[column] = row[column];
  }
  return result;
}
