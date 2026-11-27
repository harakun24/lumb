import { parseCommand, splitDirectives } from "./parser.js";
import { closeDatabase, connectDatabase } from "./connection.js";
import { defineTable, dropTable, inspectTable } from "./schema.js";
import { executeDump, executeExplain, executeGet } from "./query.js";
import { executeDelete, executePatch, executePost, executeSeed } from "./mutation.js";
import {
  beginTransaction,
  commitTransaction,
  executeCrossBatch,
  resolveExecutionTarget,
  rollbackTransaction
} from "./transaction.js";
import { createScopedDb } from "./scope.js";
import { executeImpact } from "./impact.js";
import { IMPACT_CAPTURE } from "./contracts.js";
import {
  executeClaim,
  executeLeaseStatus,
  executeRelease,
  executeRenew
} from "./lease.js";
import { ensureOutboxStore, executeOutbox } from "./outbox.js";
import { ensureOnceStore } from "./once.js";
import { ensureErasureRefStore } from "./erasure-refs.js";
import {
  ensureAccessStore,
  executeAccess,
  normalizeAccessRequest,
  recordAccess
} from "./access.js";
import { executeForget } from "./forget.js";
import {
  executeDiff,
  executeCreateBranch,
  executeAt,
  executeBranchPreview,
  executeHistory,
  executeMergeBranch,
  executeRestore,
  executeSnapshot,
  discardBranch,
  stageBranchMutation,
  executeTrackedMutation,
  executeUndo,
  executeWhy,
  ensureTimelineStore,
  TIMELINE_INTERNAL
} from "./timeline.js";

const eventListeners = new Map();

function emitEvent(eventName, detail) {
  const exact = eventListeners.get(eventName);
  if (exact) {
    for (const fn of exact) fn(detail);
  }
  const wildcard = eventListeners.get("*");
  if (wildcard) {
    for (const fn of wildcard) fn({ event: eventName, detail });
  }
}

export function db(command, payload = {}, options = {}) {
  return executePublicCommandString(command, payload, options);
}

function executePublicCommandString(command, payload, options, eventSink = emitEvent) {
  const parsed = parseCommand(command);

  if (parsed.action === "scope") {
    return createScopedDb(parsed.table, payload, db);
  }

  return executePublicCommand(parsed, payload, options, eventSink);
}

async function executePublicCommand(parsed, payload, options, eventSink = emitEvent) {
  const bodyData = Array.isArray(payload) ? {} : splitDirectives(payload).data;
  const bodyDirectives = Array.isArray(payload)
    ? {}
    : splitDirectives(payload).directives;
  const optionDirectives = splitDirectives(options).directives;
  const redactFields = normalizeRedactFields({
    ...bodyDirectives,
    ...optionDirectives
  }.$redact);
  const publicEventSink = redactFields
    ? (name, detail) => {
        const safeDetail = redactValue(detail, redactFields);
        if (name === "query" && safeDetail && typeof safeDetail === "object") {
          delete safeDetail.params;
        }
        eventSink(name, safeDetail);
      }
    : eventSink;
  const directives = { ...bodyDirectives, ...optionDirectives };
  let accessConn = null;
  let accessRequest = null;
  if (directives.$purpose !== undefined && parsed.action !== "access") {
    if (
      !["get", "dump"].includes(parsed.action) ||
      directives.$tx !== undefined ||
      directives.$stream !== undefined
    ) {
      normalizeAccessRequest(parsed, directives, {}, null);
    }
    accessConn = accessConnection(directives);
    accessRequest = normalizeAccessRequest(
      parsed,
      directives,
      { ...parsed.queryFilters, ...bodyData },
      accessConn
    );
  }
  if (accessRequest) await ensureAccessStore(accessConn);
  const result = await executeAsyncCommand(parsed, payload, options, publicEventSink);
  if (accessRequest) await recordAccess(accessConn, accessRequest, result);
  return redactFields ? redactValue(result, redactFields) : result;
}

function accessConnection(directives) {
  return resolveExecutionTarget(directives).conn;
}

function normalizeRedactFields(value) {
  if (value === undefined) return null;
  if (
    !Array.isArray(value) ||
    value.length === 0 ||
    value.some((field) => typeof field !== "string" || field.trim().length === 0)
  ) {
    throw new Error('Direktif "$redact" wajib berupa Array berisi nama kolom string yang tidak kosong.');
  }
  return new Set(value);
}

function redactValue(value, fields, seen = new WeakMap()) {
  if (value === null || typeof value !== "object" || value instanceof Date) return value;
  if (typeof value[Symbol.asyncIterator] === "function") {
    return {
      async *[Symbol.asyncIterator]() {
        for await (const item of value) {
          yield redactValue(item, fields);
        }
      }
    };
  }
  if (seen.has(value)) return seen.get(value);
  if (Array.isArray(value)) {
    const result = [];
    seen.set(value, result);
    for (const item of value) result.push(redactValue(item, fields, seen));
    return result;
  }

  const result = Object.create(Object.getPrototypeOf(value));
  seen.set(value, result);
  for (const [key, item] of Object.entries(value)) {
    Object.defineProperty(result, key, {
      value: fields.has(key) ? "[REDACTED]" : redactValue(item, fields, seen),
      enumerable: true,
      writable: true,
      configurable: true
    });
  }
  return result;
}

async function executeAsyncCommand(parsed, payload, options, eventSink = emitEvent) {
  const isArrayPayload = Array.isArray(payload);
  const { data: bodyData, directives: bodyDirectives } = isArrayPayload
    ? { data: payload, directives: {} }
    : splitDirectives(payload);

  const { directives: optionDirectives } = splitDirectives(options);
  const directives = { ...bodyDirectives, ...optionDirectives };
  if (
    directives.$once !== undefined &&
    !["post", "patch", "delete", "seed"].includes(parsed.action)
  ) {
    throw new Error('Direktif "$once" hanya mendukung perintah mutasi "post:", "patch:", "delete:", dan "seed:".');
  }
  if (
    directives.$outbox !== undefined &&
    !["post", "patch", "delete", "seed"].includes(parsed.action)
  ) {
    throw new Error('Direktif "$outbox" hanya mendukung perintah mutasi "post:", "patch:", "delete:", dan "seed:".');
  }
  if (
    directives.$expect !== undefined &&
    !["patch", "delete"].includes(parsed.action)
  ) {
    throw new Error('Direktif "$expect" hanya mendukung perintah "patch:" dan "delete:".');
  }
  if (
    directives.$maxAffected !== undefined &&
    !["patch", "delete"].includes(parsed.action)
  ) {
    throw new Error('Direktif "$maxAffected" hanya mendukung perintah "patch:" dan "delete:".');
  }
  if (
    (directives.$at !== undefined || directives.$stream !== undefined) &&
    parsed.action !== "get"
  ) {
    throw new Error('Direktif "$at" dan "$stream" hanya mendukung perintah "get:".');
  }
  if (directives.$stream !== undefined && directives.$tx !== undefined) {
    throw new Error('"$stream" tidak dapat digunakan di dalam transaksi karena hasilnya dikonsumsi setelah query selesai.');
  }
  if (directives.$retry !== undefined && parsed.action !== "batch") {
    throw new Error('Direktif "$retry" hanya mendukung perintah "batch:".');
  }
  const isTimelineInternal = Boolean(options && options[TIMELINE_INTERNAL]);
  const impactCapture = options && options[IMPACT_CAPTURE];
  if (impactCapture) directives[IMPACT_CAPTURE] = impactCapture;

  if (directives.$branch) {
    if (parsed.action === "post" || parsed.action === "patch" || parsed.action === "delete") {
      return stageBranchMutation(
        parsed,
        isArrayPayload ? payload : bodyData,
        directives
      );
    }
    if (parsed.action === "get") {
      return executeBranchPreview(
        parsed,
        { ...parsed.queryFilters, ...bodyData },
        directives,
        db,
        eventSink
      );
    }
    throw new Error(`Perintah "${parsed.action}" belum didukung dengan direktif "$branch".`);
  }

  switch (parsed.action) {
    case "connect":
      return connectDatabase(parsed.table || "default", bodyData);

    case "close":
      return closeDatabase(parsed.table);

    case "make":
      return defineTable(parsed.table, bodyData, directives);

    case "drop":
      if (parsed.pk !== null || parsed.subResource !== null) {
        throw new Error('Perintah "drop:" hanya menerima nama tabel, tanpa primary key atau sub-resource.');
      }
      return dropTable(parsed.table, directives);

    case "seed":
      return executeTrackedMutation(
        parsed,
        isArrayPayload ? payload : bodyData,
        directives,
        (activeDirectives) => executeSeed(parsed, isArrayPayload ? payload : bodyData, activeDirectives, eventSink),
        eventSink,
        !isTimelineInternal,
        impactCapture
      );

    case "get":
      if (directives.$at !== undefined) {
        if (Object.keys(bodyData).length > 0) {
          throw new Error('Filter payload tambahan belum didukung bersama "$at"; targetkan record dengan primary key.');
        }
        return executeAt(parsed, directives, eventSink);
      }
      return executeGet(parsed, { ...parsed.queryFilters, ...bodyData }, directives, eventSink);

    case "post":
      return executeTrackedMutation(
        parsed,
        isArrayPayload ? payload : bodyData,
        directives,
        (activeDirectives) => executePost(parsed, isArrayPayload ? payload : bodyData, activeDirectives, eventSink),
        eventSink,
        !isTimelineInternal,
        impactCapture
      );

    case "patch":
      return executeTrackedMutation(
        parsed,
        isArrayPayload ? payload : bodyData,
        directives,
        (activeDirectives) => executePatch(parsed, isArrayPayload ? payload : bodyData, activeDirectives, eventSink),
        eventSink,
        !isTimelineInternal,
        impactCapture
      );

    case "delete":
      return executeTrackedMutation(
        parsed,
        isArrayPayload ? payload : bodyData,
        directives,
        (activeDirectives) => executeDelete(parsed, isArrayPayload ? payload : bodyData, activeDirectives, eventSink),
        eventSink,
        !isTimelineInternal,
        impactCapture
      );

    case "batch": {
      const { conn } = resolveExecutionTarget(directives);
      await ensureTimelineStore(conn);
      if (batchHasDirective(isArrayPayload ? payload : [], "$once")) {
        await ensureOnceStore(conn);
        await ensureErasureRefStore(conn);
      }
      if (batchHasDirective(isArrayPayload ? payload : [], "$outbox")) {
        await ensureOutboxStore(conn);
        await ensureErasureRefStore(conn);
      }
      return executeCrossBatch(
        isArrayPayload ? payload : [],
        (command, targetPayload, targetOptions, targetEventSink) =>
          executePublicCommandString(command, targetPayload, targetOptions, targetEventSink),
        directives,
        eventSink
      );
    }

    case "begin":
      await ensureTimelineStore(resolveExecutionTarget(directives).conn);
      return beginTransaction(parsed.table, directives);

    case "commit":
      return commitTransaction(parsed.table);

    case "rollback":
      return rollbackTransaction(parsed.table);

    case "inspect":
      return inspectTable(parsed.table, directives);

    case "dump":
      return executeDump(parsed, directives);

    case "forget":
      return executeForget(parsed, directives);

    case "access":
      return executeAccess(parsed, directives);

    case "explain":
      return executeExplain(parsed, { ...parsed.queryFilters, ...bodyData }, directives);

    case "impact":
      return executeImpact(
        bodyData,
        directives,
        (targetParsed, targetPayload, targetOptions, targetEventSink) =>
          executeAsyncCommand(targetParsed, targetPayload, targetOptions, targetEventSink)
      );

    case "claim":
      return executeClaim(parsed, directives);

    case "renew":
      return executeRenew(parsed, directives);

    case "release":
      return executeRelease(parsed, directives);

    case "lease":
      return executeLeaseStatus(parsed, directives);

    case "outbox":
      return executeOutbox(parsed, directives);

    case "history":
      return executeHistory(parsed, directives);

    case "why":
      return executeWhy(parsed, directives);

    case "snapshot":
      return executeSnapshot(parsed, directives, eventSink);

    case "diff":
      return executeDiff(parsed, directives, eventSink);

    case "undo":
      return executeUndo(parsed, directives, db, eventSink);

    case "restore":
      return executeRestore(parsed, directives, db, eventSink);

    case "branch":
      return executeCreateBranch(parsed, directives);

    case "merge":
      return executeMergeBranch(parsed, directives, db, eventSink);

    case "discard":
      return discardBranch(parsed, directives);

    case "raw": {
      const { runner } = resolveExecutionTarget(directives);
      const params = Array.isArray(payload) ? payload : [];
      const isSelect = /^\s*(SELECT|PRAGMA|WITH|EXPLAIN)/i.test(parsed.rawSql);
      if (isSelect) {
        return runner.all(parsed.rawSql, params);
      }
      return runner.run(parsed.rawSql, params);
    }

    default:
      throw new Error(`Aksi perintah tidak dikenal: "${parsed.action}".`);
  }
}

function batchHasDirective(commands, directive) {
  return commands.some((entry) => {
    if (!Array.isArray(entry) || typeof entry[0] !== "string") return false;
    const body = Array.isArray(entry[1]) ? {} : splitDirectives(entry[1]).directives;
    const options = splitDirectives(entry[2]).directives;
    if (body[directive] !== undefined || options[directive] !== undefined) return true;
    if (entry[0].trim().toLowerCase().startsWith("batch:")) {
      return batchHasDirective(Array.isArray(entry[1]) ? entry[1] : [], directive);
    }
    return false;
  });
}

db.on = function on(eventName, listener) {
  if (!eventListeners.has(eventName)) {
    eventListeners.set(eventName, new Set());
  }
  eventListeners.get(eventName).add(listener);
  return () => db.off(eventName, listener);
};

db.off = function off(eventName, listener) {
  const set = eventListeners.get(eventName);
  if (set) set.delete(listener);
};

export default db;