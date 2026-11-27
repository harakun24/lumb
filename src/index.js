import { parseCommand, splitDirectives } from "./parser.js";
import { closeDatabase, connectDatabase } from "./connection.js";
import { defineTable, inspectTable } from "./schema.js";
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
import {
  executeDiff,
  executeCreateBranch,
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
  const parsed = parseCommand(command);

  if (parsed.action === "scope") {
    return createScopedDb(parsed.table, payload, db);
  }

  return executeAsyncCommand(parsed, payload, options);
}

async function executeAsyncCommand(parsed, payload, options, eventSink = emitEvent) {
  const isArrayPayload = Array.isArray(payload);
  const { data: bodyData, directives: bodyDirectives } = isArrayPayload
    ? { data: payload, directives: {} }
    : splitDirectives(payload);

  const { directives: optionDirectives } = splitDirectives(options);
  const directives = { ...bodyDirectives, ...optionDirectives };
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

    case "batch":
      await ensureTimelineStore(resolveExecutionTarget(directives).conn);
      return executeCrossBatch(isArrayPayload ? payload : [], db, directives);

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