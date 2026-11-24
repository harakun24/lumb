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

async function executeAsyncCommand(parsed, payload, options) {
  const isArrayPayload = Array.isArray(payload);
  const { data: bodyData, directives: bodyDirectives } = isArrayPayload
    ? { data: payload, directives: {} }
    : splitDirectives(payload);

  const { directives: optionDirectives } = splitDirectives(options);
  const directives = { ...bodyDirectives, ...optionDirectives };

  switch (parsed.action) {
    case "connect":
      return connectDatabase(parsed.table || "default", bodyData);

    case "close":
      return closeDatabase(parsed.table);

    case "make":
      return defineTable(parsed.table, bodyData, directives);

    case "seed":
      return executeSeed(parsed, isArrayPayload ? payload : bodyData, directives, emitEvent);

    case "get":
      return executeGet(parsed, { ...parsed.queryFilters, ...bodyData }, directives, emitEvent);

    case "post":
      return executePost(parsed, isArrayPayload ? payload : bodyData, directives, emitEvent);

    case "patch":
      return executePatch(parsed, isArrayPayload ? payload : bodyData, directives, emitEvent);

    case "delete":
      return executeDelete(parsed, isArrayPayload ? payload : bodyData, directives, emitEvent);

    case "batch":
      return executeCrossBatch(isArrayPayload ? payload : [], db, directives);

    case "begin":
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