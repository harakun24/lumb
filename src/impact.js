import { parseCommand } from "./parser.js";
import {
  beginTransaction,
  resolveExecutionTarget,
  rollbackTransaction
} from "./transaction.js";
import { assertSchemaReady } from "./schema.js";
import {
  ContractViolationError,
  DataOperationBlockedError,
  IMPACT_CAPTURE
} from "./contracts.js";
import { ensureTimelineStore } from "./timeline.js";

const MUTATION_ACTIONS = new Set(["post", "patch", "delete", "seed"]);
const CONSTRAINT_ERROR_CODES = new Set([
  "23502",
  "23503",
  "23505",
  "23514",
  "23P01",
  "ER_BAD_NULL_ERROR",
  "ER_CHECK_CONSTRAINT_VIOLATED",
  "ER_DUP_ENTRY",
  "ER_NO_REFERENCED_ROW_2",
  "ER_ROW_IS_REFERENCED_2"
]);

export async function executeImpact(request, directives, executeCommand) {
  if (!request || typeof request !== "object" || Array.isArray(request) ||
      typeof request.command !== "string") {
    throw new Error('Perintah "impact:" wajib menerima objek dengan properti "command".');
  }
  if (request.options !== undefined &&
      (!request.options || typeof request.options !== "object" || Array.isArray(request.options))) {
    throw new Error('"options" pada perintah "impact:" wajib berupa objek.');
  }

  const parsed = parseCommand(request.command);
  if (!MUTATION_ACTIONS.has(parsed.action) || !parsed.table) {
    throw new Error('"impact:" hanya mendukung perintah mutasi post:, patch:, delete:, dan seed:.');
  }

  const activeDirectives = { ...directives, ...(request.options ?? {}) };
  if (activeDirectives.$branch) {
    throw new Error('"impact:" tidak dapat dijalankan pada draft branch.');
  }
  if (activeDirectives.$dryRun) {
    throw new Error('"impact:" sudah menjalankan simulasi; jangan gabungkan dengan "$dryRun".');
  }

  const { conn } = resolveExecutionTarget(activeDirectives);
  const tableDef = assertSchemaReady(conn, parsed.table);
  await ensureTimelineStore(conn);
  const txId = await beginTransaction(activeDirectives.$tx ?? null, activeDirectives);
  const capture = { changes: [], nestedChanges: [], result: null, complete: true, warnings: [] };
  const txDirectives = {
    ...activeDirectives,
    $tx: txId,
    [IMPACT_CAPTURE]: capture
  };

  try {
    await executeCommand(parsed, request.payload ?? {}, txDirectives, () => {});
    await rollbackTransaction(txId);
    return buildImpactResult(
      request.command,
      request.payload,
      parsed,
      tableDef,
      conn,
      activeDirectives,
      capture,
      null
    );
  } catch (error) {
    await rollbackTransaction(txId);
    if (error instanceof ContractViolationError) {
      return buildImpactResult(
        request.command,
        request.payload,
        parsed,
        tableDef,
        conn,
        activeDirectives,
        capture,
        error.violations
      );
    }
    if (error instanceof DataOperationBlockedError) {
      return buildImpactResult(
        request.command,
        request.payload,
        parsed,
        tableDef,
        conn,
        activeDirectives,
        capture,
        [],
        error.blockers
      );
    }
    if (isDatabaseConstraintError(error)) {
      return buildImpactResult(
        request.command,
        request.payload,
        parsed,
        tableDef,
        conn,
        activeDirectives,
        capture,
        [],
        [{
          type: "database-constraint",
          code: error.code,
          message: error.message
        }]
      );
    }
    throw error;
  }
}

function isDatabaseConstraintError(error) {
  const code = String(error?.code ?? "");
  const sqliteCode = Number(error?.errcode);
  return CONSTRAINT_ERROR_CODES.has(code) ||
    code.startsWith("ERR_SQLITE_CONSTRAINT") ||
    code.startsWith("SQLITE_CONSTRAINT") ||
    (Number.isInteger(sqliteCode) && (sqliteCode & 0xff) === 19);
}

function buildImpactResult(
  command,
  payload,
  parsed,
  tableDef,
  conn,
  directives,
  capture,
  contractViolations,
  relationBlockers = null
) {
  const warnings = [...capture.warnings];
  let complete = capture.complete !== false;
  const rows = Array.isArray(payload) ? payload : [payload];
  const hasNestedWrites = rows.some((row) =>
    [...tableDef.relations.entries()].some(([field, relation]) =>
      relation.kind === "one-to-many-owned" &&
      Array.isArray(row?.[field]) &&
      row[field].length > 0
    )
  );
  if (parsed.action === "post" && Array.isArray(payload) && hasNestedWrites) {
    complete = false;
    warnings.push("Batch post dengan baris anak nested belum didukung oleh planner.");
  }
  const primaryKeyColumn = tableDef.columns.get(tableDef.primaryKey);
  const mayGenerateIds = ["post", "seed"].includes(parsed.action) &&
    primaryKeyColumn?.isAutoIncrement &&
    rows.some((row) => row && row[tableDef.primaryKey] == null);
  if (mayGenerateIds && ["mysql", "postgres"].includes(conn.dialect.name)) {
    warnings.push("Database dapat memajukan counter ID meskipun preview di-rollback.");
  }
  if (hasPotentialHooks(conn, tableDef, parsed.action, directives, payload)) {
    warnings.push("Hook JavaScript dijalankan saat preview; pastikan hook bebas efek samping eksternal.");
  }
  const contractBlockers = (contractViolations ?? []).map((violation) => ({
    type: "contract",
    ...violation
  }));
  const blockedBy = relationBlockers ?? contractBlockers;

  return {
    command,
    action: parsed.action,
    table: parsed.table,
    affectedCount: capture.changes.length,
    changes: capture.changes,
    contractViolations: contractViolations ?? [],
    blockedBy,
    canApply: blockedBy.length === 0,
    complete,
    warnings
  };
}

function hasPotentialHooks(conn, tableDef, action, directives, payload, visited = new Set()) {
  if (visited.has(tableDef.name)) return false;
  visited.add(tableDef.name);
  if ((action === "delete" && tableDef.removeFn && !directives.$hard) ||
      [...tableDef.columns.values()].some((column) => column.dynamicFn)) {
    return true;
  }

  const rows = Array.isArray(payload) ? payload : [payload];
  for (const [field, relation] of tableDef.relations) {
    const childTable = conn.schema.tables.get(relation.targetTable);
    if (!childTable) continue;

    if (["post", "seed"].includes(action) &&
        relation.kind === "one-to-many-owned" &&
        rows.some((row) => Array.isArray(row?.[field])) &&
        hasPotentialHooks(
          conn,
          childTable,
          "post",
          {},
          rows.flatMap((row) => row?.[field] ?? []),
          visited
        )) {
      return true;
    }
    if (action === "delete" &&
        (relation.kind === "one-to-many-owned" ||
         (directives.$cascade && relation.kind === "one-to-many-reverse")) &&
        hasPotentialHooks(
          conn,
          childTable,
          "delete",
          { $hard: relation.kind === "one-to-many-owned" || directives.$hard },
          [],
          visited
        )) {
      return true;
    }
  }
  return false;
}
