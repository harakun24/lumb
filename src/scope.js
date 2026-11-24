import { parseCommand } from "./parser.js";

export function createScopedDb(targetTable, scopeContext, dbExecutor) {
  if (!targetTable) {
    throw new Error('"scope:<name_table>" required name.');
  }

  return function scopedDb(command, payload = {}, options = {}) {
    const parsed = parseCommand(command);

    if (parsed.action === "batch" && Array.isArray(payload)) {
      const scopedCommands = payload.map(([cmd, subPayload = {}, subOpts = {}]) => {
        const subParsed = parseCommand(cmd);
        if (subParsed.table !== targetTable) return [cmd, subPayload, subOpts];
        if (subParsed.action === "post" || subParsed.action === "seed") {
          return [
            cmd,
            Array.isArray(subPayload)
              ? subPayload.map((item) => ({ ...scopeContext, ...item }))
              : { ...scopeContext, ...subPayload },
            subOpts
          ];
        }
        if (subParsed.action === "patch" || subParsed.action === "delete") {
          return [
            cmd,
            { ...subPayload, $where: { ...scopeContext, ...(subPayload.$where ?? {}) } },
            subOpts
          ];
        }
        return [cmd, { ...scopeContext, ...subPayload }, subOpts];
      });
      return dbExecutor(command, scopedCommands, options);
    }

    if (parsed.table !== targetTable) {
      return dbExecutor(command, payload, options);
    }

    if (parsed.action === "get" || parsed.action === "explain") {
      return dbExecutor(command, { ...scopeContext, ...payload }, options);
    }

    if (parsed.action === "post" || parsed.action === "seed") {
      if (Array.isArray(payload)) {
        const scopedArray = payload.map((item) => ({ ...scopeContext, ...item }));
        return dbExecutor(command, scopedArray, options);
      }
      return dbExecutor(command, { ...scopeContext, ...payload }, options);
    }

    if (parsed.action === "patch" || parsed.action === "delete") {
      if (Array.isArray(payload)) {
        return dbExecutor(command, payload, options);
      }
      const existingWhere = payload.$where ?? {};
      return dbExecutor(
        command,
        {
          ...payload,
          $where: { ...scopeContext, ...existingWhere }
        },
        options
      );
    }

    return dbExecutor(command, payload, options);
  };
}