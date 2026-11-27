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
              ? subPayload.map((item) => ({ ...item, ...scopeContext }))
              : { ...subPayload, ...scopeContext },
            subOpts
          ];
        }
        if (subParsed.action === "patch" || subParsed.action === "delete") {
          const scopedWhere = {
            ...(subPayload?.$where ?? {}),
            ...(subOpts.$where ?? {}),
            ...scopeContext
          };
          const subOptions = {
            ...subOpts,
            $where: scopedWhere
          };
          const scopedPayload = subParsed.action === "patch" && Array.isArray(subPayload)
            ? subPayload.map((item) => ({
              ...item,
              $where: { ...(item.$where ?? {}), ...scopedWhere }
            }))
            : subPayload;
          return [
            cmd,
            scopedPayload,
            subOptions
          ];
        }
        if (["claim", "lease", "release", "renew"].includes(subParsed.action)) {
          return [
            cmd,
            subPayload,
            {
              ...subOpts,
              $where: { ...(subOpts.$where ?? {}), ...scopeContext }
            }
          ];
        }
        return [cmd, { ...subPayload, ...scopeContext }, subOpts];
      });
      return dbExecutor(command, scopedCommands, options);
    }

    if (parsed.table !== targetTable) {
      return dbExecutor(command, payload, options);
    }

    if (["claim", "lease", "release", "renew"].includes(parsed.action)) {
      if (parsed.pk === null) {
        return dbExecutor(command, payload, options);
      }
      return (async () => {
        const scopedOptions = {
          ...options,
          $where: { ...(options.$where ?? {}), ...scopeContext }
        };
        const { primaryKey } = await dbExecutor(
          `inspect:${targetTable}`,
          {},
          scopedOptions
        );
        const scopedRows = await dbExecutor(
          `get:${targetTable}`,
          { ...scopeContext, [primaryKey]: parsed.pk },
          scopedOptions
        );
        if (scopedRows.length === 0) {
          throw new Error(`Record "${targetTable}/${parsed.pk}" tidak ditemukan dalam scope ini.`);
        }
        return dbExecutor(command, payload, scopedOptions);
      })();
    }

    if (parsed.action === "get" || parsed.action === "explain") {
      return dbExecutor(command, { ...payload, ...scopeContext }, options);
    }

    if (parsed.action === "post" || parsed.action === "seed") {
      if (Array.isArray(payload)) {
        const scopedArray = payload.map((item) => ({ ...item, ...scopeContext }));
        return dbExecutor(command, scopedArray, options);
      }
      return dbExecutor(command, { ...payload, ...scopeContext }, options);
    }

    if (parsed.action === "patch" || parsed.action === "delete") {
      if (Array.isArray(payload)) {
        const scopedWhere = { ...(options.$where ?? {}), ...scopeContext };
        const scopedPayload = parsed.action === "patch"
          ? payload.map((item) => ({
            ...item,
            $where: { ...(item.$where ?? {}), ...scopedWhere }
          }))
          : payload;
        return dbExecutor(command, scopedPayload, {
          ...options,
          $where: scopedWhere
        });
      }
      const existingWhere = payload.$where ?? {};
      const scopedWhere = {
        ...existingWhere,
        ...(options.$where ?? {}),
        ...scopeContext
      };
      return dbExecutor(
        command,
        {
          ...payload,
          $where: scopedWhere
        },
        { ...options, $where: scopedWhere }
      );
    }

    return dbExecutor(command, payload, options);
  };
}