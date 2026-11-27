export const IMPACT_CAPTURE = Symbol("lumb.impact.capture");

export class ContractViolationError extends Error {
  constructor(violations) {
    const details = violations
      .map((item) => `${item.table}/${item.pk} [${item.contract}]: ${item.message}`)
      .join("; ");
    super(`Data contract violation: ${details}`);
    this.name = "ContractViolationError";
    this.violations = violations;
  }
}

export class DataOperationBlockedError extends Error {
  constructor(blockers) {
    super(blockers.map((item) => item.message).join("; "));
    this.name = "DataOperationBlockedError";
    this.blockers = blockers;
  }
}

export function normalizeContracts(tableName, contracts) {
  if (!Array.isArray(contracts)) {
    throw new Error(`"$contracts" pada tabel "${tableName}" wajib berupa Array.`);
  }

  const ids = new Set();
  return contracts.map((contract, index) => {
    if (!contract || typeof contract !== "object" ||
        typeof contract.id !== "string" || contract.id.length === 0 ||
        typeof contract.check !== "function" ||
        typeof contract.message !== "string" || contract.message.length === 0) {
      throw new Error(
        `Contract ke-${index + 1} pada tabel "${tableName}" wajib memiliki "id", fungsi "check", dan "message".`
      );
    }
    if (ids.has(contract.id)) {
      throw new Error(`ID contract "${contract.id}" duplikat pada tabel "${tableName}".`);
    }
    ids.add(contract.id);
    return {
      id: contract.id,
      check: contract.check,
      message: contract.message
    };
  });
}

export async function validateContractChanges(conn, changes, action = null) {
  const violations = [];
  for (const change of changes) {
    const tableDef = conn.schema.tables.get(change.table);
    if (!tableDef?.contracts?.length) continue;

    for (const contract of tableDef.contracts) {
      const valid = await contract.check({
        table: change.table,
        pk: change.pk,
        operation: change.operation,
        action: action ?? change.operation,
        before: cloneContractValue(change.before),
        after: cloneContractValue(change.after)
      });
      if (typeof valid !== "boolean") {
        throw new Error(
          `Contract "${contract.id}" pada tabel "${change.table}" harus mengembalikan boolean.`
        );
      }
      if (!valid) {
        violations.push({
          table: change.table,
          pk: change.pk,
          contract: contract.id,
          message: contract.message
        });
      }
    }
  }

  if (violations.length > 0) throw new ContractViolationError(violations);
}

function cloneContractValue(value) {
  if (value instanceof Date) return new Date(value.getTime());
  if (Array.isArray(value)) return value.map(cloneContractValue);
  if (value && typeof value === "object") {
    return Object.fromEntries(
      Object.entries(value).map(([key, item]) => [key, cloneContractValue(item)])
    );
  }
  return value;
}
