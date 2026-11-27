import { DataOperationBlockedError } from "./contracts.js";

export const MAX_AFFECTED_STATE = Symbol("lumb.maxAffected.state");

export function assertExpectedState(tableDef, rows, expected) {
  if (!expected || typeof expected !== "object" || Array.isArray(expected) ||
      Object.keys(expected).length === 0) {
    throw new Error('Direktif "$expect" wajib berupa objek berisi nilai kolom yang diharapkan.');
  }

  for (const [field, value] of Object.entries(expected)) {
    if (!tableDef.columns.has(field)) {
      throw new Error(`Kolom "${field}" tidak ditemukan pada "$expect" tabel "${tableDef.name}".`);
    }
    if (
      value !== null &&
      !["string", "number", "boolean", "bigint"].includes(typeof value) &&
      !(value instanceof Date)
    ) {
      throw new Error(`Nilai "$expect.${field}" harus berupa nilai skalar atau Date.`);
    }
    if (typeof value === "number" && !Number.isFinite(value)) {
      throw new Error(`Nilai "$expect.${field}" harus berupa angka finite.`);
    }
    if (value instanceof Date && Number.isNaN(value.getTime())) {
      throw new Error(`Nilai "$expect.${field}" harus berupa Date yang valid.`);
    }
  }

  const mismatches = [];
  for (const row of rows) {
    const differences = {};
    for (const [field, expectedValue] of Object.entries(expected)) {
      if (!sameValue(row[field], expectedValue)) {
        differences[field] = { expected: expectedValue, actual: row[field] };
      }
    }
    if (Object.keys(differences).length > 0) {
      mismatches.push({
        table: tableDef.name,
        pk: row[tableDef.primaryKey],
        differences
      });
    }
  }

  if (rows.length === 0 || mismatches.length > 0) {
    const details = rows.length === 0
      ? [{ table: tableDef.name, pk: null, differences: expected }]
      : mismatches;
    throw new DataOperationBlockedError(details.map((item) => ({
      type: "expectation",
      ...item,
      message: item.pk === null
        ? `Tidak ada record "${tableDef.name}" yang cocok dengan target dan "$expect".`
        : `Record "${tableDef.name}/${item.pk}" tidak lagi memenuhi "$expect".`
    })));
  }
}

export function createAffectedState(directives, action, count, tableDef, rows) {
  if (directives.$maxAffected === undefined) return null;
  if (action !== "patch" && action !== "delete") {
    throw new Error('Direktif "$maxAffected" saat ini hanya mendukung perintah "patch:" dan "delete:".');
  }

  const max = directives.$maxAffected;
  if (typeof max !== "number" || !Number.isSafeInteger(max) || max < 0) {
    throw new Error('"$maxAffected" wajib berupa integer non-negatif.');
  }
  const state = { max, count: 0 };
  addAffectedRows(state, count, tableDef, rows);
  return state;
}

export function addAffectedRows(state, count, tableDef, rows) {
  if (!state) return;
  state.count += count;
  if (state.count <= state.max) return;

  const blockers = [{
    type: "max-affected",
    table: tableDef.name,
    count: state.count,
    max: state.max,
    message: `Mutasi akan memengaruhi ${state.count} record; batas "$maxAffected" adalah ${state.max}.`
  }];
  throw new DataOperationBlockedError(blockers);
}

function sameValue(actual, expected) {
  if (actual instanceof Date && expected instanceof Date) {
    return actual.getTime() === expected.getTime();
  }
  return Object.is(actual, expected);
}
