import { getConnection } from "./connection.js";
import { generateTypeDefinitions } from "./typegen.js";
import { normalizeContracts } from "./contracts.js";

/**
 * Menerjemahkan objek Blueprint-by-Example menjadi struktur tabel & relasi database
 */
export async function defineTable(tableName, blueprint, directives = {}) {
  if (!tableName) {
    throw new Error("Nama tabel wajib disebutkan pada perintah make:<nama_tabel>.");
  }

  for (const key of Object.keys(blueprint)) {
    if (key.startsWith("$") && key !== "$contracts") {
      throw new Error(`Nama kolom tidak boleh diawali karakter "$": "${key}".`);
    }
  }

  const conn = getConnection(directives.$connection);
  const { dialect, schema } = conn;
  const {
    $rename = {},
    $drop = [],
    $index = [],
    $dryRun = false,
    $typegen = false,
    $fresh = false,
    $ignore = false
  } = directives;

  if ($fresh && $ignore) {
    throw new Error('Direktif "$fresh" dan "$ignore" tidak dapat digunakan bersamaan pada make:.');
  }

  const columns = new Map();
  const relations = new Map();
  const getters = new Map();
  const childBlueprints = [];
  const manyToManyDefs = [];
  let primaryKey = null;
  let softDeletePatch = null;

  // 1. Deteksi perilaku soft-delete dari metode remove()
  if (typeof blueprint.remove === "function") {
    const trackedChanges = {};
    const proxy = new Proxy(
      {},
      {
        set(_, prop, value) {
          trackedChanges[prop] = value;
          return true;
        }
      }
    );
    blueprint.remove.call(proxy);
    if (Object.keys(trackedChanges).length > 0) {
      softDeletePatch = trackedChanges;
    }
  }

  // 2. Urai setiap properti pada objek contoh JS
  for (const key of Object.keys(blueprint)) {
    if (key === "$contracts") continue;
    const desc = Object.getOwnPropertyDescriptor(blueprint, key);

    if (key === "remove" && typeof desc.value === "function") {
      continue;
    }

    if (typeof desc.get === "function") {
      getters.set(key, desc.get);
      continue;
    }

    const rawDef = desc.value;

    // Relasi 1 ke Banyak milik induk: items: [ { ... } ]
    if (
      Array.isArray(rawDef) &&
      rawDef.length === 1 &&
      typeof rawDef[0] === "object" &&
      rawDef[0] !== null &&
      !(rawDef[0] instanceof Date) &&
      !rawDef[0].type
    ) {
      const childTableName = `${tableName}_${key}`;
      childBlueprints.push({ fieldName: key, childTableName, childBlueprint: rawDef[0] });
      continue;
    }

    // Relasi Banyak ke Banyak: kategori: ["@kategori"]
    if (
      Array.isArray(rawDef) &&
      rawDef.length === 1 &&
      typeof rawDef[0] === "string" &&
      rawDef[0].startsWith("@")
    ) {
      const targetTable = rawDef[0].slice(1);
      manyToManyDefs.push({ fieldName: key, targetTable });
      continue;
    }

    const colSpec = parseColumnSpec(key, rawDef);
    if (colSpec.isPrimaryKey) {
      if (primaryKey) {
        throw new Error(`Tabel "${tableName}" memiliki lebih dari satu primary key ("${primaryKey}" dan "${key}").`);
      }
      primaryKey = key;
    }

    if (colSpec.refTable) {
      relations.set(key, {
        kind: colSpec.unique ? "one-to-one" : "many-to-one",
        localKey: key,
        targetTable: colSpec.refTable,
        targetKey: null
      });
    }

    columns.set(key, colSpec);
  }

  // 3. Jika tidak ada key: true, buat kolom 'id' otomatis sebagai primary key
  if (!primaryKey) {
    primaryKey = "id";
    const autoIdCol = {
      name: "id",
      jsType: "Number",
      isInteger: true,
      isPrimaryKey: true,
      isAutoIncrement: true,
      nullable: false,
      unique: true,
      hasDefault: false
    };
    const reordered = new Map([["id", autoIdCol], ...columns]);
    columns.clear();
    for (const [k, v] of reordered) columns.set(k, v);
  }

  const tableDef = {
    name: tableName,
    primaryKey,
    columns,
    relations,
    getters,
    contracts: directives.$contracts !== undefined || blueprint.$contracts !== undefined
      ? normalizeContracts(tableName, directives.$contracts ?? blueprint.$contracts)
      : schema.tables.get(tableName)?.contracts ?? [],
    softDeletePatch,
    removeFn: typeof blueprint.remove === "function" ? blueprint.remove : null,
    indexes: $index,
    isJunction: false
  };

  // 4. Tautkan relasi ke tabel tujuan (atau antrekan jika tabel tujuan belum dibuat)
  const deferredFkConstraints = [];

  for (const [colName, rel] of relations.entries()) {
    const targetDef = rel.targetTable === tableName ? tableDef : schema.tables.get(rel.targetTable);
    if (targetDef) {
      applyForeignKeyMetadata(tableDef, colName, rel, targetDef, true);
    } else if (!$dryRun) {
      schema.pendingLinks.push({
        sourceTable: tableName,
        colName,
        rel
      });
    }
  }

  // Selesaikan antrean relasi tertunda dari tabel sebelumnya yang menunggu tabel ini
  const remainingPending = [];
  for (const pending of schema.pendingLinks) {
    if (pending.rel.targetTable === tableName) {
      const srcDef = schema.tables.get(pending.sourceTable);
      if (srcDef) {
        if (!$dryRun) {
          applyForeignKeyMetadata(srcDef, pending.colName, pending.rel, tableDef, true);
          registerReverseRelation(tableDef, srcDef, pending.colName);
        }
        if (!dialect.supportsDeferredFkInCreate) {
          const fkName = `fk_${srcDef.name}_${pending.colName}`;
          const fkCheck = dialect.hasForeignKeyQuery(srcDef.name, fkName);
          const fkExists = fkCheck
            ? await conn.adapter.get(fkCheck.sql, fkCheck.params)
            : false;

          if (!fkExists) {
            deferredFkConstraints.push(
              `ALTER TABLE ${dialect.quoteId(srcDef.name)} ADD CONSTRAINT ${dialect.quoteId(fkName)} FOREIGN KEY (${dialect.quoteId(pending.colName)}) REFERENCES ${dialect.quoteId(tableName)}(${dialect.quoteId(tableDef.primaryKey)}) ON DELETE RESTRICT;`
            );
          }
        }
      }
    } else {
      remainingPending.push(pending);
    }
  }
  if (!$dryRun) {
    schema.pendingLinks = remainingPending;
  }

  for (const [colName, rel] of relations.entries()) {
    const targetDef = schema.tables.get(rel.targetTable);
    if (!$dryRun && targetDef && targetDef.name !== tableDef.name) {
      registerReverseRelation(targetDef, tableDef, colName);
    }
  }

  // 5. Bangun perintah SQL DDL (DROP jika $fresh, abaikan jika $ignore, atau CREATE/ALTER)
  const plannedSql = await buildOrMigrateTableSql(conn, tableDef, {
    $rename,
    $drop,
    $index,
    $fresh,
    $ignore,
    childTables: childBlueprints.map((c) => c.childTableName),
    junctionTables: manyToManyDefs.map((m) => `_rel_${tableName}_${m.fieldName}`)
  });

  if (!$ignore) {
    plannedSql.push(...deferredFkConstraints);
  }

  // Eksekusi DDL tabel induk lebih dulu agar tabel referensi sudah tersedia di DB
  if (!$dryRun) {
    for (const sql of plannedSql) {
      await conn.adapter.exec(sql);
    }
  }

  // 6. Bangun sub-tabel 1:N milik induk (sekarang tabel induk sudah eksis di DB)
  for (const child of childBlueprints) {
    const fkColName = `${tableName}_id`;
    const parentPkCol = columns.get(primaryKey);
    const childSchemaObj = {
      [fkColName]: {
        type: parentPkCol.jsType === "String" ? String : Number,
        length: parentPkCol.length ?? undefined,
        max: parentPkCol.max ?? undefined,
        _ownedParent: { table: tableName, key: primaryKey }
      }
    };

    // Salin seluruh properti & getter dari childBlueprint secara utuh menggunakan descriptor
    for (const childKey of Object.keys(child.childBlueprint)) {
      const childDesc = Object.getOwnPropertyDescriptor(child.childBlueprint, childKey);
      Object.defineProperty(childSchemaObj, childKey, childDesc);
    }

    const childRes = await defineTable(child.childTableName, childSchemaObj, {
      $connection: directives.$connection,
      $dryRun,
      $fresh,
      $ignore
    });
    if ($dryRun && Array.isArray(childRes.sql)) {
      plannedSql.push(...childRes.sql);
    }

    relations.set(child.fieldName, {
      kind: "one-to-many-owned",
      localKey: primaryKey,
      targetTable: child.childTableName,
      foreignKey: fkColName
    });
  }

  // 7. Bangun tabel perantara (pivot) untuk relasi N:M
  const junctionSqlList = [];
  for (const m2m of manyToManyDefs) {
    const junctionName = `_rel_${tableName}_${m2m.fieldName}`;
    const localFk = `${tableName}_id`;
    const targetFk = `${m2m.targetTable}_id`;

    const targetDef = schema.tables.get(m2m.targetTable);
    const localSqlType = dialect.mapColumnType({ ...columns.get(primaryKey), isPrimaryKey: false, isAutoIncrement: false });
    const targetSqlType = targetDef
      ? dialect.mapColumnType({ ...targetDef.columns.get(targetDef.primaryKey), isPrimaryKey: false, isAutoIncrement: false })
      : dialect.mapColumnType({ jsType: "Number", isInteger: true });

    const q = dialect.quoteId;
    const inspectJunction = dialect.inspectColumnsQuery(junctionName);
    const junctionCols = await conn.adapter.all(inspectJunction.sql, inspectJunction.params);
    const junctionExists = junctionCols.length > 0;

    if (!($ignore && junctionExists)) {
      if ($fresh) {
        const dropJunctionSql = buildDropTableSql(dialect, junctionName);
        plannedSql.push(dropJunctionSql);
        junctionSqlList.push(dropJunctionSql);
      }

      const createJunctionSql = `CREATE TABLE IF NOT EXISTS ${q(junctionName)} (\n  ${q(localFk)} ${localSqlType} NOT NULL,\n  ${q(targetFk)} ${targetSqlType} NOT NULL,\n  PRIMARY KEY (${q(localFk)}, ${q(targetFk)})\n);`;
      plannedSql.push(createJunctionSql);
      junctionSqlList.push(createJunctionSql);
    }

    if (!$dryRun) {
      schema.tables.set(junctionName, {
        name: junctionName,
        primaryKey: localFk,
        columns: new Map(),
        relations: new Map(),
        getters: new Map(),
        softDeletePatch: null,
        isJunction: true
      });
    }

    relations.set(m2m.fieldName, {
      kind: "many-to-many",
      localKey: primaryKey,
      targetTable: m2m.targetTable,
      targetKey: targetDef?.primaryKey ?? "id",
      junctionTable: junctionName,
      junctionLocalKey: localFk,
      junctionTargetKey: targetFk
    });
  }

  if (!$dryRun) {
    schema.tables.set(tableName, tableDef);
  }

  if ($dryRun) {
    return { dryRun: true, table: tableName, dialect: dialect.name, sql: plannedSql };
  }

  for (const sql of junctionSqlList) {
    await conn.adapter.exec(sql);
  }

  if ($typegen) {
    generateTypeDefinitions(schema);
  }

  return { defined: true, table: tableName, primaryKey };
}

export async function dropTable(tableName, directives = {}) {
  if (!tableName) {
    throw new Error('Nama tabel wajib disebutkan pada perintah "drop:<nama_tabel>".');
  }
  if (directives.$tx !== undefined) {
    throw new Error('"drop:" tidak dapat dijalankan di dalam transaksi karena dukungan DDL berbeda antar-DBMS.');
  }
  if (
    directives.$cascade !== undefined &&
    typeof directives.$cascade !== "boolean"
  ) {
    throw new Error('Direktif "$cascade" pada "drop:" wajib berupa boolean.');
  }

  const conn = getConnection(directives.$connection);
  const { schema } = conn;
  if (!schema.tables.has(tableName)) {
    throw new Error(`Tabel "${tableName}" belum didefinisikan pada koneksi ini.`);
  }

  const cascade = directives.$cascade === true;
  const tables = collectDropTables(schema, tableName);
  if (!cascade && tables.size > 1) {
    const dependents = [...tables].filter((name) => name !== tableName);
    throw new Error(
      `Tabel "${tableName}" memiliki dependensi: ${dependents.join(", ")}. ` +
      'Gunakan "$cascade: true" untuk menghapus seluruh tabel yang bergantung.'
    );
  }

  const dropOrder = buildDropOrder(schema, tables);
  const sql = dropOrder.map((name) => buildExplicitDropTableSql(conn.dialect, name));
  if (directives.$dryRun) {
    return {
      dryRun: true,
      table: tableName,
      dialect: conn.dialect.name,
      tables: dropOrder,
      sql
    };
  }

  const droppedTables = [];
  for (const name of dropOrder) {
    try {
      await conn.adapter.exec(buildExplicitDropTableSql(conn.dialect, name));
    } catch (error) {
      if (droppedTables.length === 0) throw error;
      throw new Error(
        `DROP TABLE berhenti setelah menghapus [${droppedTables.join(", ")}]; ` +
        `tabel "${name}" gagal dihapus: ${error.message}`,
        { cause: error }
      );
    }
    removeDroppedTableFromSchema(schema, name);
    droppedTables.push(name);
  }
  return {
    dropped: true,
    table: tableName,
    tables: droppedTables
  };
}

function collectDropTables(schema, rootTable) {
  const tables = new Set();
  const pending = [rootTable];
  while (pending.length > 0) {
    const current = pending.pop();
    if (tables.has(current)) continue;
    tables.add(current);
    for (const dependent of getDropDependents(schema, current)) {
      if (!schema.tables.has(dependent)) {
        throw new Error(
          `Dependensi "${dependent}" dari tabel "${current}" tidak tersedia pada schema Lumb.`
        );
      }
      pending.push(dependent);
    }
  }
  return tables;
}

function getDropDependents(schema, tableName) {
  const dependents = new Set();
  const tableDef = schema.tables.get(tableName);
  for (const relation of tableDef?.relations.values() ?? []) {
    if (
      relation.kind === "one-to-many-owned" ||
      relation.kind === "one-to-many-reverse"
    ) {
      dependents.add(relation.targetTable);
    } else if (relation.kind === "many-to-many") {
      dependents.add(relation.junctionTable);
    }
  }

  for (const candidate of schema.tables.values()) {
    if (candidate.name === tableName) continue;
    for (const relation of candidate.relations.values()) {
      if (
        (relation.kind === "many-to-one" || relation.kind === "one-to-one") &&
        relation.targetTable === tableName
      ) {
        dependents.add(candidate.name);
      } else if (
        relation.kind === "many-to-many" &&
        relation.targetTable === tableName
      ) {
        dependents.add(relation.junctionTable);
      }
    }
  }

  for (const pending of schema.pendingLinks) {
    if (pending.rel.targetTable === tableName) dependents.add(pending.sourceTable);
  }
  dependents.delete(tableName);
  return dependents;
}

function buildDropOrder(schema, tables) {
  const order = [];
  const visiting = new Set();
  const visited = new Set();
  const visit = (tableName) => {
    if (visited.has(tableName)) return;
    if (visiting.has(tableName)) {
      throw new Error(`Dependensi tabel melingkar ditemukan saat drop di "${tableName}".`);
    }
    visiting.add(tableName);
    for (const dependent of getDropDependents(schema, tableName)) {
      if (tables.has(dependent)) visit(dependent);
    }
    visiting.delete(tableName);
    visited.add(tableName);
    order.push(tableName);
  };

  for (const tableName of tables) visit(tableName);
  return order;
}

function removeDroppedTableFromSchema(schema, tableName) {
  schema.tables.delete(tableName);
  schema.pendingLinks = schema.pendingLinks.filter(
    (pending) =>
      pending.sourceTable !== tableName &&
      pending.rel.targetTable !== tableName
  );
  for (const tableDef of schema.tables.values()) {
    for (const [name, relation] of tableDef.relations) {
      if (relation.targetTable === tableName || relation.junctionTable === tableName) {
        tableDef.relations.delete(name);
      }
    }
  }
}

function buildExplicitDropTableSql(dialect, tableName) {
  return `DROP TABLE IF EXISTS ${dialect.quoteId(tableName)};`;
}

function buildDropTableSql(dialect, targetTable) {
  const q = dialect.quoteId;
  if (dialect.name === "postgres") {
    return `DROP TABLE IF EXISTS ${q(targetTable)} CASCADE;`;
  }
  if (dialect.name === "mysql") {
    return `SET FOREIGN_KEY_CHECKS = 0; DROP TABLE IF EXISTS ${q(targetTable)}; SET FOREIGN_KEY_CHECKS = 1;`;
  }
  return `DROP TABLE IF EXISTS ${q(targetTable)};`;
}

function parseColumnSpec(name, rawDef) {
  let nullable = false;
  let unique = false;
  let inner = rawDef;

  if (inner instanceof Set) {
    unique = true;
    inner = [...inner][0];
  }

  if (Array.isArray(inner) && inner.includes(null)) {
    nullable = true;
    inner = inner.find((item) => item !== null);
  }

  // Referensi tabel lain: "@kategori"
  if (typeof inner === "string" && inner.startsWith("@")) {
    return {
      name,
      jsType: "Number",
      isInteger: true,
      refTable: inner.slice(1),
      nullable,
      unique,
      isPrimaryKey: false,
      hasDefault: nullable,
      defaultValue: nullable ? null : undefined
    };
  }

  // Konfigurasi objek: { type, length, min, max, key, default }
  if (inner && typeof inner === "object" && !(inner instanceof Date) && inner.type) {
    const jsType = mapConstructorName(inner.type);
    const hasDefault = "default" in inner || nullable;
    const isInteger = jsType === "Number" && (inner.default === undefined || Number.isInteger(inner.default));
    return {
      name,
      jsType,
      isInteger,
      isPrimaryKey: Boolean(inner.key),
      unique: Boolean(inner.unique || inner.key || unique),
      nullable: inner.key ? false : nullable,
      length: inner.length ?? null,
      min: inner.min ?? null,
      max: inner.max ?? null,
      hasDefault,
      defaultValue: "default" in inner ? inner.default : nullable ? null : undefined,
      ownedParent: inner._ownedParent ?? null
    };
  }

  // Konstruktor tipe murni: String, Number, Boolean, Date
  if (inner === String || inner === Number || inner === Boolean || inner === Date) {
    const jsType = mapConstructorName(inner);
    return {
      name,
      jsType,
      isInteger: false,
      isPrimaryKey: false,
      unique,
      nullable,
      hasDefault: nullable,
      defaultValue: nullable ? null : undefined
    };
  }

  // Fungsi dinamis: () => ... atau (item) => ...
  if (typeof inner === "function") {
    const sample = inner.length === 0 ? inner() : new Date();
    const jsType = sample instanceof Date
      ? "Date"
      : typeof sample === "number"
        ? "Number"
        : typeof sample === "boolean"
          ? "Boolean"
          : "String";

    return {
      name,
      jsType,
      isInteger: jsType === "Number" && Number.isInteger(sample),
      isPrimaryKey: false,
      unique,
      nullable,
      hasDefault: true,
      dynamicFn: inner,
      isUpdateHook: inner.length >= 1
    };
  }

  // Nilai primitif langsung: "", 0, false, new Date()
  const jsType = inner instanceof Date
    ? "Date"
    : typeof inner === "number"
      ? "Number"
      : typeof inner === "boolean"
        ? "Boolean"
        : "String";

  return {
    name,
    jsType,
    isInteger: jsType === "Number" && Number.isInteger(inner),
    isPrimaryKey: false,
    unique,
    nullable,
    hasDefault: true,
    defaultValue: inner
  };
}

function mapConstructorName(ctor) {
  if (ctor === String) return "String";
  if (ctor === Number) return "Number";
  if (ctor === Boolean) return "Boolean";
  if (ctor === Date) return "Date";
  return "String";
}

function applyForeignKeyMetadata(sourceTableDef, colName, rel, targetTableDef, targetExists) {
  const targetPkCol = targetTableDef.columns.get(targetTableDef.primaryKey);
  const col = sourceTableDef.columns.get(colName);
  rel.targetKey = targetTableDef.primaryKey;
  if (col && targetPkCol) {
    col.jsType = targetPkCol.jsType;
    col.isInteger = targetPkCol.isInteger;
    col.length = targetPkCol.length ?? null;
    col.max = targetPkCol.max ?? null;
    if (targetExists) {
      col.foreignKey = {
        table: targetTableDef.name,
        column: targetTableDef.primaryKey,
        onDelete: "RESTRICT"
      };
    }
  }
}

function registerReverseRelation(targetTableDef, childTableDef, fkColName) {
  if (!targetTableDef.relations.has(childTableDef.name)) {
    targetTableDef.relations.set(childTableDef.name, {
      kind: "one-to-many-reverse",
      localKey: targetTableDef.primaryKey,
      targetTable: childTableDef.name,
      foreignKey: fkColName
    });
  }
}

async function buildOrMigrateTableSql(
  conn,
  tableDef,
  { $rename, $drop, $index, $fresh, $ignore, childTables = [], junctionTables = [] }
) {
  const { dialect } = conn;
  const q = dialect.quoteId;
  const sqlList = [];

  const inspectQuery = dialect.inspectColumnsQuery(tableDef.name);
  const existingCols = await conn.adapter.all(inspectQuery.sql, inspectQuery.params);
  const tableExists = existingCols.length > 0;

  // Mode $ignore: jika tabel sudah tersedia di DB, lewati CREATE / ALTER / INDEX
  if ($ignore && tableExists) {
    return sqlList;
  }

  // Mode $fresh: hapus tabel lama (beserta tabel anak & pivot) sebelum membuat ulang
  if ($fresh) {
    for (const childTable of childTables) {
      sqlList.push(buildDropTableSql(dialect, childTable));
    }
    for (const junctionTable of junctionTables) {
      sqlList.push(buildDropTableSql(dialect, junctionTable));
    }
    sqlList.push(buildDropTableSql(dialect, tableDef.name));
  }

  if (!tableExists || $fresh) {
    const colDefs = [];
    for (const col of tableDef.columns.values()) {
      colDefs.push(compileColumnSql(col, dialect, false));
    }
    sqlList.push(`CREATE TABLE ${q(tableDef.name)} (\n  ${colDefs.join(",\n  ")}\n);`);
  } else {
    const existingNames = new Set(existingCols.map((c) => c.name));

    for (const [oldName, newName] of Object.entries($rename)) {
      if (existingNames.has(oldName) && !existingNames.has(newName)) {
        sqlList.push(`ALTER TABLE ${q(tableDef.name)} RENAME COLUMN ${q(oldName)} TO ${q(newName)};`);
        existingNames.delete(oldName);
        existingNames.add(newName);
      }
    }

    for (const col of tableDef.columns.values()) {
      if (!existingNames.has(col.name)) {
        const alterCol = compileColumnSql(col, dialect, true);
        sqlList.push(`ALTER TABLE ${q(tableDef.name)} ADD COLUMN ${alterCol};`);
      }
    }

    for (const dropCol of $drop) {
      if (existingNames.has(dropCol)) {
        sqlList.push(`ALTER TABLE ${q(tableDef.name)} DROP COLUMN ${q(dropCol)};`);
      }
    }
  }

  for (const idxCols of $index) {
    const cols = Array.isArray(idxCols) ? idxCols : [idxCols];
    const idxName = `idx_${tableDef.name}_${cols.join("_")}`;
    const colsQuoted = cols.map(q).join(", ");

    if (dialect.name === "mysql") {
      if ($fresh) {
        sqlList.push(dialect.createIndexSql(q(tableDef.name), q(idxName), colsQuoted));
      } else {
        const checkQuery = dialect.hasIndexQuery(tableDef.name, idxName);
        const exists = await conn.adapter.get(checkQuery.sql, checkQuery.params);
        if (!exists) {
          sqlList.push(dialect.createIndexSql(q(tableDef.name), q(idxName), colsQuoted));
        }
      }
    } else {
      sqlList.push(dialect.createIndexSql(q(tableDef.name), q(idxName), colsQuoted));
    }
  }

  return sqlList;
}

function compileColumnSql(col, dialect, isAlter = false) {
  const q = dialect.quoteId;
  const sqlType = dialect.mapColumnType(col);
  const parts = [q(col.name), sqlType];

  if (col.isPrimaryKey && !col.isAutoIncrement && !isAlter) {
    parts.push("PRIMARY KEY");
  }

  if (!col.nullable && !col.isPrimaryKey) {
    parts.push("NOT NULL");
  }

  if (col.unique && !col.isPrimaryKey && !isAlter) {
    parts.push("UNIQUE");
  }

  if (col.hasDefault && col.defaultValue !== undefined && !col.dynamicFn) {
    parts.push(`DEFAULT ${dialect.escapeLiteral(col.defaultValue)}`);
  }

  const checks = [];
  if (col.length !== null && col.length !== undefined) {
    checks.push(`${dialect.lengthFn(q(col.name))} = ${Number(col.length)}`);
  }
  if (col.min !== null && col.min !== undefined) {
    checks.push(
      col.jsType === "String"
        ? `${dialect.lengthFn(q(col.name))} >= ${Number(col.min)}`
        : `${q(col.name)} >= ${Number(col.min)}`
    );
  }
  if (col.max !== null && col.max !== undefined) {
    checks.push(
      col.jsType === "String"
        ? `${dialect.lengthFn(q(col.name))} <= ${Number(col.max)}`
        : `${q(col.name)} <= ${Number(col.max)}`
    );
  }
  if (checks.length > 0) {
    parts.push(`CHECK (${checks.join(" AND ")})`);
  }

  if (col.foreignKey) {
    parts.push(
      `REFERENCES ${q(col.foreignKey.table)}(${q(col.foreignKey.column)}) ON DELETE ${col.foreignKey.onDelete}`
    );
  }

  if (col.ownedParent) {
    parts.push(
      `REFERENCES ${q(col.ownedParent.table)}(${q(col.ownedParent.key)}) ON DELETE CASCADE`
    );
  }

  return parts.join(" ");
}

/**
 * Validasi tipe, panjang, dan rentang nilai pada sisi JS sebelum mengirim ke DB
 */
export function validateAndPrepareRow(tableDef, schema, inputRow, isPatch = false) {
  const prepared = {};

  for (const [colName, col] of tableDef.columns.entries()) {
    if (col.isAutoIncrement && !(colName in inputRow)) continue;

    let val = inputRow[colName];

    // Ekstrak kunci utama otomatis jika pengguna menyisipkan objek relasi utuh
    if (val && typeof val === "object" && !(val instanceof Date) && col.refTable) {
      const targetDef = schema.tables.get(col.refTable);
      if (targetDef && targetDef.primaryKey in val) {
        val = val[targetDef.primaryKey];
      }
    }

    if (val === undefined) {
      if (!isPatch) {
        if (col.dynamicFn) {
          val = col.dynamicFn(inputRow);
        } else if (col.hasDefault) {
          val = col.defaultValue;
        } else if (!col.nullable) {
          throw new Error(`Kolom "${colName}" pada tabel "${tableDef.name}" wajib diisi (NOT NULL).`);
        } else {
          val = null;
        }
      } else if (col.dynamicFn && col.isUpdateHook) {
        val = col.dynamicFn(inputRow);
      } else {
        continue;
      }
    }

    if (val === null) {
      if (!col.nullable) {
        throw new Error(`Kolom "${colName}" pada tabel "${tableDef.name}" tidak boleh null.`);
      }
      prepared[colName] = null;
      continue;
    }

    if (isPatch && typeof val === "object" && !(val instanceof Date) && ("increment" in val || "decrement" in val)) {
      prepared[colName] = val;
      continue;
    }

    if (col.jsType === "String") {
      if (typeof val !== "string") {
        throw new Error(`Kolom "${colName}" wajib bertipe String.`);
      }
      if (col.length !== null && col.length !== undefined && val.length !== col.length) {
        throw new Error(`Kolom "${colName}" wajib memiliki panjang tepat ${col.length} karakter.`);
      }
      if (col.min !== null && col.min !== undefined && val.length < col.min) {
        throw new Error(`Kolom "${colName}" minimal ${col.min} karakter.`);
      }
      if (col.max !== null && col.max !== undefined && val.length > col.max) {
        throw new Error(`Kolom "${colName}" maksimal ${col.max} karakter.`);
      }
    } else if (col.jsType === "Number") {
      if (typeof val !== "number" || !Number.isFinite(val)) {
        throw new Error(`Kolom "${colName}" wajib bertipe Number valid.`);
      }
      if (col.min !== null && col.min !== undefined && val < col.min) {
        throw new Error(`Kolom "${colName}" minimal bernilai ${col.min}.`);
      }
      if (col.max !== null && col.max !== undefined && val > col.max) {
        throw new Error(`Kolom "${colName}" maksimal bernilai ${col.max}.`);
      }
    } else if (col.jsType === "Boolean") {
      if (typeof val !== "boolean") {
        throw new Error(`Kolom "${colName}" wajib bertipe Boolean.`);
      }
    } else if (col.jsType === "Date") {
      if (!(val instanceof Date) && typeof val !== "string") {
        throw new Error(`Kolom "${colName}" wajib bertipe Date.`);
      }
      if (typeof val === "string") val = new Date(val);
    }

    prepared[colName] = val;
  }

  return prepared;
}

/**
 * Mengembalikan tipe data JS asli (Boolean, Date) dan memasang getter pada baris hasil kueri
 */
export function hydrateRow(tableDef, rawRow) {
  if (!rawRow || !tableDef) return rawRow;
  const obj = {};

  for (const [key, value] of Object.entries(rawRow)) {
    const col = tableDef.columns.get(key);
    if (!col || value === null || value === undefined) {
      obj[key] = value;
      continue;
    }

    if (col.jsType === "Boolean") {
      obj[key] = Boolean(value);
    } else if (col.jsType === "Date") {
      obj[key] = value instanceof Date ? value : new Date(value);
    } else {
      obj[key] = value;
    }
  }

  for (const [getterName, getterFn] of tableDef.getters.entries()) {
    Object.defineProperty(obj, getterName, {
      get: getterFn,
      enumerable: true,
      configurable: true
    });
  }

  return obj;
}

export function assertSchemaReady(conn, tableName) {
  const { schema } = conn;
  const tableDef = schema.tables.get(tableName);
  if (!tableDef) {
    throw new Error(`Tabel "${tableName}" belum didefinisikan. Jalankan db("make:${tableName}", ...) terlebih dahulu.`);
  }

  const unresolvedForThisTable = schema.pendingLinks.filter(
    (p) => p.sourceTable === tableName
  );
  if (unresolvedForThisTable.length > 0) {
    const missing = [...new Set(unresolvedForThisTable.map((p) => p.rel.targetTable))];
    throw new Error(`Tabel rujukan belum didefinisikan dengan make: -> ${missing.join(", ")}`);
  }

  return tableDef;
}

export async function inspectTable(tableName, directives = {}) {
  const conn = getConnection(directives.$connection);
  const tableDef = assertSchemaReady(conn, tableName);
  const inspectQuery = conn.dialect.inspectColumnsQuery(tableName);
  const columns = await conn.adapter.all(inspectQuery.sql, inspectQuery.params);

  return {
    table: tableName,
    dialect: conn.dialect.name,
    primaryKey: tableDef.primaryKey,
    softDelete: Boolean(tableDef.softDeletePatch),
    columns
  };
}