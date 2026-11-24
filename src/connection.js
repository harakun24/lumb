import { DatabaseSync } from "node:sqlite";
import { createDialect, normalizeDialectName } from "./dialect.js";

const connections = new Map();
let defaultAlias = null;

function createSqliteAdapter(url = ":memory:") {
  const cleanPath = url.startsWith("sqlite://")
    ? url.slice("sqlite://".length)
    : url.startsWith("sqlite:")
      ? url.slice("sqlite:".length)
      : url;

  const nativeDb = new DatabaseSync(cleanPath || ":memory:");
  nativeDb.exec("PRAGMA journal_mode = WAL;");
  nativeDb.exec("PRAGMA foreign_keys = ON;");
  nativeDb.exec("PRAGMA busy_timeout = 5000;");

  const adapter = {
    dialect: "sqlite",
    isSingleConnection: true,
    raw: nativeDb,

    async exec(sql) {
      nativeDb.exec(sql);
    },

    async run(sql, params = []) {
      const stmt = nativeDb.prepare(sql);
      const info = stmt.run(...params);
      return {
        lastInsertId: typeof info.lastInsertRowid === "bigint"
          ? Number(info.lastInsertRowid)
          : info.lastInsertRowid,
        changes: typeof info.changes === "bigint" ? Number(info.changes) : info.changes
      };
    },

    async get(sql, params = []) {
      const stmt = nativeDb.prepare(sql);
      const row = stmt.get(...params);
      return row ? { ...row } : null;
    },

    async all(sql, params = []) {
      const stmt = nativeDb.prepare(sql);
      return stmt.all(...params).map((r) => ({ ...r }));
    },

    async acquireDedicated() {
      return adapter;
    },

    async release() { },

    async close() {
      nativeDb.close();
    }
  };

  return adapter;
}

async function createPostgresAdapter(url, config = {}) {
  let pgModule;
  try {
    pgModule = await import("pg");
  } catch {
    throw new Error('Package "pg" needed, "npm install pg" for PostgreSQL.');
  }

  const Pool = pgModule.Pool ?? pgModule.default?.Pool;
  const pool = new Pool({
    connectionString: url,
    min: config.pool?.min ?? 1,
    max: config.pool?.max ?? 10
  });

  const wrapClient = (client, releaseFn = async () => { }) => ({
    dialect: "postgres",
    isSingleConnection: false,
    async exec(sql) {
      await client.query(sql);
    },
    async run(sql, params = []) {
      const res = await client.query(sql, params);
      const firstRow = res.rows?.[0] ?? null;
      const firstKey = firstRow ? Object.keys(firstRow)[0] : null;
      return {
        lastInsertId: firstKey ? firstRow[firstKey] : null,
        changes: res.rowCount ?? 0
      };
    },
    async get(sql, params = []) {
      const res = await client.query(sql, params);
      return res.rows[0] ? { ...res.rows[0] } : null;
    },
    async all(sql, params = []) {
      const res = await client.query(sql, params);
      return res.rows.map((r) => ({ ...r }));
    },
    async release() {
      await releaseFn();
    }
  });

  const base = wrapClient(pool);
  return {
    ...base,
    raw: pool,
    async acquireDedicated() {
      const client = await pool.connect();
      return wrapClient(client, () => client.release());
    },
    async close() {
      await pool.end();
    }
  };
}

async function createMysqlAdapter(url, config = {}) {
  let mysqlModule;
  try {
    mysqlModule = await import("mysql2/promise");
  } catch {
    throw new Error('Package "mysql2" needed, "npm install mysql2" for MySQL.');
  }

  const createPool = mysqlModule.createPool ?? mysqlModule.default?.createPool;
  const pool = createPool({
    uri: url,
    connectionLimit: config.pool?.max ?? 10
  });

  const wrapClient = (client, releaseFn = async () => { }) => ({
    dialect: "mysql",
    isSingleConnection: false,
    async exec(sql) {
      await client.query(sql);
    },
    async run(sql, params = []) {
      const [header] = await client.execute(sql, params);
      return {
        lastInsertId: header.insertId ?? null,
        changes: header.affectedRows ?? 0
      };
    },
    async get(sql, params = []) {
      const [rows] = await client.execute(sql, params);
      return rows[0] ? { ...rows[0] } : null;
    },
    async all(sql, params = []) {
      const [rows] = await client.execute(sql, params);
      return rows.map((r) => ({ ...r }));
    },
    async release() {
      await releaseFn();
    }
  });

  const base = wrapClient(pool);
  return {
    ...base,
    raw: pool,
    async acquireDedicated() {
      const client = await pool.getConnection();
      return wrapClient(client, () => client.release());
    },
    async close() {
      await pool.end();
    }
  };
}

export async function connectDatabase(alias = "default", config = {}) {
  const name = alias || "default";

  if (connections.has(name)) {
    await closeDatabase(name);
  }

  let adapter;
  if (config.adapter) {
    adapter = config.adapter;
  } else {
    const url = typeof config === "string" ? config : (config.url ?? ":memory:");
    const detected = normalizeDialectName(config.dialect ?? url);

    if (detected === "postgres") {
      adapter = await createPostgresAdapter(url, config);
    } else if (detected === "mysql") {
      adapter = await createMysqlAdapter(url, config);
    } else {
      adapter = createSqliteAdapter(url);
    }
  }

  const dialect = createDialect(adapter.dialect ?? config.dialect ?? "sqlite");

  connections.set(name, {
    alias: name,
    adapter,
    dialect,
    schema: {
      tables: new Map(),
      pendingLinks: []
    },
    txLockQueue: Promise.resolve()
  });

  if (!defaultAlias || config.default === true) {
    defaultAlias = name;
  }

  return { connected: true, alias: name, dialect: dialect.name };
}

export function getConnection(alias) {
  const target = alias ?? defaultAlias;

  if (!target) {
    const adapter = createSqliteAdapter(":memory:");
    const dialect = createDialect("sqlite");
    connections.set("default", {
      alias: "default",
      adapter,
      dialect,
      schema: {
        tables: new Map(),
        pendingLinks: []
      },
      txLockQueue: Promise.resolve()
    });
    defaultAlias = "default";
    return connections.get("default");
  }

  const conn = connections.get(target);
  if (!conn) {
    throw new Error(`connection to "${target}" unregistered. call db("connect:${target}", ...) first.`);
  }
  return conn;
}

export async function closeDatabase(alias) {
  if (alias) {
    const conn = connections.get(alias);
    if (conn) {
      await conn.adapter.close();
      connections.delete(alias);
      if (defaultAlias === alias) {
        defaultAlias = connections.keys().next().value ?? null;
      }
    }
    return { closed: true, alias };
  }

  for (const [name, conn] of connections.entries()) {
    await conn.adapter.close();
    connections.delete(name);
  }
  defaultAlias = null;
  return { closed: true, all: true };
}