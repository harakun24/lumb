# lumb

URL-driven database library for JavaScript that pairs plain-object simplicity with full control over schemas, relations, and transactions.

## Why the Name `lumb`

The name `lumb` was chosen after checking package name availability and confirming that the name was not yet taken on npm. This makes the package short, memorable, and clean for both installation (`npm install lumb`) and imports (`import db from "lumb"`).

Beyond availability, `lumb` fits a focused data layer that keeps daily application code concise while exposing explicit database primitives such as schema definitions, relational modeling, batch operations, transactions, and dialect-aware SQL compilation.

---

## Core Design Principles

- **Single Entry Point (`db`)**: Every operation runs through one function signature: `db(command, payload, options)`.
- **URL-Prefixed Command Paradigm**: Actions and targets are written in a compact `action:target` string format (for example, `make:produk`, `get:produk/KP0001`, `patch:produk/MT0001`, `begin:`, `commit:<id>`, `rollback:<id>`).
- **Plain JavaScript Schemas (Blueprint-by-Example)**: Table structures, constraints, defaults, dynamic hooks, computed getters, soft-delete rules, and relationships are defined directly from plain JavaScript objects without model classes or decorators.
- **Explicit Control Over Database Primitives**: Supports explicit transaction IDs (`begin:`, `commit:`, `rollback:`), savepoints, row locks (`$lock`), composite indexes (`$index`), relation integrity (`RESTRICT` vs `$cascade`), SQL execution plans (`explain:`), and raw parameterized SQL (`raw:`).
- **Multi-Dialect Support with Built-in SQLite**: Uses Node.js built-in `node:sqlite` out of the box with zero required external dependencies, while supporting PostgreSQL (`pg`) and MySQL/MariaDB (`mysql2/promise`) through dynamic imports and dialect compilation in `src/dialect.js`.

---

## Installation and Testing

### Installation

```bash
npm install lumb
```

Optional drivers for external databases:

```bash
# For PostgreSQL
npm install pg

# For MySQL / MariaDB
npm install mysql2
```

---

## Complete Feature Reference

### 1. Connections, Default Selection, and Lifecycle (`connect:`, `close:`)

Register connections by alias name. The first registered connection becomes the default unless another connection passes `default: true`.

```javascript
import db from "lumb";

// SQLite in-memory or file (zero external dependencies)
await db("connect:arsip", { url: ":memory:" });

// Set explicit default connection
await db("connect:utama", {
  url: "sqlite:./kasir.db",
  default: true,
});

// Additional connection alias
await db("connect:cabang_b", {
  url: "sqlite:./cabang_b.db",
});

// PostgreSQL & MySQL connection URLs (when pg / mysql2 are installed)
await db("connect:pg", {
  url: "postgres://user:pass@localhost:5432/app",
  pool: { min: 2, max: 10 },
});
await db("connect:mysql", {
  url: "mysql://user:pass@localhost:3306/app",
  pool: { max: 10 },
});

// Target a non-default connection per call
const rows = await db("get:produk", { $connection: "cabang_b" });

// Close a single alias or all connections
await db("close:arsip");
await db("close:");
```

---

### 2. Schema Definition, Constraints, Hooks, Soft-Delete, and Relations (`make:`)

Define tables with plain JavaScript example objects. If no property sets `key: true`, an auto-incrementing integer primary key named `id` is created automatically.

#### Column Types, Modifiers, and Validation

- **Primitive literals**: `"text"`, `0`, `false`, `new Date()` infer type and act as `NOT NULL DEFAULT <value>` (`0` infers integer; non-integer numbers infer floating-point).
- **Type constructors**: `String`, `Number`, `Boolean`, `Date` define `NOT NULL` columns without a static default.
- **Nullable columns**: `[Type, null]` allows `NULL` values.
- **Unique non-primary columns**: `new Set([Type])` or `new Set(["@otherTable"])` marks the column as `UNIQUE`.
- **Primary key**: `{ type: String, length: 6, key: true }` sets the primary key column used by `/<pk>` URL paths.
- **Length and range constraints**: `length` (exact string length), `min` (minimum string length or minimum number), `max` (maximum string length or maximum number).
- **Dynamic generator & update hooks**:
  - `() => ...` (0 arguments): evaluated when inserting a row (for example, timestamps or generated IDs).
  - `(row) => ...` (1 or more arguments): evaluated on insert and update hooks.
- **Computed getters**: `get subtotal() { return this.qty * this.hargaSatuan; }` attaches non-persisted getters to hydrated result objects.
- **Soft-delete via `remove()`**: Defining a `remove() { this.deletedAt = new Date(); }` method enables soft-delete so `delete:` updates the target column instead of deleting the row physically.
- **Composite and single indexes**: `$index: [["colA", "colB"], "colC"]`.
- **Dry run & type generation**: `$dryRun: true` returns generated SQL statements without executing them; `$typegen: true` writes `.lumb.d.ts` in non-production environments.

#### Relations and Deferred Linking

- **Many-to-One / One-to-One reference**: `kategori: "@kategori"` links to the target table's primary key (`ON DELETE RESTRICT`). If the target table has not been defined yet, the link is queued as a deferred link and resolved once `make:<targetTable>` runs.
- **Owned One-to-Many child table**: `items: [{ produk: "@produk", qty: 1, hargaSatuan: 0 }]` creates a child table (`<parent>_items`) with a foreign key referencing the parent (`ON DELETE CASCADE` when parent rows are deleted).
- **Many-to-Many relation**: `tags: ["@tag"]` creates a junction table (`_rel_<table>_<field>`) with a composite primary key.

#### Safe Schema Evolution

Running `make:<table>` on an existing table compares columns and applies safe changes:

- Adds new columns (`ALTER TABLE ... ADD COLUMN ...`).
- Renames columns without losing data using `$rename: { oldCol: "newCol" }`.
- Drops columns only when explicitly listed in `$drop: ["obsoleteCol"]`.
- Adds $fresh: true for always make a fresh table
- Adds $ignore: true for ignoring the table definition when the table already exist

Drop a table with `drop:<table>`. Lumb refuses when it knows the table has dependents; pass `$cascade: true` to drop the dependent tables too, including owned child tables and many-to-many junctions. `$dryRun: true` returns the ordered SQL plan without changing the database or schema registry. DDL cannot run inside `$tx`.

```javascript
// Deferred link: produk references kategori before kategori is created
await db("make:produk", {
  kode: { type: String, length: 6, key: true },
  nama: { type: String, min: 3, max: 100 },
  harga: { type: Number, default: 0, min: 0 },
  diskon: 0,
  stok: 0,
  cabang: "CBG-01",
  kategori: "@kategori",
  tags: ["@tag"],
  dibuatPada: () => new Date(),
  diubahPada: (row) => new Date(),
  dihapusPada: [Date, null],
  remove() {
    this.dihapusPada = new Date();
  },
  $index: [["cabang", "kategori"], "stok"],
});

await db("make:kategori", {
  nama: new Set([String]),
});

await db("make:tag", {
  nama: new Set([String]),
});

await db("make:transaksi", {
  kasir: String,
  total: 0,
  items: [
    {
      produk: "@produk",
      qty: 1,
      hargaSatuan: 0,
      get subtotal() {
        return this.qty * this.hargaSatuan;
      },
    },
  ],
});

// always fresh
await db(
  "make:produk",
  {
    kode: { type: String, length: 6, key: true },
    nama: String,
    stok: 0,
  },
  { $fresh: true },
);

// not making table that already exist
await db(
  "make:produk",
  {
    kode: { type: String, length: 6, key: true },
    nama: String,
    stok: 0,
  },
  { $ignore: true },
);

// Inspect dependent tables before dropping them
const dropPlan = await db("drop:produk", {}, {
  $cascade: true,
  $dryRun: true,
});

// Execute the planned drop
await db("drop:produk", {}, { $cascade: true });
```

`$cascade` includes tables known to the current Lumb schema; database-level dependencies not registered in that schema are left to the DBMS to reject. Since DDL transactionality differs between DBMSs, a failed multi-table cascade may have already dropped some tables; the error reports which ones completed.

Dropping a user table does not purge its retained Timeline or internal audit/idempotency data. Use `forget:` before dropping where applicable, and account for retained non-null fields and existing historical entries.

---

### 3. Seeding (`seed:`)

`seed:<table>` inserts initial records idempotently by checking the primary key (or unique column when the primary key is auto-incrementing).

```javascript
// Skip rows that already exist
await db("seed:kategori", [{ nama: "Minuman" }, { nama: "Makanan" }]);

// Update existing rows when ?sync=true is passed
await db("seed:produk?sync=true", [
  { kode: "KP0001", nama: "Kopi Susu", harga: 18000, stok: 20, kategori: 1 },
]);
```

---

### 4. Create and Batch Insert (`post:`)

Insert a single record (including nested `1:N` child items and `N:M` target IDs) or pass an array for batch insertion.

```javascript
// Single insert with nested 1:N items and N:M tags
const order = await db("post:transaksi", {
  kasir: "Dina",
  total: 36000,
  items: [{ produk: "KP0001", qty: 2, hargaSatuan: 18000 }],
});

// Batch insert with chunking and conflict handling ("ignore" or "update")
await db(
  "post:produk",
  [
    { kode: "PR0001", nama: "Roti Bakar", harga: 15000, stok: 10, kategori: 2 },
    {
      kode: "PR0002",
      nama: "Pisang Goreng",
      harga: 12000,
      stok: 25,
      kategori: 2,
    },
    {
      kode: "KP0001",
      nama: "Kopi Susu Aren",
      harga: 19000,
      stok: 20,
      kategori: 1,
    },
  ],
  {
    $chunk: 500,
    $onConflict: "update",
  },
);
```

---

### 5. Querying, Relations, Pagination, Aggregation, and Inspection (`get:`, `explain:`, `dump:`, `inspect:`)

#### URL Paths and Query Parameters

- `get:<table>` returns all matching rows as an array.
- `get:<table>/<pk>` returns a single hydrated row or `null`.
- `get:<table>/<pk>/<relation>` navigates directly to a sub-resource or relation collection.
- Query string parameters such as `get:produk?stok=4&aktif=true` are coerced (`true`, `false`, `null`, numbers, strings) and merged with filter objects.

#### Filter Operators and Logical Directives

- Direct equality: `{ kategori: 1 }`
- `IN` list via array: `{ kategori: [1, 2, 3] }` or `{ kode: { in: ["KP0001", "TH0001"] } }`
- Null check: `{ dihapusPada: null }`
- Comparison operators: `eq`, `ne`, `gt`, `gte`, `lt`, `lte`
- String operators: `includes`, `startsWith`, `endsWith`
- Relation existence: `{ items: { $exists: true } }` or `{ items: null }`
- Logical combinators: `$or: [{ stok: { lte: 5 } }, { harga: { gte: 50000 } }]` and `$not: { kode: "KP0001" }`

#### Relations (`$with`), Joins, Sorting, Field Selection, Pagination, and Aggregation

- **Nested relation loading**: `$with: { items: { produk: { tags: true } } }`
- **Explicit SQL joins**: `$join: "inner" | "left" | "right" | "full" | "cross"`, `$on: ["localCol", "targetCol"]`, and `$flat: true`
- **Sorting & projection**: `$sort: "harga:desc,nama:asc"` and `$fields: ["kode", "nama", "harga"]`
- **Page pagination**: `$page: 1, $limit: 20` returns `{ data, meta: { page, limit, total, totalPages } }`
- **Cursor pagination**: `$cursor: 100, $limit: 20` fetches records with primary key greater than the cursor
- **Streaming**: `$stream: true` or a batch size returns an async iterable paged by primary key; `$limit` caps the total rows
- **Point-in-time reads**: `get:<table>/<pk>` with `$at: Date | timestamp string` reconstructs a single record from tracked Timeline changes
- **Aggregations**: `$group`, `$count`, `$sum`, `$avg`, `$min`, `$max`
- **Soft-delete visibility**: default excludes soft-deleted rows; `$trash: "include"` includes them; `$trash: "only"` returns only soft-deleted rows
- **Diagnostics and export**: `explain:<table>` returns the compiled SQL, parameters, and query plan; `inspect:<table>` returns column metadata; `dump:<table>` returns all hydrated rows

```javascript
const filtered = await db("get:produk", {
  $or: [{ stok: { lte: 5 } }, { harga: { gte: 20000 } }],
  $not: { kode: "PR0002" },
  $sort: "harga:desc",
  $fields: ["kode", "nama", "harga", "stok"],
});

const paged = await db("get:produk", { $page: 1, $limit: 10 });
const cursorRows = await db("get:transaksi", { $cursor: 0, $limit: 20 });

const stream = await db("get:transaksi", {}, { $stream: 200 });
for await (const transaction of stream) {
  await processTransaction(transaction);
}

const summary = await db("get:produk", {
  $group: "kategori",
  $count: "kode",
  $sum: "stok",
  $avg: "harga",
});

const plan = await db("explain:produk", { stok: { lte: 5 } });
const info = await db("inspect:produk");
const backup = await db("dump:kategori");
```

#### Purpose-Aware Access and Data Forgetting

Attach `$purpose` and `$actor.id` to a `get:` or `dump:` read to save an access receipt. Receipts contain the actor, purpose, table, requested fields/relations, filter field names, timestamp, and returned row count—not filter values or row data. Read them with `access:history`, optionally filtering by table, `$actor`, `$purpose`, `$since`, and `$limit`.

```javascript
const customer = await db("get:customers/42", {}, {
  $purpose: "support ticket #184",
  $actor: { id: "agent-7" },
  $fields: ["name", "email"],
});

const recentReads = await db("access:history", {}, {
  $actor: { id: "agent-7" },
  $since: new Date(Date.now() - 24 * 60 * 60 * 1000),
  $limit: 100,
});
```

`$purpose` requires `$actor.id`; it does not support `$tx`, `$stream`, aggregation, or sub-resource reads, so each receipt describes a complete table read and can be committed independently. `access:history` exposes audit metadata; applications should apply their normal authorization checks before returning receipts.

Use `forget:<table>/<pk>` with `$fields` to null selected nullable, non-primary-key columns. Lumb also scrubs matching tracked Timeline changes/snapshots, invalidates matching branches, replaces linked `$once` results with a tombstone, and marks linked undelivered outbox events as forgotten.

```javascript
await db("forget:customers/42", {}, {
  $fields: ["email", "phone"],
});
```

This keeps the record and its primary key. Outbox events already claimed by a worker or delivered to external systems cannot be reliably recalled. Historical `$once`/outbox entries created before forget-reference tracking, changes made outside tracked Lumb mutations, and copies in external systems cannot be discovered or erased automatically.

---

### 6. Update and Batch Update (`patch:`)

Update by URL primary key (`patch:<table>/<pk>`), by `$where` filter object (`patch:<table>` with `{ $where: ... }`), or pass an array of objects containing the primary key for per-row batch updates. Supports atomic `increment` and `decrement` objects.

```javascript
// Single update with atomic decrement and N:M tag replacement
await db("patch:produk/KP0001", {
  stok: { decrement: 2 },
  tags: [1],
});

// Uniform bulk update via $where
await db("patch:produk", {
  $where: { kategori: 2 },
  diskon: 1000,
});

// Per-row batch update
await db("patch:produk", [
  { kode: "PR0001", stok: { increment: 5 } },
  { kode: "PR0002", harga: 13000 },
]);
```

---

### 7. Delete, Soft-Delete, and Relation Integrity (`delete:`)

`delete:` supports single primary key (`delete:<table>/<pk>`), array of primary keys (`delete:<table>`, `["id1", "id2"]`), filter payload or `$where`, soft-delete (when `remove()` is defined), hard delete (`$hard: true`), and cascade deletion (`$cascade: true`).

```javascript
// Soft-delete (if remove() is defined on the table)
await db("delete:produk/PR0002");

// Hard-delete (even if soft-delete is defined)
await db("delete:produk/PR0002", { $hard: true });

// Batch delete by array of keys
await db("delete:produk", ["PR0001"]);

// Cascade delete referencing rows when allowed
await db("delete:kategori/1", { $cascade: true });
```

#### Operator-Safe Micro-Directives

These directives add application-level safeguards around mutations and data delivery:

- **`$expect`**: compare exact scalar column values inside the mutation transaction before `patch:` or `delete:`. A stale or missing row blocks the whole operation with `DataOperationBlockedError`.
- **`$maxAffected`**: abort `patch:` or `delete:` if the selected rows exceed the limit. Deletes also count cascaded child records; many-to-many junction rows are not counted.
- **`$once`**: attach a persistent idempotency key to `post:`, `patch:`, `delete:`, or `seed:`. Identical requests replay the original result (including `Date` values); reusing a key for a different request is blocked. Receipts remain in the database and do not expire automatically.
- **`$outbox`**: append one or more `{ type, payload }` events in the same transaction as a mutation. Workers claim with `outbox:claim`, then acknowledge or reschedule with their claim token. Delivery is at-least-once; publish externally before acknowledging, and make consumers idempotent.
- **`$redact`**: replace matching field names with `"[REDACTED]"` in returned values and emitted events. This is presentation filtering only; it does not erase stored rows or Timeline history.
- **`$stream`**: return an async iterable without holding a transaction while the consumer processes it. Streams use ascending primary-key pagination; `true` uses batches of 100, or pass an integer from 1 to 1000. Offset, custom sorting, aggregation, explicit joins, and primary-key filters are not supported. A `$fields` projection must include the primary key.
- **`$at`**: reconstruct one record at a `Date` or timestamp string using Timeline entries. It requires `get:<table>/<pk>` and does not support filters or `$with`; writes made outside lumb or through `raw:` are not represented.
- **`$retry`**: retry a `batch:` on recognized transient database errors. A boolean `true` retries twice; an integer from 0 to 5 specifies additional retries. Events are emitted only after a successful commit. Retry is refused if the schema contains custom contract, getter, or soft-delete callbacks, which could have external side effects.

```javascript
await db(`patch:orders/${order.id}`, { status: "approved" }, {
  $expect: { status: "pending" },
  $maxAffected: 1,
  $outbox: {
    type: "order.approved",
    payload: { orderId: order.id },
  },
});

const events = await db("outbox:claim", {}, {
  $worker: "mail-worker",
  $limit: 20,
  $ttl: 30000,
});
for (const event of events) {
  await publish(event);
  await db(`outbox:ack/${event.id}`, {}, { $claim: event.claimToken });
}

const safeOrder = await db(`get:orders/${order.id}`, {}, {
  $at: new Date("2025-01-01T00:00:00Z"),
  $redact: ["internalNote"],
});
```

---

### 8. Explicit Transactions, Savepoints, Cross-Table Batch, Scope, and Raw SQL

#### Explicit Transaction IDs (`begin:`, `commit:`, `rollback:`) and Savepoints (`begin:<txId>`)

Pass `$tx: txId` to commands executed inside the transaction. Calling `rollback:<txId>` is idempotent and safe to repeat.

```javascript
const tx = await db("begin:", { $timeout: 5000 });

try {
  const item = await db("get:produk/KP0001", { $lock: true, $tx: tx });
  if (!item || item.stok < 2) {
    await db(`rollback:${tx}`);
  } else {
    await db("patch:produk/KP0001", { stok: { decrement: 2 }, $tx: tx });

    // Nested savepoint
    const sp = await db(`begin:${tx}`);
    try {
      await db("post:transaksi", { kasir: "Dina", total: 36000, $tx: sp });
      await db(`commit:${sp}`);
    } catch {
      await db(`rollback:${sp}`);
    }

    await db(`commit:${tx}`);
  }
} catch (err) {
  await db(`rollback:${tx}`);
  throw err;
}
```

#### Atomic Cross-Table Batch (`batch:`)

Executes a list of `[command, payload, options]` tuples inside a single transaction that commits on success or rolls back on failure.
Pass `$retry: true` or an integer from 0 to 5 to retry the entire batch after recognized transient database errors.

```javascript
await db("batch:", [
  ["patch:produk/KP0001", { stok: { decrement: 1 } }],
  ["post:transaksi", { kasir: "Dina", total: 18000 }],
]);
```

#### Scoped Context (`scope:`) and Raw SQL (`raw:`)

```javascript
// Scope operations on a table to a fixed context
const dbCabang1 = db("scope:produk", { cabang: "CBG-01" });
const cabang1Products = await dbCabang1("get:produk");

// Raw parameterized SQL
const rawResult = await db(
  "raw:SELECT COUNT(*) AS total FROM produk WHERE stok > ?",
  [0],
);
```

---

### 9. Event Listeners (`db.on` and `db.off`)

Subscribe to mutation and query events across the library:

```javascript
const unsubscribe = db.on("patch:produk", (updatedRow) => {
  if (updatedRow && updatedRow.stok <= 5) {
    console.log(`Low stock alert: ${updatedRow.nama} (${updatedRow.stok})`);
  }
});

db.on("query", ({ action, durationMs }) => {
  if (durationMs > 200) {
    console.warn(`Slow query on ${action}: ${durationMs.toFixed(2)}ms`);
  }
});

// Unsubscribe when no longer needed
unsubscribe();
// Or: db.off("patch:produk", handler);
```

---

### 10. Data Timeline (`history:`, `why:`, `undo:`, `snapshot:`, `diff:`, `restore:`, `branch:`, `merge:`, `discard:`)

Mutations performed through `post:`, `patch:`, `delete:`, and `seed:` are recorded automatically in the internal `_lumb_timeline` table. Each change is stored with before/after row values and optional audit metadata:

```javascript
await db("patch:produk/KP0001", { stok: 8 }, {
  $actor: { id: "worker-1" },
  $reason: "stock correction",
  $source: "inventory-import",
});

const changes = await db("history:produk/KP0001");
const stockOrigins = await db("why:produk/KP0001/stok");
await db(`undo:${changes[0].id}`);
```

`history:<table>/<pk>` lists changes newest first. `why:<table>/<pk>/<field>` lists the recorded origins of a field. `undo:<changeId>` applies the inverse as a new tracked change; if the current row or tracked relation state no longer matches the recorded state, it fails with a conflict instead of overwriting newer data. Use `$dryRun: true` with `undo:` to inspect conflicts without applying the inverse.

Snapshots save the selected rows and can be compared or restored later:

```javascript
const snapshot = await db("snapshot:produk", {
  $where: { cabang: "CBG-01" },
  $with: { tags: true },
  $label: "before-import",
});

const changes = await db("diff:produk", { $from: snapshot.id });
const restorePlan = await db("restore:produk", {
  $from: snapshot.id,
  $dryRun: true,
});
await db("restore:produk", { $from: snapshot.id });
```

Restore defaults to merging snapshot contents into current rows and leaves rows absent from the snapshot untouched. Pass `$mode: "replace"` to also delete rows that were not present in the snapshot. Conflicts are detected atomically before changes are committed. Many-to-many links included through `$with` are restored; snapshots do not currently restore owned child-table graphs. Likewise, undo does not reconstruct child-table rows removed by a cascading delete.

Branches stage draft operations without changing live rows:

```javascript
const branch = await db("branch:produk/KP0001", { $label: "revised-stock" });
await db(`patch:produk/KP0001`, { stok: 12 }, { $branch: branch.id });
const draft = await db(`get:produk/KP0001`, {}, { $branch: branch.id });

await db(`merge:${branch.id}`, { $dryRun: true });
await db(`merge:${branch.id}`);
// Or abandon an open branch:
await db(`discard:${branch.id}`);
```

Branch writes are limited to the branch's table; `patch:` and `delete:` may target only its base record. Merge replays the staged operations in one transaction and rejects a stale base record. Timeline tracking covers mutations made through lumb; SQL issued through `raw:` and writes made outside lumb are not journaled.

---

### 11. Data Contracts and Impact Preview (`$contracts`, `impact:`)

Attach business rules to a table with `$contracts`. Each `check` receives `{ table, pk, operation, action, before, after }` and must return a boolean (or a promise resolving to one). A failed rule rejects the mutation and rolls back that operation, including inside an explicit transaction or batch.

```javascript
await db(
  "make:inventory",
  { name: String, stock: 0 },
  {
    $contracts: [{
      id: "stock-not-negative",
      check: ({ after }) => after === null || after.stock >= 0,
      message: "Stock cannot be negative.",
    }],
  },
);
```

Contracts also work on owned child-table blueprints by putting `$contracts` inside the child object. Contract functions should be deterministic and free of external side effects. They currently validate changed rows; cross-row or aggregate contracts are not supported. They run for direct mutations, nested child writes/deletes, and timeline undo/restore operations performed through lumb.

Use `impact:` to simulate a supported mutation in a rollback transaction and inspect its row changes and blockers:

```javascript
const plan = await db("impact:", {
  command: "delete:orders/42",
  payload: {},
  options: { $cascade: true },
});

if (plan.canApply && plan.complete) {
  console.log(plan.changes);
}
```

Impact preview supports `post:`, `patch:`, `delete:`, and `seed:`. It reports Data Contract, database-constraint, and relation-integrity blockers; it does not persist rows or emit normal lumb mutation events, and it returns warnings for incomplete plans or hooks with possible external side effects. Batch `post:` with nested child rows is not yet fully planned. PostgreSQL and MySQL may advance auto-ID counters even though the preview transaction is rolled back. As with timeline tracking, writes through `raw:` or outside lumb do not receive contract enforcement.

---

### 12. Data Leases (`claim:`, `lease:`, `renew:`, `release:`)

Data Leases let an operator hold a record while reviewing it, without keeping a database transaction open. Claims require `$actor.id`; `$ttl` is in milliseconds, defaults to 10 minutes, and must be between 1 second and 24 hours.

```javascript
const actor = { id: "operator-17", name: "Rina" };

const claim = await db("claim:orders/42", {}, {
  $actor: actor,
  $ttl: 10 * 60 * 1000,
});

if (!claim.claimed) {
  console.log("Sedang ditinjau oleh:", claim.lease.owner);
} else {
  const order = await db("get:orders/42");

  await db("patch:orders/42", { reviewStatus: "approved" }, {
    $actor: actor,
    $lease: claim.token,
    $reason: "Review selesai",
  });

  await db("release:orders/42", {}, {
    $actor: actor,
    $lease: claim.token,
  });
}
```

`lease:<table>/<pk>` inspects the active holder without returning its token. `renew:<table>/<pk>` extends an active lease and requires the same `$actor.id` and `$lease` token. `release:<table>/<pk>` also requires both; a stale release returns `{ released: false }` and cannot release a newer claim. Reclaiming an expired or released record increments its generation.

Tracked `patch:`, `delete:`, `seed:` synchronization, conflicting `post:` operations, and nested cascade deletes are blocked when they touch an actively leased record unless the matching `$lease` token and owner are provided. Bulk writes are rejected atomically if any selected row is held. Mutations with a stale supplied token are rejected; tokenless mutations may proceed after expiration. Lease commands cannot run inside `$tx` because a human review must not depend on an open database transaction. Writes through `raw:` or outside lumb bypass lease enforcement.

`$actor` must be derived by the application from its authenticated user; do not trust an operator ID supplied directly by a client. Lease rows remain in the internal `_lumb_leases` table after release or expiration so the generation stays monotonic; the table retains one small row per resource ever claimed.
