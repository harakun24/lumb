import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import db from "../src/index.js";
import { executeCrossBatch, resolveExecutionTarget } from "../src/transaction.js";

afterEach(async () => {
  await db("close:");
});

test("scope context overrides caller filters and write values", async () => {
  await db("make:items", { org: String, name: String });
  await db("post:items", [
    { org: "A", name: "visible" },
    { org: "B", name: "private" }
  ]);
  const scoped = db("scope:items", { org: "A" });

  assert.deepEqual(
    await scoped("get:items", { org: "B" }),
    [{ id: 1, org: "A", name: "visible" }]
  );
  await scoped("post:items", { org: "B", name: "scoped insert" });
  await scoped(
    "patch:items",
    { $where: { org: "B" }, name: "changed" },
    { $where: { org: "B" } }
  );
  await scoped("delete:items/2", {}, { $where: { org: "B" } });
  await scoped(
    "patch:items",
    [{ id: 2, name: "batch changed", $where: { org: "B" } }],
    { $where: { org: "B" } }
  );
  await scoped("delete:items", [2], { $where: { org: "B" } });

  assert.deepEqual(await db("get:items", { $sort: "id:asc" }), [
    { id: 1, org: "A", name: "changed" },
    { id: 2, org: "B", name: "private" },
    { id: 3, org: "A", name: "changed" }
  ]);
});

test("$expect blocks stale mutations without changing the record", async () => {
  await db("make:orders", { status: String, note: String });
  const order = await db("post:orders", { status: "pending", note: "" });

  await db(`patch:orders/${order.id}`, { status: "approved" }, {
    $expect: { status: "pending" }
  });
  await assert.rejects(
    db(`patch:orders/${order.id}`, { note: "stale write" }, {
      $expect: { status: "pending" }
    }),
    (error) => error.name === "DataOperationBlockedError" &&
      error.blockers[0].type === "expectation"
  );
  await assert.rejects(
    db(`delete:orders/${order.id}`, {}, { $expect: { status: "pending" } }),
    (error) => error.name === "DataOperationBlockedError" &&
      error.blockers[0].type === "expectation"
  );
  assert.deepEqual(await db(`get:orders/${order.id}`), {
    id: order.id,
    status: "approved",
    note: ""
  });
});

test("$maxAffected aborts a mutation before it changes too many rows", async () => {
  await db("make:orders", { status: String });
  await db("post:orders", [{ status: "pending" }, { status: "pending" }]);

  await assert.rejects(
    db("patch:orders", { $where: { status: "pending" }, status: "approved" }, {
      $maxAffected: 1
    }),
    (error) => error.name === "DataOperationBlockedError" &&
      error.blockers[0].type === "max-affected"
  );
  assert.deepEqual(
    (await db("get:orders", { $sort: "id:asc" })).map((order) => order.status),
    ["pending", "pending"]
  );

  await db("make:parents", {
    name: String,
    children: [{ name: String }]
  });
  const parent = await db("post:parents", {
    name: "parent",
    children: [{ name: "child" }]
  });
  await assert.rejects(
    db(`delete:parents/${parent.id}`, {}, { $maxAffected: 1 }),
    (error) => error.name === "DataOperationBlockedError" &&
      error.blockers[0].count === 2
  );
  assert.equal((await db("get:parents")).length, 1);
  assert.equal((await db("get:parents_children")).length, 1);
});

test("$once replays the original typed result and rejects key reuse", async () => {
  await db("make:payments", { reference: String, createdAt: Date });
  const command = "post:payments";
  const payload = { reference: "PAY-1", createdAt: new Date("2025-01-02T03:04:05.000Z") };
  const first = await db(command, payload, { $once: "payment:create:PAY-1" });
  const replay = await db(command, payload, { $once: "payment:create:PAY-1" });

  assert.deepEqual(replay, first);
  assert.ok(replay.createdAt instanceof Date);
  assert.equal((await db("get:payments")).length, 1);
  await assert.rejects(
    db(command, { ...payload, reference: "PAY-2" }, { $once: "payment:create:PAY-1" }),
    (error) => error.name === "DataOperationBlockedError" &&
      error.blockers[0].type === "once-conflict"
  );
  assert.equal((await db("get:payments")).length, 1);
});

test("$outbox enqueues events atomically and claims them with acknowledgement tokens", async () => {
  await db("make:orders", { status: String });
  const order = await db("post:orders", { status: "paid" }, {
    $outbox: {
      type: "order.paid",
      payload: { orderId: 1 }
    }
  });
  const claimed = await db("outbox:claim", {}, { $worker: "mailer" });
  assert.equal(claimed.length, 1);
  assert.equal(claimed[0].type, "order.paid");
  assert.deepEqual(claimed[0].payload, { orderId: order.id });
  assert.equal(claimed[0].attempts, 1);
  assert.deepEqual(await db("outbox:claim", {}, { $worker: "other" }), []);

  assert.deepEqual(
    await db(`outbox:ack/${claimed[0].id}`, {}, { $claim: claimed[0].claimToken }),
    { settled: true, status: "delivered" }
  );
  assert.deepEqual(
    await db(`outbox:ack/${claimed[0].id}`, {}, { $claim: claimed[0].claimToken }),
    { settled: false, reason: "claim-changed" }
  );
  await db(`patch:orders/${order.id}`, { status: "paid" }, {
    $outbox: { type: "order.paid", payload: { orderId: order.id } }
  });
  assert.deepEqual(await db("outbox:claim", {}, { $worker: "mailer" }), []);

  await db("post:orders", { status: "paid" }, {
    $outbox: { type: "order.receipt", payload: { orderId: order.id } }
  });
  const retry = (await db("outbox:claim", {}, { $worker: "mailer" }))[0];
  assert.deepEqual(
    await db(`outbox:fail/${retry.id}`, {}, {
      $claim: retry.claimToken,
      $retryAfter: 0,
      $error: "temporary publish failure"
    }),
    { settled: true, status: "queued" }
  );
  const redelivered = (await db("outbox:claim", {}, { $worker: "mailer" }))[0];
  assert.equal(redelivered.attempts, 2);
});

test("$outbox write failures roll back the associated mutation", async () => {
  await db("make:orders", { status: String });
  await assert.rejects(
    db("post:orders", { status: "paid" }, { $outbox: { payload: {} } }),
    /type.*string/
  );
  assert.deepEqual(await db("get:orders"), []);
});

test("$redact filters returned data and events without changing stored values", async () => {
  await db("make:users", { email: String, password: String });
  const events = [];
  const unsubscribe = db.on("post:users", (detail) => events.push(detail));

  const created = await db("post:users", {
    email: "operator@example.test",
    password: "secret"
  }, { $redact: ["password"] });
  unsubscribe();

  assert.equal(created.password, "[REDACTED]");
  assert.equal(events[0].password, "[REDACTED]");
  assert.equal((await db(`get:users/${created.id}`)).password, "secret");
  assert.equal(
    (await db("get:users", {}, { $redact: ["email", "password"] }))[0].email,
    "[REDACTED]"
  );
});

test("$stream pages by primary key and redacts rows as they are consumed", async () => {
  await db("make:events", { message: String, token: String });
  await db("post:events", [
    { message: "one", token: "secret-1" },
    { message: "two", token: "secret-2" },
    { message: "three", token: "secret-3" }
  ]);
  const stream = await db("get:events", {}, {
    $stream: 2,
    $redact: ["token"]
  });
  const rows = [];
  for await (const row of stream) rows.push(row);

  assert.deepEqual(rows.map((row) => row.id), [1, 2, 3]);
  assert.deepEqual(rows.map((row) => row.token), [
    "[REDACTED]",
    "[REDACTED]",
    "[REDACTED]"
  ]);

  const limited = await db("get:events", {}, { $stream: true, $limit: 2 });
  const limitedRows = [];
  for await (const row of limited) limitedRows.push(row);
  assert.equal(limitedRows.length, 2);
  await assert.rejects(
    db("get:events", {}, { $stream: true, $sort: "message:asc" }),
    /tidak menerima filter primary key|mendukung query/
  );
  await assert.rejects(
    db("batch:", [["get:events", {}, { $stream: true }]]),
    /tidak dapat digunakan di dalam transaksi/
  );
});

test("$at reconstructs a record from its tracked timeline", async () => {
  await db("make:orders", { status: String, changedAt: Date });
  const firstTime = new Date("2025-02-01T10:00:00.000Z");
  const secondTime = new Date("2025-02-02T10:00:00.000Z");
  const order = await db("post:orders", { status: "pending", changedAt: firstTime });
  const inserted = (await db(`history:orders/${order.id}`))[0];
  await db(`patch:orders/${order.id}`, { status: "approved", changedAt: secondTime });
  const changed = (await db(`history:orders/${order.id}`))[0];

  const before = await db(`get:orders/${order.id}`, {}, { $at: inserted.createdAt });
  const after = await db(`get:orders/${order.id}`, {}, { $at: changed.createdAt });
  assert.equal(before.status, "pending");
  assert.ok(before.changedAt instanceof Date);
  assert.equal(before.changedAt.toISOString(), firstTime.toISOString());
  assert.equal(after.status, "approved");
  await assert.rejects(
    db("get:orders", {}, { $at: inserted.createdAt }),
    /single record|satu record/
  );
});

test("$retry reruns a transient batch failure without publishing failed-attempt events", async () => {
  await db("make:items", { name: String });
  await db("batch:", []);
  let attempts = 0;
  const events = [];
  const result = await executeCrossBatch(
    [["post:items", { name: "once" }, {}]],
    async (command, payload, options, eventSink) => {
      attempts += 1;
      const { conn, runner } = resolveExecutionTarget(options);
      const q = conn.dialect.quoteId;
      await runner.run(
        `INSERT INTO ${q("items")} (${q("name")}) VALUES (${conn.dialect.param(1)});`,
        [conn.dialect.serialize(payload.name)]
      );
      eventSink("post:items", { name: payload.name });
      if (attempts === 1) {
        throw Object.assign(new Error("temporary lock"), { code: "ERR_SQLITE_BUSY" });
      }
      return { inserted: 1 };
    },
    { $retry: 1 },
    (name, detail) => events.push({ name, detail })
  );

  assert.equal(attempts, 2);
  assert.equal(result.length, 1);
  assert.equal((await db("get:items")).length, 1);
  assert.equal(events.filter((event) => event.name === "post:items").length, 1);
});

test("$retry refuses schemas with callbacks that might have external side effects", async () => {
  await db("make:users", {
    name: String,
    deletedAt: [Date, null],
    remove() {
      this.deletedAt = new Date();
    }
  });

  await assert.rejects(
    db("batch:", [], { $retry: true }),
    /soft-delete callback/
  );
});

test("scope context is retained inside cross-command batches", async () => {
  await db("make:items", { org: String, name: String });
  await db("post:items", [
    { org: "A", name: "visible" },
    { org: "B", name: "private" }
  ]);
  const scoped = db("scope:items", { org: "A" });

  await scoped("batch:", [
    ["patch:items", { $where: { org: "B" }, name: "batch changed" }, { $where: { org: "B" } }],
    ["delete:items", [2], { $where: { org: "B" } }],
    ["post:items", { org: "B", name: "batch insert" }]
  ]);

  assert.deepEqual(await db("get:items", { $sort: "id:asc" }), [
    { id: 1, org: "A", name: "batch changed" },
    { id: 2, org: "B", name: "private" },
    { id: 3, org: "A", name: "batch insert" }
  ]);
});

test("closing a transaction invalidates its active savepoints", async () => {
  await db("make:items", { name: String });
  const tx = await db("begin:");
  const savepoint = await db(`begin:${tx}`);
  await db(`rollback:${tx}`);

  await assert.rejects(
    db("post:items", { name: "must not persist", $tx: savepoint }),
    /kedaluwarsa atau ditutup/
  );
  assert.deepEqual(await db("get:items"), []);
});

test("trash mode applies to nested relations", async () => {
  await db("make:parents", {
    name: String,
    children: [{
      name: String,
      deletedAt: [Date, null],
      remove() {
        this.deletedAt = new Date();
      }
    }]
  });
  const parent = await db("post:parents", {
    name: "parent",
    children: [{ name: "child" }]
  });
  await db(`delete:parents_children/${parent.children[0].id}`);

  assert.equal((await db("get:parents", { $with: { children: true } }))[0].children.length, 0);
  const included = await db("get:parents", {
    $trash: "include",
    $with: { children: true }
  });
  assert.equal(included[0].children.length, 1);
  assert.ok(included[0].children[0].deletedAt instanceof Date);
});

test("SQL joins exclude soft-deleted related rows by default", async () => {
  await db("make:authors", {
    name: String,
    deletedAt: [Date, null],
    remove() {
      this.deletedAt = new Date();
    }
  });
  await db("make:posts", { title: String, author: "@authors" });
  const author = await db("post:authors", { name: "deleted" });
  await db("post:posts", { title: "post", author: author.id });
  await db(`patch:authors/${author.id}`, { deletedAt: new Date() });

  assert.deepEqual(
    await db("get:posts", { $with: { author: { $join: "inner" } } }),
    []
  );
  assert.equal(
    (await db("get:posts", {
      $trash: "include",
      $with: { author: { $join: "inner" } }
    }))[0].author.name,
    "deleted"
  );
});

test("field projection retains internal keys needed for eager relations", async () => {
  await db("make:parents", {
    name: String,
    children: [{ name: String }]
  });
  await db("post:parents", {
    name: "parent",
    children: [{ name: "child" }]
  });

  assert.deepEqual(
    await db("get:parents", {
      $fields: ["name"],
      $with: { children: { $fields: ["name"] } }
    }),
    [{ name: "parent", children: [{ name: "child" }] }]
  );
});

test("field projection retains keys for many-to-one and many-to-many relations", async () => {
  await db("make:categories", { name: String });
  await db("make:tags", { name: String });
  await db("make:products", {
    name: String,
    category: "@categories",
    tags: ["@tags"]
  });
  const category = await db("post:categories", { name: "category" });
  const tag = await db("post:tags", { name: "tag" });
  await db("post:products", {
    name: "product",
    category: category.id,
    tags: [tag.id]
  });

  assert.deepEqual(
    await db("get:products", {
      $fields: ["name"],
      $with: {
        category: { $fields: ["name"] },
        tags: { $fields: ["name"] }
      }
    }),
    [{
      name: "product",
      category: { name: "category" },
      tags: [{ name: "tag" }]
    }]
  );
});

test("batch inserts preserve explicit IDs in mixed auto-ID rows", async () => {
  await db("make:items", { name: String });
  await db("post:items", [
    { name: "generated" },
    { id: 42, name: "explicit" }
  ]);

  assert.deepEqual(await db("get:items", { $sort: "id:asc" }), [
    { id: 1, name: "generated" },
    { id: 42, name: "explicit" }
  ]);
});

test("dry runs do not register tables that were not created", async () => {
  await db("make:planned_items", { name: String }, { $dryRun: true });

  await assert.rejects(
    db("get:planned_items"),
    /belum didefinisikan/
  );
});

test("inserting an empty auto-ID row uses the dialect default", async () => {
  await db("make:empty_items", {});

  assert.deepEqual(await db("post:empty_items", {}), { id: 1 });
});

test("data contracts reject invalid changes and roll back their savepoint", async () => {
  await db("make:inventory", { name: String, stock: 0 }, {
    $contracts: [{
      id: "stock-not-negative",
      check: ({ after }) => after === null || after.stock >= 0,
      message: "Stock cannot be negative."
    }]
  });
  const item = await db("post:inventory", { name: "item", stock: 2 });
  const tx = await db("begin:");

  await assert.rejects(
    db(`patch:inventory/${item.id}`, { stock: -1 }, { $tx: tx }),
    /stock-not-negative/
  );
  await db(`commit:${tx}`);
  assert.equal((await db(`get:inventory/${item.id}`)).stock, 2);

  await assert.rejects(
    db("post:inventory", [
      { name: "valid", stock: 1 },
      { name: "invalid", stock: -1 }
    ]),
    /stock-not-negative/
  );
  assert.equal((await db("get:inventory")).length, 1);
});

test("contract checks cannot mutate the rows saved to history", async () => {
  await db("make:items", { stock: 0 }, {
    $contracts: [{
      id: "read-only-contract",
      check: ({ after }) => {
        after.stock = -100;
        return true;
      },
      message: "This check always passes."
    }]
  });
  const item = await db("post:items", { stock: 4 });

  assert.equal((await db(`get:items/${item.id}`)).stock, 4);
  const history = await db(`history:items/${item.id}`);
  assert.equal(history[0].changes[0].after.stock, 4);
});

test("undo cannot restore data that violates an active contract", async () => {
  await db("make:inventory", { stock: 0 });
  const item = await db("post:inventory", { stock: -1 });
  await db("make:inventory", { stock: 0 }, {
    $contracts: [{
      id: "stock-not-negative",
      check: ({ after }) => after === null || after.stock >= 0,
      message: "Stock cannot be negative."
    }]
  });
  await db(`patch:inventory/${item.id}`, { stock: 1 });
  const patch = (await db(`history:inventory/${item.id}`))
    .find((entry) => entry.action === "patch");

  await assert.rejects(db(`undo:${patch.id}`), /stock-not-negative/);
  assert.equal((await db(`get:inventory/${item.id}`)).stock, 1);
});

test("impact previews changes without writes or mutation events", async () => {
  await db("make:inventory", { name: String, stock: 0 }, {
    $contracts: [{
      id: "stock-not-negative",
      check: ({ after }) => after === null || after.stock >= 0,
      message: "Stock cannot be negative."
    }]
  });
  const item = await db("post:inventory", { name: "item", stock: 2 });
  let mutationEvents = 0;
  const unsubscribe = db.on("patch:inventory", () => mutationEvents++);

  const plan = await db("impact:", {
    command: `patch:inventory/${item.id}`,
    payload: { stock: 0 }
  });
  assert.equal(plan.canApply, true);
  assert.equal(plan.affectedCount, 1);
  assert.equal((await db(`get:inventory/${item.id}`)).stock, 2);
  assert.equal(mutationEvents, 0);

  const blocked = await db("impact:", {
    command: `patch:inventory/${item.id}`,
    payload: { stock: -1 }
  });
  assert.equal(blocked.canApply, false);
  assert.equal(blocked.contractViolations[0].contract, "stock-not-negative");
  assert.equal((await db(`get:inventory/${item.id}`)).stock, 2);
  unsubscribe();
});

test("first impact call leaves timeline storage ready for later mutations", async () => {
  await db("make:items", { name: String });
  const plan = await db("impact:", {
    command: "post:items",
    payload: { name: "preview" }
  });
  assert.equal(plan.canApply, true);
  assert.deepEqual(await db("get:items"), []);

  const item = await db("post:items", { name: "persisted" });
  assert.equal((await db(`get:items/${item.id}`)).name, "persisted");
  assert.equal((await db("history:items")).length, 1);
});

test("contracts validate nested child writes and cascades", async () => {
  await db("make:parents", {
    name: String,
    children: [{
      name: String,
      $contracts: [{
        id: "child-delete-prohibited",
        check: ({ action }) => action !== "delete",
        message: "Child rows cannot be deleted."
      }, {
        id: "child-name-required",
        check: ({ after }) => after === null || after.name.length > 0,
        message: "Child rows need a name."
      }]
    }]
  });
  const parent = await db("post:parents", {
    name: "parent",
    children: [{ name: "child" }]
  });
  const insertPlan = await db("impact:", {
    command: "post:parents",
    payload: { name: "planned", children: [{ name: "planned child" }] }
  });
  assert.equal(insertPlan.complete, true);
  assert.equal(insertPlan.affectedCount, 2);
  assert.equal((await db("get:parents")).length, 1);
  await assert.rejects(
    db("post:parents", { name: "invalid", children: [{ name: "" }] }),
    /child-name-required/
  );
  assert.equal((await db("get:parents")).length, 1);

  const plan = await db("impact:", {
    command: `delete:parents/${parent.id}`
  });
  assert.equal(plan.canApply, false);
  assert.equal(plan.complete, false);
  assert.equal(plan.contractViolations[0].contract, "child-delete-prohibited");
  assert.ok(plan.changes.some((change) => change.table === "parents_children"));
  assert.ok(await db(`get:parents/${parent.id}`));

  await assert.rejects(
    db(`delete:parents/${parent.id}`),
    /child-delete-prohibited/
  );
  assert.equal((await db("get:parents_children")).length, 1);
});

test("impact previews relation integrity blockers", async () => {
  await db("make:authors", { name: String });
  await db("make:posts", { title: String, author: "@authors" });
  const author = await db("post:authors", { name: "author" });
  await db("post:posts", { title: "post", author: author.id });

  const plan = await db("impact:", {
    command: `delete:authors/${author.id}`
  });
  assert.equal(plan.canApply, false);
  assert.equal(plan.blockedBy[0].type, "relation");
  assert.equal((await db(`get:authors/${author.id}`)).name, "author");
});

test("impact previews database constraint blockers", async () => {
  await db("make:users", { email: new Set([String]) });
  await db("post:users", { email: "person@example.test" });

  const plan = await db("impact:", {
    command: "post:users",
    payload: { email: "person@example.test" }
  });
  assert.equal(plan.canApply, false);
  assert.equal(plan.blockedBy[0].type, "database-constraint");
  assert.equal((await db("get:users")).length, 1);
});

test("impact previews owned-child cascades without persisting them", async () => {
  await db("make:parents", {
    name: String,
    children: [{ name: String }]
  });
  const parent = await db("post:parents", {
    name: "parent",
    children: [{ name: "child" }]
  });

  const plan = await db("impact:", {
    command: `delete:parents/${parent.id}`
  });
  assert.equal(plan.canApply, true);
  assert.equal(plan.complete, true);
  assert.equal(plan.affectedCount, 2);
  assert.deepEqual(
    new Set(plan.changes.map((change) => change.table)),
    new Set(["parents", "parents_children"])
  );
  assert.equal((await db("get:parents")).length, 1);
  assert.equal((await db("get:parents_children")).length, 1);
});

test("history, why, and undo use persisted row changes", async () => {
  await db("make:items", { name: String, qty: 0 });
  const item = await db("post:items", { name: "item", qty: 1 });
  await db(`patch:items/${item.id}`, { qty: 2 }, {
    $actor: { id: "worker-1" },
    $reason: "correction"
  });

  const history = await db(`history:items/${item.id}`);
  const correction = history.find((entry) => entry.reason === "correction");
  assert.ok(correction);
  assert.deepEqual(await db(`why:items/${item.id}/qty`), [
    {
      changeId: correction.id,
      createdAt: correction.createdAt,
      actor: { id: "worker-1" },
      reason: "correction",
      source: null,
      operation: "patch",
      before: 1,
      after: 2
    },
    {
      changeId: history.find((entry) => entry.action === "post").id,
      createdAt: history.find((entry) => entry.action === "post").createdAt,
      actor: null,
      reason: null,
      source: null,
      operation: "insert",
      before: null,
      after: 1
    }
  ]);

  await db(`undo:${correction.id}`);
  assert.equal((await db(`get:items/${item.id}`)).qty, 1);
  const post = history.find((entry) => entry.action === "post");
  await db(`undo:${post.id}`);
  assert.equal(await db(`get:items/${item.id}`), null);
});

test("snapshots diff and restore current table data", async () => {
  await db("make:items", { name: String, qty: 0 });
  const item = await db("post:items", { name: "item", qty: 1 });
  const snapshot = await db("snapshot:items", { $label: "baseline" });
  await db(`patch:items/${item.id}`, { qty: 9 });

  const diff = await db("diff:items", { $from: snapshot.id });
  assert.deepEqual(diff.changes.map((change) => change.operation), ["changed"]);
  const plan = await db("restore:items", { $from: snapshot.id, $dryRun: true });
  assert.equal(plan.changes.length, 1);
  assert.equal((await db(`get:items/${item.id}`)).qty, 9);

  await db("restore:items", { $from: snapshot.id });
  assert.equal((await db(`get:items/${item.id}`)).qty, 1);
});

test("undo restores deleted rows and their many-to-many links", async () => {
  await db("make:tags", { name: String });
  await db("make:items", {
    name: String,
    tags: ["@tags"]
  });
  const firstTag = await db("post:tags", { name: "first" });
  const secondTag = await db("post:tags", { name: "second" });
  const item = await db("post:items", {
    name: "item",
    tags: [firstTag.id]
  });

  await db(`patch:items/${item.id}`, { tags: [secondTag.id] });
  const patch = (await db(`history:items/${item.id}`)).find((entry) => entry.action === "patch");
  await db(`undo:${patch.id}`);
  assert.deepEqual(
    (await db(`get:items/${item.id}`, { $with: { tags: true } })).tags.map((tag) => tag.id),
    [firstTag.id]
  );

  await db(`delete:items/${item.id}`);
  const deletion = (await db(`history:items/${item.id}`)).find((entry) => entry.action === "delete");
  await db(`undo:${deletion.id}`);
  assert.deepEqual(
    (await db(`get:items/${item.id}`, { $with: { tags: true } })).tags.map((tag) => tag.id),
    [firstTag.id]
  );
});

test("snapshot restore rewinds many-to-many links", async () => {
  await db("make:tags", { name: String });
  await db("make:items", {
    name: String,
    tags: ["@tags"]
  });
  const firstTag = await db("post:tags", { name: "first" });
  const secondTag = await db("post:tags", { name: "second" });
  const item = await db("post:items", {
    name: "item",
    tags: [firstTag.id]
  });
  const snapshot = await db("snapshot:items", { $with: { tags: true } });
  await db(`patch:items/${item.id}`, { tags: [secondTag.id] });

  await db("restore:items", { $from: snapshot.id });
  assert.deepEqual(
    (await db(`get:items/${item.id}`, { $with: { tags: true } })).tags.map((tag) => tag.id),
    [firstTag.id]
  );
});

test("branches preview staged writes and reject stale merges", async () => {
  await db("make:items", { name: String });
  const item = await db("post:items", { name: "live" });
  const branch = await db(`branch:items/${item.id}`, { $label: "draft" });
  await db(`patch:items/${item.id}`, { name: "draft" }, { $branch: branch.id });

  assert.equal((await db(`get:items/${item.id}`, {}, { $branch: branch.id })).name, "draft");
  assert.equal((await db(`get:items/${item.id}`)).name, "live");
  assert.equal((await db(`merge:${branch.id}`, { $dryRun: true })).dryRun, true);
  assert.equal((await db(`get:items/${item.id}`)).name, "live");
  assert.equal((await db(`merge:${branch.id}`)).merged, true);
  assert.equal((await db(`get:items/${item.id}`)).name, "draft");

  const staleBranch = await db(`branch:items/${item.id}`);
  await db(`patch:items/${item.id}`, { name: "changed elsewhere" });
  await db(`patch:items/${item.id}`, { name: "stale draft" }, { $branch: staleBranch.id });
  const merge = await db(`merge:${staleBranch.id}`);
  assert.equal(merge.merged, false);
  assert.equal(merge.conflicts.length, 1);
});

test("branches detect many-to-many changes to their base record", async () => {
  await db("make:tags", { name: String });
  await db("make:items", { name: String, tags: ["@tags"] });
  const firstTag = await db("post:tags", { name: "first" });
  const secondTag = await db("post:tags", { name: "second" });
  const item = await db("post:items", { name: "item", tags: [firstTag.id] });
  const branch = await db(`branch:items/${item.id}`);
  await db(`patch:items/${item.id}`, { tags: [secondTag.id] });

  await assert.rejects(
    db(`get:items/${item.id}`, {}, { $branch: branch.id }),
    /konflik karena data dasarnya telah berubah/
  );
  const merge = await db(`merge:${branch.id}`);
  assert.equal(merge.merged, false);
  assert.equal(merge.conflicts.length, 1);
});

test("forget nulls selected fields and scrubs tracked copies and pending events", async () => {
  await db("make:users", {
    name: String,
    email: [String, null],
    note: [String, null]
  });
  const user = await db("post:users", {
    name: "Ada",
    email: "forgotten@example.test",
    note: "keep this"
  }, {
    $once: "user:create:ada",
    $outbox: {
      type: "user.created",
      payload: { email: "forgotten@example.test" }
    }
  });
  const snapshot = await db("snapshot:users");
  const branch = await db(`branch:users/${user.id}`);
  await db(`patch:users/${user.id}`, {
    email: "branch-copy@example.test"
  }, { $branch: branch.id });

  const result = await db(`forget:users/${user.id}`, {}, { $fields: ["email"] });
  assert.deepEqual(result, {
    forgotten: true,
    table: "users",
    fields: ["email"],
    referenceCount: 2
  });
  assert.deepEqual(await db(`get:users/${user.id}`), {
    id: user.id,
    name: "Ada",
    email: null,
    note: "keep this"
  });

  const history = await db(`history:users/${user.id}`);
  assert.equal(JSON.stringify(history).includes("forgotten@example.test"), false);
  const historical = await db(`get:users/${user.id}`, {}, {
    $at: history[history.length - 1].createdAt
  });
  assert.equal(historical.email, null);
  await assert.rejects(
    db(`get:users/${user.id}`, {}, { $branch: branch.id }),
    /Branch .*tidak ditemukan atau sudah ditutup/
  );

  const snapshotRow = await db("raw:SELECT payload FROM _lumb_timeline WHERE kind = 'snapshot'");
  assert.equal(snapshotRow[0].payload.includes("forgotten@example.test"), false);
  assert.equal(JSON.parse(snapshotRow[0].payload).rows[0].email, null);
  const timeline = await db("raw:SELECT payload FROM _lumb_timeline");
  assert.equal(JSON.stringify(timeline).includes("branch-copy@example.test"), false);

  const outbox = await db("raw:SELECT event_type, payload, status FROM _lumb_outbox");
  assert.deepEqual(outbox, [{
    event_type: "forgotten",
    payload: "null",
    status: "forgotten"
  }]);
  assert.deepEqual(await db("outbox:claim", {}, { $worker: "mailer" }), []);

  const once = await db("raw:SELECT fingerprint, result FROM _lumb_once");
  assert.equal(once[0].fingerprint, "forgotten");
  assert.equal(JSON.parse(once[0].result).type, "value");
  assert.equal(JSON.parse(once[0].result).value, null);
  await assert.rejects(
    db("post:users", {
      name: "Ada",
      email: "forgotten@example.test",
      note: "keep this"
    }, { $once: "user:create:ada" }),
    (error) => error.name === "DataOperationBlockedError" &&
      error.blockers[0].type === "once-conflict"
  );

  assert.deepEqual(
    (await db("diff:users", {}, { $from: snapshot.id })).changes,
    []
  );
});

test("forget only accepts nullable non-primary-key columns", async () => {
  await db("make:users", { email: [String, null], name: String });
  const user = await db("post:users", { email: "private", name: "Ada" });

  await assert.rejects(
    db(`forget:users/${user.id}`, {}, { $fields: ["name"] }),
    /tidak nullable/
  );
  await assert.rejects(
    db(`forget:users/${user.id}`, {}, { $fields: ["id"] }),
    /Primary key/
  );
  await assert.rejects(
    db("forget:users/999", {}, { $fields: ["email"] }),
    /tidak ditemukan/
  );
  assert.equal((await db(`get:users/${user.id}`)).email, "private");
});

test("forget references are recorded for batch mutations", async () => {
  await db("make:users", { email: [String, null] });
  const [user] = await db("batch:", [
    ["post:users", { email: "batch-private@example.test" }, {
      $once: "batch-user:create",
      $outbox: {
        type: "user.created",
        payload: { email: "batch-private@example.test" }
      }
    }]
  ]);

  const result = await db(`forget:users/${user.id}`, {}, { $fields: ["email"] });
  assert.equal(result.referenceCount, 2);
  assert.equal((await db("outbox:claim", {}, { $worker: "mailer" })).length, 0);
  const once = await db("raw:SELECT fingerprint FROM _lumb_once");
  assert.equal(once[0].fingerprint, "forgotten");
});

test("forget invalidates snapshots that omitted related record identifiers", async () => {
  await db("make:parents", {
    name: String,
    children: [{ email: [String, null] }]
  });
  const parent = await db("post:parents", {
    name: "parent",
    children: [{ email: "child-private@example.test" }]
  });
  const snapshot = await db("snapshot:parents", {
    $with: { children: { $fields: ["email"] } }
  });
  const childId = parent.children[0].id;

  await db(`forget:parents_children/${childId}`, {}, { $fields: ["email"] });
  const entry = await db(
    `raw:SELECT payload FROM _lumb_timeline WHERE id = '${snapshot.id}'`
  );
  assert.equal(entry[0].payload.includes("child-private@example.test"), false);
  assert.equal(JSON.parse(entry[0].payload).status, "forgotten");
  assert.deepEqual(JSON.parse(entry[0].payload).rows, []);
  await assert.rejects(
    db("diff:parents", {}, { $from: snapshot.id }),
    /data di dalamnya telah dilupakan/
  );
});

test("drop requires explicit cascade and removes dependent schema metadata", async () => {
  await db("make:tags", { name: String });
  await db("make:parents", {
    name: String,
    children: [{ label: String }],
    tags: ["@tags"]
  });
  await db("make:posts", { title: String, parent: "@parents" });
  const tag = await db("post:tags", { name: "tag" });
  const parent = await db("post:parents", {
    name: "parent",
    children: [{ label: "child" }],
    tags: [tag.id]
  });
  await db("post:posts", { title: "post", parent: parent.id });

  await assert.rejects(
    db("drop:parents"),
    /memiliki dependensi/
  );
  assert.equal((await db(`get:parents/${parent.id}`)).name, "parent");

  const plan = await db("drop:parents", {}, {
    $cascade: true,
    $dryRun: true
  });
  assert.equal(plan.dryRun, true);
  assert.ok(plan.tables.includes("parents_children"));
  assert.ok(plan.tables.includes("_rel_parents_tags"));
  assert.ok(plan.tables.includes("posts"));
  assert.equal((await db("get:parents")).length, 1);

  const dropped = await db("drop:parents", {}, { $cascade: true });
  assert.deepEqual(new Set(dropped.tables), new Set(plan.tables));
  await assert.rejects(db("get:parents"), /belum didefinisikan/);
  await assert.rejects(db("get:parents_children"), /belum didefinisikan/);
  await assert.rejects(db("get:posts"), /belum didefinisikan/);
  assert.equal((await db(`get:tags/${tag.id}`)).name, "tag");

  await db("make:temporary_items", { name: String });
  assert.deepEqual(await db("drop:temporary_items"), {
    dropped: true,
    table: "temporary_items",
    tables: ["temporary_items"]
  });
  await assert.rejects(db("get:temporary_items"), /belum didefinisikan/);
});

test("$purpose records privacy-safe access receipts", async () => {
  await db("make:users", { email: String, name: String });
  await db("post:users", {
    email: "private@example.test",
    name: "Ada"
  });

  const result = await db("get:users", {
    email: "private@example.test",
    $purpose: "customer support",
    $actor: { id: "agent-7" },
    $fields: ["name"]
  });
  assert.deepEqual(result, [{ name: "Ada" }]);

  const [receipt] = await db("access:history", {}, {
    $actor: { id: "agent-7" },
    $purpose: "customer support"
  });
  assert.equal(receipt.actorId, "agent-7");
  assert.equal(receipt.purpose, "customer support");
  assert.equal(receipt.table, "users");
  assert.deepEqual(receipt.fields, ["name"]);
  assert.deepEqual(receipt.filterFields, ["email"]);
  assert.equal(receipt.recordCount, 1);
  assert.equal(JSON.stringify(receipt).includes("private@example.test"), false);
  assert.equal(JSON.stringify(receipt).includes("Ada"), false);

  await assert.rejects(
    db("get:users", {}, { $purpose: "support" }),
    /mewajibkan/
  );
  await assert.rejects(
    db("get:users", {}, {
      $purpose: "support",
      $actor: { id: "agent-7" },
      $stream: true
    }),
    /belum mendukung/
  );
  await assert.rejects(
    db("get:users", {}, {
      $purpose: "support",
      $actor: { id: "agent-7" },
      $tx: "invalid-tx"
    }),
    /receipt akses harus tetap tercatat/
  );
});
