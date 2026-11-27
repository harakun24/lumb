import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import db from "../src/index.js";

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
