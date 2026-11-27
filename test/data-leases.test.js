import assert from "node:assert/strict";
import { afterEach, test } from "node:test";
import db from "../src/index.js";

afterEach(async () => {
  await db("close:");
});

test("leases can be claimed, inspected, enforced, released, and reclaimed", async () => {
  await db("make:orders", { status: String });
  const order = await db("post:orders", { status: "pending" });
  const actor = { id: "operator-1", name: "Rina" };

  const lease = await db(`claim:orders/${order.id}`, {}, { $actor: actor });
  assert.equal(lease.claimed, true);
  assert.equal(lease.owner.id, actor.id);
  assert.equal(lease.generation, 1);
  assert.equal((await db(`lease:orders/${order.id}`)).held, true);

  const conflict = await db(`claim:orders/${order.id}`, {}, {
    $actor: { id: "operator-2" }
  });
  assert.equal(conflict.claimed, false);
  assert.equal(conflict.lease.owner.id, actor.id);

  await assert.rejects(
    db(`patch:orders/${order.id}`, { status: "approved" }),
    /sedang dipegang/
  );
  await db(`patch:orders/${order.id}`, { status: "approved" }, {
    $actor: actor,
    $lease: lease.token,
    $reason: "review complete"
  });
  assert.equal((await db(`get:orders/${order.id}`)).status, "approved");

  assert.deepEqual(
    await db(`release:orders/${order.id}`, {}, {
      $actor: actor,
      $lease: lease.token
    }),
    { released: true, generation: 1 }
  );
  assert.equal((await db(`lease:orders/${order.id}`)).held, false);

  const nextLease = await db(`claim:orders/${order.id}`, {}, {
    $actor: { id: "operator-2" }
  });
  assert.equal(nextLease.generation, 2);
});

test("lease renewal requires the active owner's token and extends its expiry", async () => {
  await db("make:orders", { status: String });
  const order = await db("post:orders", { status: "pending" });
  const actor = { id: "operator-1" };
  const lease = await db(`claim:orders/${order.id}`, {}, {
    $actor: actor,
    $ttl: 1000
  });

  await assert.rejects(
    db(`renew:orders/${order.id}`, {}, {
      $actor: { id: "operator-2" },
      $lease: lease.token
    }),
    /tidak valid/
  );

  const renewed = await db(`renew:orders/${order.id}`, {}, {
    $actor: actor,
    $lease: lease.token,
    $ttl: 5000
  });
  assert.equal(renewed.renewed, true);
  assert.equal(renewed.generation, lease.generation);
  assert.ok(Date.parse(renewed.expiresAt) > Date.parse(lease.expiresAt));
});

test("concurrent claims produce one holder and stale releases cannot clear a new lease", async () => {
  await db("make:orders", { status: String });
  const order = await db("post:orders", { status: "pending" });

  const [first, second] = await Promise.all([
    db(`claim:orders/${order.id}`, {}, { $actor: { id: "operator-1" } }),
    db(`claim:orders/${order.id}`, {}, { $actor: { id: "operator-2" } })
  ]);
  assert.equal([first, second].filter((result) => result.claimed).length, 1);
  assert.equal([first, second].filter((result) => !result.claimed).length, 1);

  const owner = first.claimed ? first : second;
  const challenger = first.claimed ? second : first;
  await db(`release:orders/${order.id}`, {}, {
    $actor: owner.owner,
    $lease: owner.token
  });
  const next = await db(`claim:orders/${order.id}`, {}, {
    $actor: { id: "operator-3" }
  });

  assert.deepEqual(
    await db(`release:orders/${order.id}`, {}, {
      $actor: challenger.lease.owner,
      $lease: owner.token
    }),
    { released: false, reason: "lease-changed" }
  );
  assert.equal((await db(`lease:orders/${order.id}`)).owner.id, "operator-3");
  assert.equal(next.generation, owner.generation + 1);
});

test("expired leases do not block tokenless writes but reject stale tokens", async () => {
  await db("make:orders", { status: String });
  const order = await db("post:orders", { status: "pending" });
  const actor = { id: "operator-1" };
  const lease = await db(`claim:orders/${order.id}`, {}, {
    $actor: actor,
    $ttl: 1000
  });

  await new Promise((resolve) => setTimeout(resolve, 1100));
  assert.equal((await db(`lease:orders/${order.id}`)).held, false);

  await assert.rejects(
    db(`patch:orders/${order.id}`, { status: "stale" }, {
      $actor: actor,
      $lease: lease.token
    }),
    /kedaluwarsa atau token tidak valid/
  );
  await db(`patch:orders/${order.id}`, { status: "manual" });
  assert.equal((await db(`get:orders/${order.id}`)).status, "manual");
});

test("scoped lease commands cannot claim records outside the scope", async () => {
  await db("make:orders", { org: String, status: String });
  const visible = await db("post:orders", { org: "A", status: "pending" });
  const hidden = await db("post:orders", { org: "B", status: "pending" });
  await db(`claim:orders/${hidden.id}`, {}, { $actor: { id: "operator-2" } });
  const scoped = db("scope:orders", { org: "A" });

  await assert.rejects(
    scoped(`claim:orders/${hidden.id}`, {}, { $actor: { id: "operator-1" } }),
    /tidak ditemukan dalam scope/
  );
  await assert.rejects(
    scoped(`lease:orders/${hidden.id}`),
    /tidak ditemukan dalam scope/
  );
  await assert.rejects(
    scoped("batch:", [[`lease:orders/${hidden.id}`]]),
    /tidak ditemukan atau tidak aktif/
  );
  const claim = await scoped(`claim:orders/${visible.id}`, {}, {
    $actor: { id: "operator-1" }
  });
  assert.equal(claim.claimed, true);
});

test("bulk writes and impact previews are blocked by a lease", async () => {
  await db("make:orders", { status: String });
  const first = await db("post:orders", { status: "pending" });
  const second = await db("post:orders", { status: "pending" });
  const lease = await db(`claim:orders/${second.id}`, {}, {
    $actor: { id: "operator-1" }
  });

  await assert.rejects(
    db("patch:orders", {
      $where: { id: [first.id, second.id] },
      status: "approved"
    }),
    /sedang dipegang/
  );
  assert.deepEqual(
    (await db("get:orders", { $sort: "id:asc" })).map((row) => row.status),
    ["pending", "pending"]
  );

  const plan = await db("impact:", {
    command: `patch:orders/${second.id}`,
    payload: { status: "approved" }
  });
  assert.equal(plan.canApply, false);
  assert.equal(plan.blockedBy[0].type, "lease");

  await db(`release:orders/${second.id}`, {}, {
    $actor: { id: "operator-1" },
    $lease: lease.token
  });
});

test("no-op seed and conflict-ignore inserts are not blocked by a lease", async () => {
  await db("make:items", { name: String });
  const item = await db("post:items", { name: "original" });
  const lease = await db(`claim:items/${item.id}`, {}, {
    $actor: { id: "operator-1" }
  });

  const seed = await db("seed:items", [{ id: item.id, name: "ignored" }]);
  assert.equal(seed.skipped, 1);
  const ignored = await db("post:items", [{ id: item.id, name: "ignored" }], {
    $onConflict: "ignore"
  });
  assert.equal(ignored.inserted, 0);

  await assert.rejects(
    db("seed:items?sync=true", [{ id: item.id, name: "changed" }]),
    /sedang dipegang/
  );
  assert.equal((await db(`get:items/${item.id}`)).name, "original");

  await db(`release:items/${item.id}`, {}, {
    $actor: { id: "operator-1" },
    $lease: lease.token
  });
});

test("cascade deletion cannot remove a row held by an operator", async () => {
  await db("make:customers", { name: String });
  await db("make:orders", { customer: "@customers", status: String });
  const customer = await db("post:customers", { name: "Customer" });
  const order = await db("post:orders", { customer: customer.id, status: "pending" });
  const lease = await db(`claim:orders/${order.id}`, {}, {
    $actor: { id: "operator-1" }
  });

  await assert.rejects(
    db(`delete:customers/${customer.id}`, { $cascade: true }),
    /sedang dipegang/
  );
  assert.ok(await db(`get:customers/${customer.id}`));
  assert.ok(await db(`get:orders/${order.id}`));

  await db(`release:orders/${order.id}`, {}, {
    $actor: { id: "operator-1" },
    $lease: lease.token
  });
});
