const tx = await db("begin:", { $timeout: 5000 });

try {
  const kopi = await db("get:produk/KP0001", { $lock: true, $tx: tx });

  if (!kopi || kopi.stok < 2) {
    await db(`rollback:${tx}`);
    throw new Error("Stok kopi tidak mencukupi");
  }

  await db("patch:produk/KP0001", {
    stok: { decrement: 2 },
    $tx: tx
  });

  const sp = await db(`begin:${tx}`);
  try {
    await db("patch:produk/KP0001", { harga: -5000, $tx: sp });
    await db(`commit:${sp}`);
  } catch {
    await db(`rollback:${sp}`);
  }

  await db("post:transaksi", {
    kasir: kasirDina.id,
    total: kopi.harga * 2,
    items: [{ produk: "KP0001", qty: 2, hargaSatuan: kopi.harga }],
    $tx: tx
  });

  await db(`commit:${tx}`);
} catch (err) {
  await db(`rollback:${tx}`);
  throw err;
}

await db("batch:", [
  ["patch:produk/TH0001", { stok: { decrement: 1 } }],
  ["post:transaksi", {
    kasir: kasirDina.id,
    total: 5000,
    items: [{ produk: "TH0001", qty: 1, hargaSatuan: 5000 }]
  }]
]);