import db from "../src/index.js";

export default async function () {
  // Ambil data kategori yang sudah di-seed/tersimpan sebelumnya
  const [katMinuman, katMakanan] = await db("get:kategori", { $sort: "id:asc" });
  const [tagDingin, tagFavorit] = await db("get:tag", { $sort: "id:asc" });

  if (!katMinuman || !katMakanan) {
    throw new Error('Data "Minuman" dan "Makanan" belum ada di tabel kategori. Jalankan seed:kategori terlebih dahulu.');
  }

  const akunDina = await db("post:akun", { username: "dina01" });
  const kasirDina = await db("post:kasir", { nama: "Dina", akun: akunDina });

  await db("post:produk", {
    kode: "MT0001",
    nama: "Matcha Latte",
    harga: 22000,
    stok: 15,
    kategori: katMinuman,
    tags: [tagDingin?.id, tagFavorit?.id].filter(Boolean)
  });

  const notaPertama = await db("post:transaksi", {
    kasir: kasirDina.id,
    total: 40000,
    items: [
      { produk: "KP0001", qty: 1, hargaSatuan: 18000 },
      { produk: "MT0001", qty: 1, hargaSatuan: 22000 }
    ]
  });
  console.log("Subtotal item 2:", notaPertama.items[1].subtotal);

  await db(
    "post:produk",
    [
      { kode: "PR0001", nama: "Roti Bakar", harga: 15000, stok: 12, kategori: katMakanan.id },
      { kode: "PR0002", nama: "Pisang Goreng", harga: 12000, stok: 25, kategori: katMakanan.id },
      { kode: "PR0003", nama: "Kentang Goreng", harga: 14000, stok: 4, kategori: katMakanan.id },
      { kode: "KP0001", nama: "Kopi Susu Aren", harga: 19000, stok: 20, kategori: katMinuman.id }
    ],
    {
      $chunk: 2,
      $onConflict: "update"
    }
  );

  const detailMatcha = await db("get:produk/MT0001");
  const stokEmpat = await db("get:produk?stok=4");

  const hasilFilter = await db("get:produk", {
    kategori: [katMinuman.id, katMakanan.id],
    nama: { includes: "Kopi" },
    $or: [{ stok: { lte: 5 } }, { harga: { gte: 20000 } }],
    $not: { kode: "PR0003" },
    $sort: "harga:desc",
    $fields: ["kode", "nama", "harga", "stok"]
  });

  const detailTransaksi = await db(`get:transaksi/${notaPertama.id}`, {
    $with: {
      kasir: { akun: true },
      items: {
        produk: { tags: true }
      }
    }
  });
  const daftarTagMatcha = await db("get:produk/MT0001/tags");

  const halaman1 = await db("get:produk", { $page: 1, $limit: 3, $sort: "kode:asc" });

  const logBerikutnya = await db("get:transaksi", { $cursor: 0, $limit: 10 });

  const rekapStok = await db("get:produk", {
    $group: "kategori",
    $count: "kode",
    $sum: "stok",
    $avg: "harga"
  });

  const rencanaSql = await db("explain:produk", { stok: { lte: 5 } });
  const strukturTabel = await db("inspect:produk");
  const cadanganKategori = await db("dump:kategori");

  await db("patch:produk/MT0001", {
    stok: { decrement: 11 },
    tags: tagFavorit ? [tagFavorit.id] : []
  });

  await db("patch:produk", {
    $where: { kategori: katMakanan.id },
    diskon: 1000
  });

  await db("patch:produk", [
    { kode: "PR0001", stok: { increment: 8 } },
    { kode: "PR0002", harga: 13000, stok: 30 }
  ]);

  await db("delete:produk/PR0003");

  console.log("Cek PR0003 aktif:", await db("get:produk/PR0003"));
  console.log("Jumlah arsip:", (await db("get:produk", { $trash: "only" })).length);
  console.log("Semua termasuk arsip:", (await db("get:produk", { $trash: "include" })).length);

  await db("delete:produk/PR0003", { $hard: true });

  await db("delete:produk", ["PR0001", "PR0002"]);

  try {
    await db(`delete:kategori/${katMinuman.id}`);
  } catch (err) {
    console.log("Proteksi aktif:", err.message);
  }

  await db(`delete:kategori/${katMakanan.id}`, { $cascade: true });

  console.log({
    akunDina,
    detailMatcha: detailMatcha?.kode,
    hasilFilterCount: hasilFilter.length,
    halaman1Total: halaman1.meta.total,
    rekapStokCount: rekapStok.length,
    rencanaSqlCount: rencanaSql.plan.length,
    strukturKolom: strukturTabel.columns.length,
    cadanganKategoriCount: cadanganKategori.length,
    logBerikutnyaCount: logBerikutnya.length,
    daftarTagMatchaCount: daftarTagMatcha.length,
    stokEmpatCount: stokEmpat.length,
    detailTransaksiId: detailTransaksi?.id
  });
}