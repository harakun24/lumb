import db from "../src/index.js";
export default async function () {

  await db("seed:kategori", [
    { nama: "Minuman" },
    { nama: "Makanan" }
  ]);

  await db("seed:tag", [
    { nama: "Dingin" },
    { nama: "Favorit" },
    { nama: "Promo" }
  ]);

  const [katMinuman, katMakanan] = await db("get:kategori", { $sort: "id:asc" });

  // 2. Seed dengan ?sync=true: menyisipkan data baru sekaligus memperbarui nilai jika sudah ada
  await db("seed:produk?sync=true", [
    { kode: "KP0001", nama: "Kopi Susu", harga: 18000, stok: 20, kategori: katMinuman.id },
    { kode: "TH0001", nama: "Es Teh Manis", harga: 5000, stok: 30, kategori: katMinuman.id }
  ]);

}
