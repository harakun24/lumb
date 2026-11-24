const dbCabang1 = db("scope:produk", { cabang: "CBG-01" });
const dbCabang2 = db("scope:produk", { cabang: "CBG-02" });

await dbCabang2("post:produk", {
  kode: "CB2001",
  nama: "Es Jeruk Cabang 2",
  harga: 8000,
  stok: 15,
  kategori: katMinuman.id
});

console.log("Cabang 1 lihat CB2001:", await dbCabang1("get:produk/CB2001")); // null
console.log("Cabang 2 lihat CB2001:", (await dbCabang2("get:produk/CB2001")).nama); // "Es Jeruk Cabang 2"

const totalAsetStok = await db(
  "raw:SELECT SUM(harga * stok) AS nilai_aset FROM produk WHERE dihapusPada IS NULL AND stok > ?",
  [0]
);
console.log("Total Nilai Aset Stok:", totalAsetStok[0].nilai_aset);

await db("close:");