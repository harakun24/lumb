import db from "../src/index.js";
export default async function () {

  await db("make:produk", {
    kode: { type: String, length: 6, key: true }, // Primary Key, tepat 6 karakter
    nama: { type: String, min: 3, max: 100 },     // Wajib isi, 3-100 karakter
    harga: { type: Number, default: 0, min: 0 },  // Default 0, tidak boleh negatif
    stok: 0,                                      // Default 0 (INTEGER)
    cabang: "CBG-01",                             // Default "CBG-01"
    kategori: "@kategori",                        // Relasi N:1 (tertunda hingga make:kategori)
    tags: ["@tag"],                               // Relasi N:M (otomatis buat tabel pivot)
    dibuatPada: () => new Date(),                 // Otomatis diisi saat post:
    diubahPada: (item) => new Date(),             // Otomatis diperbarui saat post: & patch:
    dihapusPada: [Date, null],                    // Nullable
    remove() {
      this.dihapusPada = new Date();              // Aktifkan soft-delete
    },
    $index: [["cabang", "kategori"], ["stok"]]    // Indeks gabungan & tunggal
  });

  await db("make:kategori", {
    nama: new Set([String]), // Unik (UNIQUE), bukan primary key
    dihapusPada: [Date, null],
    remove() {
      this.dihapusPada = new Date();
    }
  });

  await db("make:tag", {
    nama: new Set([String])
  });

  await db("make:akun", {
    username: new Set([String]),
    peran: "kasir"
  });

  await db("make:kasir", {
    nama: String,
    akun: new Set(["@akun"]) // Set pada "@akun" membentuk relasi 1:1
  });

  await db("make:transaksi", {
    kasir: "@kasir",
    total: 0,
    waktu: () => new Date(),
    items: [
      {
        produk: "@produk",
        qty: 1,
        hargaSatuan: 0,
        get subtotal() {
          return this.qty * this.hargaSatuan;
        }
      }
    ]
  });

  await db("make:produk", {
    kode: { type: String, length: 6, key: true },
    nama: { type: String, min: 3, max: 100 },
    harga: { type: Number, default: 0, min: 0 },
    diskon: 0, // Kolom baru -> otomatis ALTER TABLE ADD COLUMN
    stok: 0,
    cabang: "CBG-01",
    kategori: "@kategori",
    tags: ["@tag"],
    dibuatPada: () => new Date(),
    diubahPada: (item) => new Date(),
    dihapusPada: [Date, null],
    remove() {
      this.dihapusPada = new Date();
    }
  });
}
