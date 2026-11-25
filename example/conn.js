import db from "../src/index.js";

export default async function () {

  await db("connect:main", { url: "sqlite:example.db" });
  // await db("connect:main", { url: "mysql://kasir:123@localhost:3306/kasir" });
  // await db("connect:main", { url: "postgres://postgres:postgres@localhost:5432/kasir" });
  // await db("connect:arsip", { url: ":memory:" });
  // PostgreSQL: { url: "postgres://user:pass@localhost:5432/kasir" }
  // MySQL:      { url: "mysql://user:pass@localhost:3306/kasir" }

  db.on("get:produk", (produk) => {
    if (produk) {
      console.log(`[STOCK WARNING] ${produk.nama} - ${produk.stok}`);
    }
  });

  db.on("query", ({ action, durationMs }) => {
    if (durationMs > 200) {
      console.warn(`[SLOW QUERY] ${action} time: ${durationMs.toFixed(2)}ms`);
    }
  });
}