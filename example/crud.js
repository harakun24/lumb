import db from "../src/index.js";

export default async function () {
  const data = await db("get:produk");

  console.log({ data })
}