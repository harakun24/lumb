console.log("hi")
import connect from "./conn.js";
import tables from "./tables.js";
import seeder from "./seed.js";

import crud from "./crud.js";

await connect();
await tables();
await seeder();
await crud();


