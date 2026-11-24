import fs from "node:fs";
import path from "node:path";

export function generateTypeDefinitions(schema, outputFile = ".lumb.d.ts") {
  if (process.env.NODE_ENV === "production") return;

  const lines = [
    "// automatic when make: with $typegen: true.",
    "export interface LumbTables {"
  ];

  for (const [tableName, tableDef] of schema.tables.entries()) {
    if (tableDef.isJunction) continue;
    lines.push(`  ${tableName}: {`);
    for (const [colName, colDef] of tableDef.columns.entries()) {
      const tsType = mapColumnToTsType(colDef);
      const opt = colDef.nullable || colDef.hasDefault || colDef.isAutoIncrement ? "?" : "";
      lines.push(`    ${colName}${opt}: ${tsType};`);
    }
    lines.push("  };");
  }

  lines.push("}");
  lines.push("");

  try {
    const targetPath = path.resolve(process.cwd(), outputFile);
    fs.writeFileSync(targetPath, lines.join("\n"), "utf8");
  } catch {
    // Abaikan jika sistem berkas tidak dapat ditulisi
  }
}

function mapColumnToTsType(colDef) {
  let base = "unknown";
  switch (colDef.jsType) {
    case "String":
      base = "string";
      break;
    case "Number":
      base = "number";
      break;
    case "Boolean":
      base = "boolean";
      break;
    case "Date":
      base = "Date | string";
      break;
    default:
      base = "string | number";
  }
  return colDef.nullable ? `${base} | null` : base;
}