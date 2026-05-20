import { drizzle } from "drizzle-orm/postgres-js";
import { migrate } from "drizzle-orm/postgres-js/migrator";
import { dirname, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import postgres from "postgres";

const databaseUrl = process.env["DATABASE_URL"];
if (!databaseUrl) {
  throw new Error("DATABASE_URL ausente");
}

const currentDir = dirname(fileURLToPath(import.meta.url));
const migrationsFolder = resolve(currentDir, "../../drizzle");
const sql = postgres(databaseUrl, { max: 1 });
const db = drizzle(sql);

try {
  console.log(`Aplicando migrations em ${migrationsFolder}`);
  await migrate(db, { migrationsFolder });
  console.log("Migrations aplicadas");
} finally {
  await sql.end();
}
