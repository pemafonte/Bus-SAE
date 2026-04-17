#!/usr/bin/env node
/**
 * Aplica database/schema.sql de forma idempotente (CREATE/ALTER IF NOT EXISTS).
 * Não apaga tabelas nem linhas: motoristas, escalas e histórico de serviços mantêm-se.
 *
 * Uso: na pasta backend, com DATABASE_URL no .env
 *   npm run db:apply
 */
const fs = require("fs");
const path = require("path");
require("dotenv").config({ path: path.join(__dirname, "..", ".env") });
const { Pool } = require("pg");

const schemaPath = path.join(__dirname, "..", "..", "database", "schema.sql");

async function main() {
  const url = process.env.DATABASE_URL;
  if (!url || !String(url).trim()) {
    console.error("DATABASE_URL não definido. Copie backend/.env.example para backend/.env.");
    process.exit(1);
  }
  if (!fs.existsSync(schemaPath)) {
    console.error("Ficheiro não encontrado:", schemaPath);
    process.exit(1);
  }

  const sql = fs.readFileSync(schemaPath, "utf8");
  const pool = new Pool({ connectionString: url });

  try {
    await pool.query(sql);
  } catch (err) {
    console.error(
      "Falha ao aplicar schema com uma única query. Se o erro falar de várias instruções, use o cliente psql:\n" +
        `  psql "%DATABASE_URL%" -v ON_ERROR_STOP=1 -f database/schema.sql\n` +
        "Ou (Docker): docker compose exec -T postgres psql -U postgres -d bus_platform -v ON_ERROR_STOP=1 < database/schema.sql"
    );
    console.error(err);
    process.exit(1);
  } finally {
    await pool.end();
  }

  console.log("Schema aplicado com sucesso:", schemaPath);
}

main().catch((e) => {
  console.error(e);
  process.exit(1);
});
