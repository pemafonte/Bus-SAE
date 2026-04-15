const bcrypt = require("bcryptjs");
const db = require("../db");

const DEFAULT_SUPERVISOR_USERNAME = "pedro.fonte";
const DEFAULT_SUPERVISOR_PASSWORD = "6489";
const DEFAULT_SUPERVISOR_NAME = "Administrador do Sistema";

function getSystemSupervisorConfig() {
  return {
    username: String(process.env.SYSTEM_SUPERVISOR_USERNAME || DEFAULT_SUPERVISOR_USERNAME).trim(),
    password: String(process.env.SYSTEM_SUPERVISOR_PASSWORD || DEFAULT_SUPERVISOR_PASSWORD),
    name: String(process.env.SYSTEM_SUPERVISOR_NAME || DEFAULT_SUPERVISOR_NAME).trim(),
  };
}

async function ensureSystemSupervisor() {
  const config = getSystemSupervisorConfig();
  if (!config.username || !config.password) {
    console.warn("[bootstrap] Supervisor fixo nao configurado: username/password em falta.");
    return;
  }

  const passwordHash = await bcrypt.hash(config.password, 10);
  const fallbackEmail = `${config.username.replace(/[^a-zA-Z0-9._-]/g, "").toLowerCase() || "supervisor"}@system.local`;

  await db.query(
    `INSERT INTO users (name, username, email, role, is_active, password_hash)
     VALUES ($1, $2, $3, 'supervisor', TRUE, $4)
     ON CONFLICT (username)
     DO UPDATE SET
       name = EXCLUDED.name,
       email = EXCLUDED.email,
       role = 'supervisor',
       is_active = TRUE,
       password_hash = EXCLUDED.password_hash`,
    [config.name, config.username, fallbackEmail, passwordHash]
  );

  console.log(`[bootstrap] Supervisor fixo garantido: ${config.username}`);
}

module.exports = { ensureSystemSupervisor };
