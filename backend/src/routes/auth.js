const express = require("express");
const bcrypt = require("bcryptjs");
const jwt = require("jsonwebtoken");
const db = require("../db");
const { normalizeRole } = require("../middleware/roles");

const router = express.Router();

function buildFallbackEmail(username, mechanicNumber) {
  const safeUser = String(username || "user").replace(/[^a-zA-Z0-9._-]/g, "").toLowerCase() || "user";
  const safeMec = String(mechanicNumber || "sem-mec").replace(/[^a-zA-Z0-9._-]/g, "");
  return `${safeUser}.${safeMec}.${Date.now()}@no-email.local`;
}

router.post("/register", async (req, res) => {
  const { name, username, email, mechanicNumber, password, role } = req.body;
  const normalizedRole = normalizeRole(role || "driver");
  const acceptedRoles = ["driver", "supervisor", "admin", "viewer"];

  if (!name || !username || !password) {
    return res.status(400).json({ message: "Preencha nome, username e password." });
  }
  if (normalizedRole === "driver" && !mechanicNumber) {
    return res.status(400).json({ message: "Numero mecanografico obrigatorio para motorista." });
  }
  if (!acceptedRoles.includes(normalizedRole)) {
    return res.status(400).json({ message: "Perfil invalido." });
  }

  try {
    const hash = await bcrypt.hash(password, 10);
    const normalizedEmail = String(email || "").trim() || null;
    try {
      const result = await db.query(
        `INSERT INTO users (name, username, email, mechanic_number, role, password_hash)
         VALUES ($1, $2, $3, $4, $5, $6)
         RETURNING id, name, username, email, mechanic_number, role`,
        [name, username, normalizedEmail, mechanicNumber || null, normalizedRole, hash]
      );
      return res.status(201).json(result.rows[0]);
    } catch (innerError) {
      if (innerError?.code === "23502" && innerError?.column === "email" && !normalizedEmail) {
        const fallbackEmail = buildFallbackEmail(username, mechanicNumber);
        const fallbackResult = await db.query(
          `INSERT INTO users (name, username, email, mechanic_number, role, password_hash)
           VALUES ($1, $2, $3, $4, $5, $6)
           RETURNING id, name, username, email, mechanic_number, role`,
          [name, username, fallbackEmail, mechanicNumber || null, normalizedRole, hash]
        );
        return res.status(201).json(fallbackResult.rows[0]);
      }
      throw innerError;
    }
  } catch (error) {
    if (error.code === "23505") {
      return res.status(409).json({ message: "Username, email ou numero mecanografico ja existe." });
    }
    return res.status(500).json({ message: "Erro ao registar utilizador." });
  }
});

/** Mesma lógica que no supervisor: só dígitos para comparar n.º mecanográfico. */
function mechanicDigits(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits || null;
}

router.post("/login", async (req, res) => {
  const { username, password } = req.body;

  if (!username || !password) {
    return res.status(400).json({ message: "Username e password obrigatorios." });
  }

  try {
    const normalizedUsername = String(username).trim();
    let result = await db.query(
      `SELECT id, name, username, email, mechanic_number, role, is_active, password_hash
       FROM users
       WHERE LOWER(TRIM(username)) = LOWER($1)`,
      [normalizedUsername]
    );

    if (result.rowCount === 0) {
      const mecDigits = mechanicDigits(normalizedUsername);
      if (mecDigits) {
        result = await db.query(
          `SELECT id, name, username, email, mechanic_number, role, is_active, password_hash
           FROM users
           WHERE mechanic_number IS NOT NULL
             AND NULLIF(regexp_replace(TRIM(mechanic_number), '\\D', '', 'g'), '') IS NOT NULL
             AND (NULLIF(regexp_replace(TRIM(mechanic_number), '\\D', '', 'g'), ''))::bigint = $1::bigint`,
          [mecDigits]
        );
      }
    }

    if (result.rowCount === 0) {
      return res.status(401).json({ message: "Credenciais invalidas." });
    }

    const user = result.rows[0];
    const normalizedUserRole = normalizeRole(user.role);
    if (!user.is_active) {
      return res.status(403).json({ message: "Utilizador desativado. Contacte o supervisor." });
    }
    const passwordOk = await bcrypt.compare(password, user.password_hash);

    if (!passwordOk) {
      return res.status(401).json({ message: "Credenciais invalidas." });
    }

    const token = jwt.sign(
      {
        id: user.id,
        name: user.name,
        username: user.username,
        email: user.email,
        mechanicNumber: user.mechanic_number,
        role: normalizedUserRole,
      },
      process.env.JWT_SECRET,
      { expiresIn: "12h" }
    );

    return res.json({
      token,
      user: {
        id: user.id,
        name: user.name,
        username: user.username,
        email: user.email,
        mechanicNumber: user.mechanic_number,
        role: normalizedUserRole,
      },
    });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao fazer login." });
  }
});

module.exports = router;
