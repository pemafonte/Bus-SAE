const express = require("express");
const db = require("../db");
const authMiddleware = require("../middleware/auth");
const { requireRoles } = require("../middleware/roles");
const { OVERVIEW_TODAY_SQL } = require("../utils/overviewToday");
const { serviceActivityLisbonDayFilter } = require("../utils/serviceListFilters");

const router = express.Router();
router.use(authMiddleware);
router.use(requireRoles("viewer", "supervisor", "admin"));

function csvEscape(value) {
  const raw = value == null ? "" : String(value);
  const escaped = raw.replace(/"/g, '""');
  return `"${escaped}"`;
}

router.get("/overview", async (_req, res) => {
  try {
    const result = await db.query(OVERVIEW_TODAY_SQL);
    return res.json(result.rows[0]);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao obter resumo." });
  }
});

router.get("/services", async (req, res) => {
  const { driverId, lineCode, status, fromDate, toDate } = req.query;
  const where = [];
  const values = [];
  let i = 1;

  if (driverId) {
    where.push(`s.driver_id = $${i}`);
    values.push(driverId);
    i += 1;
  }
  if (lineCode) {
    where.push(`s.line_code = $${i}`);
    values.push(lineCode);
    i += 1;
  }
  if (status) {
    where.push(`s.status = $${i}`);
    values.push(status);
    i += 1;
  }
  const dayFilter = serviceActivityLisbonDayFilter(fromDate, toDate, i);
  if (dayFilter) {
    where.push(dayFilter.sql);
    dayFilter.values.forEach((v) => values.push(v));
    i = dayFilter.nextIndex;
  }

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  try {
    const result = await db.query(
      `SELECT
         s.id,
         s.driver_id,
         u.name AS driver_name,
         s.plate_number,
         s.service_schedule,
         s.line_code,
         s.fleet_number,
         s.status,
         s.started_at,
         s.ended_at,
         s.total_km
       FROM services s
       JOIN users u ON u.id = s.driver_id
       ${whereSql}
       ORDER BY s.started_at DESC`,
      values
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar servicos." });
  }
});

router.get("/services/export.csv", async (req, res) => {
  const { driverId, lineCode, status, fromDate, toDate } = req.query;
  const where = [];
  const values = [];
  let i = 1;

  if (driverId) {
    where.push(`s.driver_id = $${i}`);
    values.push(driverId);
    i += 1;
  }
  if (lineCode) {
    where.push(`s.line_code = $${i}`);
    values.push(lineCode);
    i += 1;
  }
  if (status) {
    where.push(`s.status = $${i}`);
    values.push(status);
    i += 1;
  }
  const dayFilter = serviceActivityLisbonDayFilter(fromDate, toDate, i);
  if (dayFilter) {
    where.push(dayFilter.sql);
    dayFilter.values.forEach((v) => values.push(v));
    i = dayFilter.nextIndex;
  }

  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
  try {
    const result = await db.query(
      `SELECT
         s.id,
         u.name AS driver_name,
         u.email AS driver_email,
         s.plate_number,
         s.service_schedule,
         s.line_code,
         s.fleet_number,
         s.status,
         s.started_at,
         s.ended_at,
         s.total_km
       FROM services s
       JOIN users u ON u.id = s.driver_id
       ${whereSql}
       ORDER BY s.started_at DESC`,
      values
    );

    const header = [
      "service_id",
      "motorista",
      "email",
      "chapa",
      "horario",
      "linha",
      "frota",
      "estado",
      "inicio",
      "fim",
      "kms",
    ];
    const rows = result.rows.map((r) =>
      [
        r.id,
        r.driver_name,
        r.driver_email,
        r.plate_number,
        r.service_schedule,
        r.line_code,
        r.fleet_number,
        r.status,
        r.started_at ? new Date(r.started_at).toISOString() : "",
        r.ended_at ? new Date(r.ended_at).toISOString() : "",
        r.total_km,
      ]
        .map(csvEscape)
        .join(",")
    );
    const csv = [header.map(csvEscape).join(","), ...rows].join("\n");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=resumo_servicos.csv");
    return res.status(200).send(csv);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar resumo." });
  }
});

module.exports = router;
