const express = require("express");
const db = require("../db");
const authMiddleware = require("../middleware/auth");
const { requireRoles } = require("../middleware/roles");
const { OVERVIEW_TODAY_SQL } = require("../utils/overviewToday");
const { serviceActivityLisbonDayFilter } = require("../utils/serviceListFilters");

const router = express.Router();
router.use(authMiddleware);
router.use(requireRoles("viewer", "supervisor", "admin"));

let deadheadOverviewTableEnsured = false;
async function ensureDeadheadTableForOverview() {
  if (deadheadOverviewTableEnsured) return;
  await db.query(
    `CREATE TABLE IF NOT EXISTS deadhead_movements (
      id BIGSERIAL PRIMARY KEY,
      imei VARCHAR(40) NOT NULL,
      fleet_number VARCHAR(50),
      plate_number VARCHAR(50),
      started_at TIMESTAMPTZ NOT NULL,
      ended_at TIMESTAMPTZ NOT NULL,
      start_lat NUMERIC(10,7),
      start_lng NUMERIC(10,7),
      end_lat NUMERIC(10,7),
      end_lng NUMERIC(10,7),
      total_km NUMERIC(12,3) NOT NULL DEFAULT 0,
      points_count INT NOT NULL DEFAULT 0,
      open_state BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  deadheadOverviewTableEnsured = true;
}

function emptyOverviewPayload() {
  return {
    report_date: null,
    total_services: 0,
    completed_services: 0,
    in_progress_services: 0,
    total_km: 0,
    avg_km: 0,
    planned_roster_count: 0,
    realized_roster_slots: 0,
    not_realized_count: 0,
    deadhead_km: 0,
    total_km_with_deadhead: 0,
    estimated_planned_km_today: 0,
    km_not_realized_estimate: 0,
    degraded: true,
  };
}

function csvEscape(value) {
  const raw = value == null ? "" : String(value);
  const escaped = raw.replace(/"/g, '""');
  return `"${escaped}"`;
}

router.get("/overview", async (_req, res) => {
  try {
    await ensureDeadheadTableForOverview();
    const result = await db.query(OVERVIEW_TODAY_SQL);
    return res.json(result.rows[0] || emptyOverviewPayload());
  } catch (_error) {
    try {
      await ensureDeadheadTableForOverview();
      const retry = await db.query(OVERVIEW_TODAY_SQL);
      return res.json(retry.rows[0] || emptyOverviewPayload());
    } catch (_retryError) {
      return res.json(emptyOverviewPayload());
    }
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
