const db = require("../db");
const { calculatePathDistance } = require("../utils/distance");
const { parseServiceScheduleRangeMinutes } = require("../utils/serviceSchedule");
const { findBestTripForLine, getTripTerminusMinutes } = require("../utils/gtfsTripResolve");
const { getRosterServiceDateForExecution } = require("../utils/rosterServiceDate");
const { resolveTotalKmWithPlannedFallback } = require("../utils/plannedKmFallback");

const GRACE_AFTER_SCHEDULED_END_MS = 15 * 60 * 1000;

async function ensureDriverNotificationsTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS driver_notifications (
      id BIGSERIAL PRIMARY KEY,
      driver_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      title VARCHAR(200) NOT NULL,
      message TEXT NOT NULL,
      notification_type VARCHAR(40) NOT NULL DEFAULT 'roster_change',
      roster_id INT,
      read_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_driver_notifications_driver_created
     ON driver_notifications(driver_id, created_at DESC)`
  );
}

async function getActiveSegment(serviceId) {
  const segmentResult = await db.query(
    `SELECT id, driver_id, fleet_number, started_at
     FROM service_segments
     WHERE service_id = $1 AND status = 'in_progress'
     ORDER BY started_at DESC
     LIMIT 1`,
    [serviceId]
  );
  return segmentResult.rows[0] || null;
}

async function closeActiveSegment(serviceId, finalStatus = "completed") {
  const activeSegment = await getActiveSegment(serviceId);
  if (!activeSegment) return null;

  const pointsResult = await db.query(
    `SELECT lat, lng
     FROM service_points
     WHERE service_segment_id = $1
     ORDER BY captured_at ASC`,
    [activeSegment.id]
  );

  const points = pointsResult.rows.map((p) => ({
    lat: Number(p.lat),
    lng: Number(p.lng),
  }));
  const kmSegment = calculatePathDistance(points);

  await db.query(
    `UPDATE service_segments
     SET status = $2,
         ended_at = NOW(),
         km_segment = $3
     WHERE id = $1`,
    [activeSegment.id, finalStatus, kmSegment]
  );

  return { ...activeSegment, kmSegment };
}

/**
 * Prazo de encerramento: data do dia de escala + hora do terminus (última paragem GTFS),
 * com fallback ao fim do intervalo do horário textual quando não há GTFS.
 */
async function getAutoCloseDeadlineFromEscalacaoTerminus(row) {
  const { driver_id, planned_service_id, started_at, gtfs_trip_id, line_code, service_schedule } = row;

  let escalaDay = null;
  if (planned_service_id) {
    escalaDay = await getRosterServiceDateForExecution(driver_id, planned_service_id, started_at);
  }
  if (!escalaDay) {
    const { rows } = await db.query(
      `SELECT ($1::timestamptz AT TIME ZONE 'Europe/Lisbon')::date AS d`,
      [started_at]
    );
    escalaDay = rows[0]?.d;
  }
  if (!escalaDay) return null;

  let tripId = gtfs_trip_id || null;
  if (!tripId && line_code && service_schedule) {
    const trip = await findBestTripForLine(line_code, service_schedule);
    tripId = trip?.trip_id || null;
  }

  let endMinutes = await getTripTerminusMinutes(tripId);
  if (endMinutes == null) {
    const range = parseServiceScheduleRangeMinutes(service_schedule);
    endMinutes = range?.end ?? null;
  }
  if (endMinutes == null) return null;

  const { rows } = await db.query(
    `SELECT (($1::date::timestamp AT TIME ZONE 'Europe/Lisbon') + ($2::int * interval '1 minute')) AS end_ts`,
    [escalaDay, endMinutes]
  );
  const t = rows[0]?.end_ts;
  return t ? new Date(t) : null;
}

async function autoCloseOneService(row) {
  const serviceId = row.id;
  const scheduledEnd = await getAutoCloseDeadlineFromEscalacaoTerminus(row);
  if (!scheduledEnd) return false;

  const deadline = scheduledEnd.getTime() + GRACE_AFTER_SCHEDULED_END_MS;
  if (Date.now() < deadline) return false;

  const fresh = await db.query(
    `SELECT id, driver_id, planned_service_id, started_at, service_schedule, status,
            gtfs_trip_id, line_code
     FROM services WHERE id = $1 AND status = 'in_progress'`,
    [serviceId]
  );
  if (!fresh.rowCount) return false;
  const se2 = await getAutoCloseDeadlineFromEscalacaoTerminus(fresh.rows[0]);
  if (!se2 || Date.now() < se2.getTime() + GRACE_AFTER_SCHEDULED_END_MS) return false;

  await closeActiveSegment(serviceId);

  const pointsResult = await db.query(
    `SELECT lat, lng, captured_at
     FROM service_points
     WHERE service_id = $1
     ORDER BY captured_at ASC`,
    [serviceId]
  );

  const points = pointsResult.rows.map((p) => ({
    lat: Number(p.lat),
    lng: Number(p.lng),
    capturedAt: p.captured_at,
  }));

  const totalKmGps = calculatePathDistance(points);
  const plannedForKm = fresh.rows[0]?.planned_service_id ?? row.planned_service_id;
  const totalKm = await resolveTotalKmWithPlannedFallback(totalKmGps, plannedForKm);
  const routeGeoJSON = {
    type: "Feature",
    geometry: {
      type: "LineString",
      coordinates: points.map((p) => [p.lng, p.lat]),
    },
    properties: {
      points: points.length,
    },
  };

  const svcForRoster = await db.query(
    `SELECT driver_id, planned_service_id, started_at FROM services WHERE id = $1`,
    [serviceId]
  );
  const s0 = svcForRoster.rows[0];
  let rosterDate = null;
  if (s0?.planned_service_id) {
    rosterDate = await getRosterServiceDateForExecution(s0.driver_id, s0.planned_service_id, s0.started_at);
  }
  if (!rosterDate && s0?.started_at) {
    const rosterDateRes = await db.query(
      `SELECT ($1::timestamptz AT TIME ZONE 'Europe/Lisbon')::date AS d`,
      [s0.started_at]
    );
    rosterDate = rosterDateRes.rows[0]?.d;
  }

  const updated = await db.query(
    `UPDATE services
     SET status = 'completed',
         ended_at = NOW(),
         total_km = $2,
         route_geojson = $3
     WHERE id = $1 AND status = 'in_progress'
     RETURNING id, driver_id, planned_service_id, service_schedule, line_code`,
    [serviceId, totalKm, JSON.stringify(routeGeoJSON)]
  );

  if (!updated.rowCount) {
    return false;
  }

  const u = updated.rows[0];

  if (u.planned_service_id && rosterDate) {
    await db.query(
      `UPDATE daily_roster
       SET status = 'completed'
       WHERE driver_id = $1
         AND planned_service_id = $2
         AND service_date = $3`,
      [u.driver_id, u.planned_service_id, rosterDate]
    );
  }

  await ensureDriverNotificationsTable();
  const lineLabel = u.line_code ? `Linha ${u.line_code}` : "Serviço";
  await db.query(
    `INSERT INTO driver_notifications (driver_id, title, message, notification_type)
     VALUES ($1, $2, $3, 'system_auto_complete')`,
    [
      u.driver_id,
      "Encerramento automático",
      `${lineLabel} (${u.service_schedule || "-"}): a viagem foi concluída pelo sistema por não ter sido finalizada até 15 minutos após o horário do último ponto do roteiro, no dia de escala.`,
    ]
  );

  return true;
}

async function runAutoCloseStaleServicesOnce() {
  const result = await db.query(
    `SELECT id, driver_id, planned_service_id, service_schedule, started_at, status,
            gtfs_trip_id, line_code
     FROM services
     WHERE status = 'in_progress'`
  );
  for (const row of result.rows) {
    try {
      await autoCloseOneService(row);
    } catch (e) {
      console.error("[autoCloseStaleServices] falha ao processar servico", row.id, e);
    }
  }
}

function startAutoCloseStaleServicesLoop(intervalMs = 60_000) {
  if (String(process.env.DISABLE_AUTO_CLOSE_STALE_SERVICES || "").trim() === "1") {
    console.log("[autoCloseStaleServices] desativado (DISABLE_AUTO_CLOSE_STALE_SERVICES=1).");
    return () => {};
  }
  const ms = Number(process.env.AUTO_CLOSE_STALE_SERVICES_INTERVAL_MS || intervalMs);
  const tick = () => {
    runAutoCloseStaleServicesOnce().catch((e) => {
      console.error("[autoCloseStaleServices] erro no ciclo.", e);
    });
  };
  tick();
  const id = setInterval(tick, Number.isFinite(ms) && ms >= 10_000 ? ms : intervalMs);
  return () => clearInterval(id);
}

module.exports = {
  runAutoCloseStaleServicesOnce,
  startAutoCloseStaleServicesLoop,
  getAutoCloseDeadlineFromEscalacaoTerminus,
};
