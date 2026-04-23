const express = require("express");
const db = require("../db");
const authMiddleware = require("../middleware/auth");
const { calculatePathDistance, minDistanceToPolylineMeters } = require("../utils/distance");
const { findBestTripForLine, getShapePointsByTripId, getStopsByTripId } = require("../utils/gtfsTripResolve");
const { parseServiceScheduleRangeMinutes, scheduleRangesOverlap } = require("../utils/serviceSchedule");
const { resolveTotalKmWithPlannedFallback } = require("../utils/plannedKmFallback");
const { getRosterServiceDateForExecution } = require("../utils/rosterServiceDate");

const router = express.Router();
router.use(authMiddleware);
let servicePointsQualityColumnsEnsured = false;
let serviceStopProgressTableEnsured = false;
let serviceVehicleOdometerColumnsEnsured = false;

async function ensurePlannedServiceLocationColumns() {
  await db.query(
    `ALTER TABLE planned_services
       ADD COLUMN IF NOT EXISTS start_location VARCHAR(120)`
  );
  await db.query(
    `ALTER TABLE planned_services
       ADD COLUMN IF NOT EXISTS end_location VARCHAR(120)`
  );
  await db.query(
    `ALTER TABLE planned_services
       ADD COLUMN IF NOT EXISTS kms_carga NUMERIC(12,3)`
  );
}

async function ensureServicePointsQualityColumns() {
  if (servicePointsQualityColumnsEnsured) return;
  await db.query(
    `ALTER TABLE service_points
       ADD COLUMN IF NOT EXISTS accuracy_m NUMERIC(8,2)`
  );
  await db.query(
    `ALTER TABLE service_points
       ADD COLUMN IF NOT EXISTS speed_kmh NUMERIC(8,2)`
  );
  await db.query(
    `ALTER TABLE service_points
       ADD COLUMN IF NOT EXISTS heading_deg NUMERIC(6,2)`
  );
  await db.query(
    `ALTER TABLE service_points
       ADD COLUMN IF NOT EXISTS point_source VARCHAR(20) NOT NULL DEFAULT 'mobile'`
  );
  servicePointsQualityColumnsEnsured = true;
}

async function ensureServiceStopProgressTable() {
  if (serviceStopProgressTableEnsured) return;
  await db.query(
    `CREATE TABLE IF NOT EXISTS service_stop_progress (
      service_id BIGINT PRIMARY KEY REFERENCES services(id) ON DELETE CASCADE,
      last_passed_stop_id VARCHAR(120),
      last_passed_stop_sequence INT,
      last_passed_at TIMESTAMPTZ,
      last_announced_stop_id VARCHAR(120),
      last_announced_stop_sequence INT,
      last_announcement_type VARCHAR(20),
      last_announced_at TIMESTAMPTZ,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  serviceStopProgressTableEnsured = true;
}

async function ensureServiceVehicleOdometerColumns() {
  if (serviceVehicleOdometerColumnsEnsured) return;
  await db.query(
    `ALTER TABLE services
       ADD COLUMN IF NOT EXISTS vehicle_odometer_start_km NUMERIC(12,1)`
  );
  await db.query(
    `ALTER TABLE services
       ADD COLUMN IF NOT EXISTS vehicle_odometer_end_km NUMERIC(12,1)`
  );
  await db.query(
    `ALTER TABLE services
       ADD COLUMN IF NOT EXISTS vehicle_odometer_delta_km NUMERIC(12,3)`
  );
  await db.query(
    `ALTER TABLE services
       ADD COLUMN IF NOT EXISTS vehicle_odometer_vs_gps_diff_km NUMERIC(12,3)`
  );
  await db.query(
    `ALTER TABLE services
       ADD COLUMN IF NOT EXISTS close_mode VARCHAR(30)`
  );
  await db.query(
    `CREATE TABLE IF NOT EXISTS tracker_devices (
      id BIGSERIAL PRIMARY KEY,
      imei VARCHAR(40) UNIQUE NOT NULL,
      fleet_number VARCHAR(50),
      plate_number VARCHAR(50),
      provider VARCHAR(40) NOT NULL DEFAULT 'teltonika',
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      install_odometer_km NUMERIC(12,1),
      current_odometer_km NUMERIC(12,1),
      current_odometer_updated_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE TABLE IF NOT EXISTS vehicle_odometer_logs (
      id BIGSERIAL PRIMARY KEY,
      imei VARCHAR(40),
      fleet_number VARCHAR(50),
      plate_number VARCHAR(50),
      odometer_km NUMERIC(12,1) NOT NULL,
      captured_at TIMESTAMPTZ NOT NULL,
      source VARCHAR(30) NOT NULL DEFAULT 'manual',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  serviceVehicleOdometerColumnsEnsured = true;
}

function geoDistanceMeters(aLat, aLng, bLat, bLng) {
  const toRad = (deg) => (Number(deg) * Math.PI) / 180;
  const R = 6371000;
  const lat1 = Number(aLat);
  const lng1 = Number(aLng);
  const lat2 = Number(bLat);
  const lng2 = Number(bLng);
  if (![lat1, lng1, lat2, lng2].every(Number.isFinite)) return null;
  const dLat = toRad(lat2 - lat1);
  const dLng = toRad(lng2 - lng1);
  const p1 = toRad(lat1);
  const p2 = toRad(lat2);
  const h =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(p1) * Math.cos(p2) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  const c = 2 * Math.atan2(Math.sqrt(h), Math.sqrt(1 - h));
  return R * c;
}

function normalizeStopSequence(value) {
  const n = Number(value);
  if (!Number.isFinite(n)) return null;
  return Math.trunc(n);
}

function canReadServiceByRole(reqUser, serviceDriverId) {
  const role = String(reqUser?.role || "")
    .trim()
    .toLowerCase();
  if (role === "admin" || role === "supervisor") return true;
  return Number(reqUser?.id) === Number(serviceDriverId);
}

function normalizeIncomingPoint(rawPoint) {
  const lat = Number(rawPoint?.lat);
  const lng = Number(rawPoint?.lng);
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return { ok: false, error: "Latitude e longitude invalidas." };
  }

  const normalizedSource = String(rawPoint?.source || "mobile")
    .trim()
    .toLowerCase();
  if (!["mobile", "tracker"].includes(normalizedSource)) {
    return { ok: false, error: "Origem de ponto invalida. Use mobile ou tracker." };
  }

  const normalizedAccuracyM = Number(rawPoint?.accuracyM);
  const normalizedSpeedKmh = Number(rawPoint?.speedKmh);
  const normalizedHeadingDeg = Number(rawPoint?.headingDeg);
  const accuracyValue = Number.isFinite(normalizedAccuracyM) ? normalizedAccuracyM : null;
  const speedValue = Number.isFinite(normalizedSpeedKmh) ? normalizedSpeedKmh : null;
  const headingValue = Number.isFinite(normalizedHeadingDeg) ? normalizedHeadingDeg : null;

  if (accuracyValue !== null && accuracyValue < 0) {
    return { ok: false, error: "accuracyM invalido." };
  }
  if (speedValue !== null && speedValue < 0) {
    return { ok: false, error: "speedKmh invalido." };
  }
  if (headingValue !== null && (headingValue < 0 || headingValue > 360)) {
    return { ok: false, error: "headingDeg invalido. Use 0..360." };
  }

  return {
    ok: true,
    point: {
      lat,
      lng,
      capturedAt: rawPoint?.capturedAt || null,
      source: normalizedSource,
      accuracyM: accuracyValue,
      speedKmh: speedValue,
      headingDeg: headingValue,
    },
  };
}

async function insertServicePoint(serviceId, serviceSegmentId, point) {
  await db.query(
    `INSERT INTO service_points (
       service_id, service_segment_id, lat, lng, captured_at,
       accuracy_m, speed_kmh, heading_deg, point_source
     )
     VALUES ($1, $2, $3, $4, COALESCE($5::timestamptz, NOW()), $6, $7, $8, $9)`,
    [
      serviceId,
      serviceSegmentId,
      point.lat,
      point.lng,
      point.capturedAt || null,
      point.accuracyM,
      point.speedKmh,
      point.headingDeg,
      point.source,
    ]
  );
}

async function updateServiceRouteCheck(serviceId, gtfsTripId, point) {
  let deviationMeters = null;
  let isOffRoute = false;
  if (gtfsTripId) {
    const shapePoints = await getShapePointsByTripId(gtfsTripId);
    if (shapePoints.length >= 2) {
      deviationMeters = minDistanceToPolylineMeters({ lat: point.lat, lng: point.lng }, shapePoints);
      isOffRoute = deviationMeters > 150;
      await db.query(
        `UPDATE services
         SET route_deviation_m = $2,
             is_off_route = $3
         WHERE id = $1`,
        [serviceId, deviationMeters, isOffRoute]
      );
    }
  }
  return { deviationMeters, isOffRoute };
}

async function resolveServiceTripId(service) {
  let tripId = service?.gtfs_trip_id || null;
  if (!tripId && service?.line_code && service?.service_schedule) {
    const bestTrip = await findBestTripForLine(service.line_code, service.service_schedule);
    tripId = bestTrip?.trip_id || null;
    if (tripId) {
      await db.query(`UPDATE services SET gtfs_trip_id = $1 WHERE id = $2 AND gtfs_trip_id IS NULL`, [tripId, service.id]);
    }
  }
  return tripId;
}

async function maybeAutoCompleteServiceAtLastStop(service, latestPoint, options = {}) {
  const serviceId = Number(service?.id);
  if (!Number.isFinite(serviceId) || service?.status !== "in_progress") return { autoClosed: false };
  await ensureServiceVehicleOdometerColumns();
  const arrivalMeters = Math.max(20, Math.min(180, Number(options.arrivalMeters) || 60));
  const tripId = await resolveServiceTripId(service);
  if (!tripId) return { autoClosed: false };
  const stops = (await getStopsByTripId(tripId))
    .map((s) => ({
      stop_id: s.stop_id,
      stop_sequence: normalizeStopSequence(s.stop_sequence),
      lat: Number(s.lat),
      lng: Number(s.lng),
    }))
    .filter((s) => s.stop_sequence != null && Number.isFinite(s.lat) && Number.isFinite(s.lng))
    .sort((a, b) => a.stop_sequence - b.stop_sequence);
  if (!stops.length) return { autoClosed: false };
  const lastStop = stops[stops.length - 1];
  const distanceToLast = geoDistanceMeters(latestPoint.lat, latestPoint.lng, lastStop.lat, lastStop.lng);
  if (distanceToLast == null || distanceToLast > arrivalMeters) return { autoClosed: false };

  await ensureServiceStopProgressTable();
  await db.query(
    `INSERT INTO service_stop_progress (service_id, last_passed_stop_id, last_passed_stop_sequence, last_passed_at, updated_at)
     VALUES ($1, $2, $3, NOW(), NOW())
     ON CONFLICT (service_id) DO UPDATE
       SET last_passed_stop_id = EXCLUDED.last_passed_stop_id,
           last_passed_stop_sequence = EXCLUDED.last_passed_stop_sequence,
           last_passed_at = EXCLUDED.last_passed_at,
           updated_at = NOW()`,
    [serviceId, lastStop.stop_id || null, lastStop.stop_sequence]
  );

  await closeActiveSegment(serviceId);
  const pointsResult = await db.query(
    `SELECT lat, lng, captured_at, accuracy_m, speed_kmh
     FROM service_points
     WHERE service_id = $1
     ORDER BY captured_at ASC`,
    [serviceId]
  );
  const points = pointsResult.rows.map((p) => ({
    lat: Number(p.lat),
    lng: Number(p.lng),
    capturedAt: p.captured_at,
    accuracyM: p.accuracy_m == null ? null : Number(p.accuracy_m),
    speedKmh: p.speed_kmh == null ? null : Number(p.speed_kmh),
  }));
  const totalKmGps = calculatePathDistance(points);
  const totalKm = await resolveTotalKmWithPlannedFallback(totalKmGps, service.planned_service_id);
  const routeGeoJSON = {
    type: "Feature",
    geometry: {
      type: "LineString",
      coordinates: points.map((p) => [p.lng, p.lat]),
    },
    properties: { points: points.length },
  };
  const updated = await db.query(
    `UPDATE services
     SET status = 'completed',
         ended_at = NOW(),
         total_km = $2,
         route_geojson = $3,
         close_mode = 'auto_last_stop'
     WHERE id = $1
       AND status = 'in_progress'
     RETURNING id, driver_id, planned_service_id, started_at`,
    [serviceId, totalKm, JSON.stringify(routeGeoJSON)]
  );
  if (!updated.rowCount) return { autoClosed: false };
  const closed = updated.rows[0];
  if (closed.planned_service_id) {
    let rosterDay = await getRosterServiceDateForExecution(closed.driver_id, closed.planned_service_id, closed.started_at);
    if (!rosterDay && closed.started_at) {
      const rosterDayRes = await db.query(
        `SELECT ($1::timestamptz AT TIME ZONE 'Europe/Lisbon')::date AS d`,
        [closed.started_at]
      );
      rosterDay = rosterDayRes.rows[0]?.d;
    }
    if (rosterDay) {
      await db.query(
        `UPDATE daily_roster
         SET status = 'completed'
         WHERE driver_id = $1
           AND planned_service_id = $2
           AND service_date = $3`,
        [closed.driver_id, closed.planned_service_id, rosterDay]
      );
    }
  }
  return { autoClosed: true, reason: "last_stop_reached" };
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
    `SELECT lat, lng, captured_at, accuracy_m, speed_kmh
     FROM service_points
     WHERE service_segment_id = $1
     ORDER BY captured_at ASC`,
    [activeSegment.id]
  );

  const points = pointsResult.rows.map((p) => ({
    lat: Number(p.lat),
    lng: Number(p.lng),
    capturedAt: p.captured_at,
    accuracyM: p.accuracy_m == null ? null : Number(p.accuracy_m),
    speedKmh: p.speed_kmh == null ? null : Number(p.speed_kmh),
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

router.get("/", async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, plate_number, service_schedule, line_code, fleet_number, status,
              started_at, ended_at, total_km, route_deviation_m, is_off_route
       FROM services
       WHERE driver_id = $1
       ORDER BY created_at DESC`,
      [req.user.id]
    );
    return res.json(result.rows);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao listar servicos." });
  }
});

router.get("/active", async (req, res) => {
  try {
    const result = await db.query(
      `SELECT id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number, status, started_at
       FROM services
       WHERE driver_id = $1
         AND status = 'in_progress'
       ORDER BY started_at DESC
       LIMIT 1`,
      [req.user.id]
    );
    if (!result.rowCount) {
      return res.json({ activeService: null });
    }
    return res.json({ activeService: result.rows[0] });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao obter servico ativo." });
  }
});

router.get("/:serviceId/handover-roster-overlap-check", async (req, res) => {
  const serviceId = Number(req.params.serviceId);
  const mechanicNumber = String(req.query.mechanicNumber || "").trim();
  if (!Number.isFinite(serviceId) || serviceId <= 0) {
    return res.status(400).json({ message: "Identificador de servico invalido." });
  }
  if (!mechanicNumber) {
    return res.status(400).json({ message: "Indique o numero mecanografico do motorista de destino." });
  }
  try {
    const svcRes = await db.query(
      `SELECT id, driver_id, service_schedule, line_code, planned_service_id, status
       FROM services
       WHERE id = $1 AND driver_id = $2`,
      [serviceId, req.user.id]
    );
    if (!svcRes.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    const svc = svcRes.rows[0];
    if (svc.status !== "in_progress") {
      return res.status(409).json({ message: "Só é possível verificar sobreposição com um serviço em curso." });
    }

    const toDriverRes = await db.query(
      `SELECT id, name
       FROM users
       WHERE TRIM(mechanic_number) = TRIM($1) AND LOWER(TRIM(role)) = 'driver'`,
      [mechanicNumber]
    );
    if (!toDriverRes.rowCount) {
      return res.status(404).json({ message: "Motorista destino nao encontrado.", conflicts: [] });
    }
    const toDriverId = toDriverRes.rows[0].id;
    if (toDriverId === req.user.id) {
      return res.json({ conflicts: [] });
    }

    const rangeCurrent = parseServiceScheduleRangeMinutes(svc.service_schedule);
    if (!rangeCurrent) {
      return res.json({ conflicts: [] });
    }

    const rosterRes = await db.query(
      `SELECT ps.service_code, ps.service_schedule, ps.line_code, dr.status AS roster_status, dr.planned_service_id
       FROM daily_roster dr
       JOIN planned_services ps ON ps.id = dr.planned_service_id
       WHERE dr.driver_id = $1
         AND dr.service_date = (SELECT (started_at::date) FROM services WHERE id = $2)
         AND LOWER(TRIM(dr.status::text)) IN ('pending', 'assigned', 'in_progress')
         AND (dr.planned_service_id IS DISTINCT FROM (SELECT planned_service_id FROM services WHERE id = $2))`,
      [toDriverId, serviceId]
    );

    const conflicts = [];
    for (const r of rosterRes.rows) {
      const other = parseServiceScheduleRangeMinutes(r.service_schedule);
      if (other && scheduleRangesOverlap(rangeCurrent, other)) {
        conflicts.push({
          service_code: r.service_code,
          service_schedule: r.service_schedule,
          line_code: r.line_code,
          roster_status: r.roster_status,
        });
      }
    }

    return res.json({ conflicts });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao verificar sobreposicao na escala." });
  }
});

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

async function ensureSupervisorConflictAlertsTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS supervisor_conflict_alerts (
      id BIGSERIAL PRIMARY KEY,
      driver_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      alert_type VARCHAR(50) NOT NULL DEFAULT 'roster_conflict',
      roster_id INT,
      planned_service_id INT,
      affected_driver_id INT REFERENCES users(id) ON DELETE SET NULL,
      affected_planned_service_id INT,
      service_schedule VARCHAR(80),
      line_code VARCHAR(40),
      conflict_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
      unassigned_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
      notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `ALTER TABLE supervisor_conflict_alerts
       ADD COLUMN IF NOT EXISTS alert_type VARCHAR(50) NOT NULL DEFAULT 'roster_conflict'`
  );
  await db.query(
    `ALTER TABLE supervisor_conflict_alerts
       ADD COLUMN IF NOT EXISTS affected_driver_id INT REFERENCES users(id) ON DELETE SET NULL`
  );
  await db.query(
    `ALTER TABLE supervisor_conflict_alerts
       ADD COLUMN IF NOT EXISTS affected_planned_service_id INT`
  );
  await db.query(
    `ALTER TABLE supervisor_conflict_alerts
       ADD COLUMN IF NOT EXISTS unassigned_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_supervisor_conflict_alerts_created
     ON supervisor_conflict_alerts(created_at DESC)`
  );
}

async function ensureOpsMessagesTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS ops_messages (
      id BIGSERIAL PRIMARY KEY,
      from_user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      to_user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      message_text TEXT NOT NULL,
      preset_code VARCHAR(60),
      is_traffic_alert BOOLEAN NOT NULL DEFAULT FALSE,
      related_service_id BIGINT REFERENCES services(id) ON DELETE SET NULL,
      read_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_ops_messages_to_user_created
     ON ops_messages(to_user_id, created_at DESC)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_ops_messages_users_created
     ON ops_messages(from_user_id, to_user_id, created_at DESC)`
  );
}

const BUILTIN_DRIVER_MESSAGE_PRESETS = [
  { code: "delay_traffic", label: "Atraso por trânsito intenso" },
  { code: "breakdown", label: "Avaria na viatura" },
  { code: "accident", label: "Acidente no percurso" },
  { code: "route_blocked", label: "Via cortada/desvio necessário" },
  { code: "request_support", label: "Preciso de apoio operacional" },
];

async function ensureOpsMessagePresetsTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS ops_message_presets (
      id BIGSERIAL PRIMARY KEY,
      scope VARCHAR(20) NOT NULL,
      code VARCHAR(60) NOT NULL,
      label VARCHAR(200) NOT NULL,
      default_message_text TEXT,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_by_user_id INT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (scope, code)
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_ops_message_presets_scope_active
     ON ops_message_presets(scope, is_active)`
  );
}

function mergeMessagePresets(builtin, dbRows) {
  const byCode = new Map();
  builtin.forEach((p) => {
    if (!p?.code) return;
    byCode.set(p.code, { ...p, source: "builtin" });
  });
  (Array.isArray(dbRows) ? dbRows : []).forEach((row) => {
    if (!row?.code) return;
    byCode.set(row.code, {
      id: row.id,
      code: row.code,
      label: row.label,
      defaultText: row.default_message_text,
      isActive: row.is_active,
      source: "custom",
    });
  });
  return [...byCode.values()].sort((a, b) => String(a.label || "").localeCompare(String(b.label || ""), "pt"));
}

async function listDriverMessagePresetsMerged() {
  await ensureOpsMessagePresetsTable();
  const custom = await db.query(
    `SELECT id, code, label, default_message_text, is_active
     FROM ops_message_presets
     WHERE scope = 'driver' AND is_active = TRUE
     ORDER BY label ASC`
  );
  return mergeMessagePresets(BUILTIN_DRIVER_MESSAGE_PRESETS, custom.rows);
}

async function resolveSupervisorRecipient(preferredId) {
  const preferred = Number(preferredId);
  if (Number.isFinite(preferred) && preferred > 0) {
    const byId = await db.query(
      `SELECT id, name, role
       FROM users
       WHERE id = $1
         AND is_active = TRUE
         AND LOWER(TRIM(role::text)) IN ('supervisor', 'admin')
       LIMIT 1`,
      [preferred]
    );
    if (byId.rowCount) return byId.rows[0];
  }
  const fallback = await db.query(
    `SELECT id, name, role
     FROM users
     WHERE is_active = TRUE
       AND LOWER(TRIM(role::text)) IN ('supervisor', 'admin')
     ORDER BY CASE WHEN LOWER(TRIM(role::text)) = 'admin' THEN 0 ELSE 1 END, id ASC
     LIMIT 1`
  );
  return fallback.rows[0] || null;
}

router.get("/message-presets", async (_req, res) => {
  try {
    const list = await listDriverMessagePresetsMerged();
    return res.json(list);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar predefinidas de mensagem (motorista)." });
  }
});

router.get("/messages", async (req, res) => {
  try {
    await ensureOpsMessagesTable();
    const sinceId = Number(req.query.sinceId);
    const useSince = Number.isFinite(sinceId) && sinceId > 0;
    const result = await db.query(
      `SELECT
         m.id,
         m.from_user_id,
         m.to_user_id,
         m.message_text,
         m.preset_code,
         m.is_traffic_alert,
         m.related_service_id,
         m.read_at,
         m.created_at,
         fu.name AS from_name,
         fu.role::text AS from_role,
         tu.name AS to_name,
         tu.role::text AS to_role
       FROM ops_messages m
       JOIN users fu ON fu.id = m.from_user_id
       JOIN users tu ON tu.id = m.to_user_id
       WHERE (m.from_user_id = $1 OR m.to_user_id = $1)
         AND ($2::bigint IS NULL OR m.id > $2::bigint)
       ORDER BY m.created_at DESC
       LIMIT 200`,
      [req.user.id, useSince ? sinceId : null]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar mensagens operacionais." });
  }
});

router.post("/messages", async (req, res) => {
  try {
    await ensureOpsMessagesTable();
    const role = String(req.user?.role || "").trim().toLowerCase();
    if (role !== "driver") {
      return res.status(403).json({ message: "Apenas motoristas podem usar este endpoint." });
    }

    const messageText = String(req.body?.message || "").trim();
    const presetCode = String(req.body?.presetCode || "").trim() || null;
    const isTrafficAlert = req.body?.isTrafficAlert === true;
    const relatedServiceIdRaw = Number(req.body?.relatedServiceId);
    const relatedServiceId = Number.isFinite(relatedServiceIdRaw) && relatedServiceIdRaw > 0 ? relatedServiceIdRaw : null;
    if (!messageText) {
      return res.status(400).json({ message: "Mensagem obrigatória." });
    }

    const recipient = await resolveSupervisorRecipient(req.body?.toUserId);
    if (!recipient) {
      return res.status(404).json({ message: "Sem supervisor/admin ativo para receber a mensagem." });
    }

    const inserted = await db.query(
      `INSERT INTO ops_messages (
         from_user_id, to_user_id, message_text, preset_code, is_traffic_alert, related_service_id
       )
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, created_at`,
      [req.user.id, recipient.id, messageText, presetCode, isTrafficAlert, relatedServiceId]
    );
    return res.status(201).json({
      message: "Mensagem enviada ao supervisor.",
      id: inserted.rows[0].id,
      createdAt: inserted.rows[0].created_at,
      toUser: { id: recipient.id, name: recipient.name, role: recipient.role },
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao enviar mensagem operacional." });
  }
});

router.patch("/messages/:messageId/read", async (req, res) => {
  try {
    await ensureOpsMessagesTable();
    const messageId = Number(req.params.messageId);
    if (!Number.isFinite(messageId) || messageId <= 0) {
      return res.status(400).json({ message: "Identificador de mensagem inválido." });
    }
    const updated = await db.query(
      `UPDATE ops_messages
       SET read_at = NOW()
       WHERE id = $1
         AND to_user_id = $2
         AND read_at IS NULL
       RETURNING id`,
      [messageId, req.user.id]
    );
    if (!updated.rowCount) {
      return res.status(404).json({ message: "Mensagem não encontrada." });
    }
    return res.json({ ok: true, id: updated.rows[0].id });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao marcar mensagem como lida." });
  }
});

router.get("/notifications", async (req, res) => {
  try {
    await ensureDriverNotificationsTable();
    const result = await db.query(
      `SELECT id, title, message, notification_type, read_at, created_at
       FROM driver_notifications
       WHERE driver_id = $1
       ORDER BY created_at DESC
       LIMIT 50`,
      [req.user.id]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar notificacoes." });
  }
});

router.patch("/notifications/:notificationId/read", async (req, res) => {
  try {
    await ensureDriverNotificationsTable();
    const notificationId = Number(req.params.notificationId);
    if (!Number.isFinite(notificationId) || notificationId <= 0) {
      return res.status(400).json({ message: "Identificador invalido." });
    }
    const result = await db.query(
      `UPDATE driver_notifications
       SET read_at = NOW()
       WHERE id = $1 AND driver_id = $2 AND read_at IS NULL
       RETURNING id`,
      [notificationId, req.user.id]
    );
    if (!result.rowCount) {
      return res.status(404).json({ message: "Notificacao nao encontrada." });
    }
    return res.json({ ok: true, id: result.rows[0].id });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao marcar notificacao como lida." });
  }
});

router.post("/conflicts/notify-supervisor", async (req, res) => {
  try {
    const role = String(req.user?.role || "").trim().toLowerCase();
    if (role !== "driver") {
      return res.status(403).json({ message: "Apenas motoristas podem enviar este alerta." });
    }
    await ensureSupervisorConflictAlertsTable();
    const rosterId = Number(req.body?.rosterId);
    const plannedServiceId = Number(req.body?.plannedServiceId);
    const serviceSchedule = String(req.body?.serviceSchedule || "").trim() || null;
    const lineCode = String(req.body?.lineCode || "").trim() || null;
    const notes = String(req.body?.notes || "").trim() || null;
    const rawConflictIds = Array.isArray(req.body?.conflictPlannedServiceIds) ? req.body.conflictPlannedServiceIds : [];
    const conflictPlannedServiceIds = rawConflictIds
      .map((id) => Number(id))
      .filter((id) => Number.isFinite(id) && id > 0);

    if (!Number.isFinite(plannedServiceId) || plannedServiceId <= 0) {
      return res.status(400).json({ message: "plannedServiceId invalido." });
    }

    const result = await db.query(
      `INSERT INTO supervisor_conflict_alerts (
         driver_id, roster_id, planned_service_id, service_schedule, line_code, conflict_planned_service_ids, notes
       )
       VALUES ($1, $2, $3, $4, $5, $6::jsonb, $7)
       RETURNING id, created_at`,
      [
        req.user.id,
        Number.isFinite(rosterId) && rosterId > 0 ? rosterId : null,
        plannedServiceId,
        serviceSchedule,
        lineCode,
        JSON.stringify(conflictPlannedServiceIds),
        notes,
      ]
    );

    return res.status(201).json({
      message: "Alerta enviado ao supervisor.",
      alertId: result.rows[0].id,
      createdAt: result.rows[0].created_at,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao enviar alerta ao supervisor." });
  }
});

router.get("/history-detailed", async (req, res) => {
  try {
    const servicesResult = await db.query(
      `SELECT
         s.id,
         s.plate_number,
         s.service_schedule,
         s.line_code,
         s.fleet_number,
         s.status,
         s.started_at,
         s.ended_at,
         s.total_km,
         s.route_deviation_m,
         s.is_off_route
       FROM services s
       WHERE EXISTS (
         SELECT 1
         FROM service_segments seg
         WHERE seg.service_id = s.id
           AND seg.driver_id = $1
       )
       ORDER BY s.started_at DESC`,
      [req.user.id]
    );

    const services = [];
    for (const s of servicesResult.rows) {
      const segmentsResult = await db.query(
        `SELECT
           seg.id,
           seg.fleet_number,
           seg.status,
           seg.started_at,
           seg.ended_at,
           seg.km_segment,
           u.id AS driver_id,
           u.name AS driver_name,
           u.mechanic_number
         FROM service_segments seg
         JOIN users u ON u.id = seg.driver_id
         WHERE seg.service_id = $1
         ORDER BY seg.started_at ASC`,
        [s.id]
      );

      const handoversResult = await db.query(
        `SELECT
           h.id,
           h.reason,
           h.notes,
           h.from_fleet_number,
           h.to_fleet_number,
           h.handover_lat,
           h.handover_lng,
           h.handover_location_text,
           h.created_at,
           h.completed_at,
           h.status,
           u_from.name AS from_driver_name,
           u_from.mechanic_number AS from_driver_mechanic_number,
           u_to.name AS to_driver_name,
           u_to.mechanic_number AS to_driver_mechanic_number
         FROM service_handover_events h
         JOIN users u_from ON u_from.id = h.from_driver_id
         LEFT JOIN users u_to ON u_to.id = h.to_driver_id
         WHERE h.service_id = $1
         ORDER BY h.created_at ASC`,
        [s.id]
      );

      const initiatedBy = segmentsResult.rows[0]
        ? {
            driverId: segmentsResult.rows[0].driver_id,
            driverName: segmentsResult.rows[0].driver_name,
            mechanicNumber: segmentsResult.rows[0].mechanic_number,
            fleetNumber: segmentsResult.rows[0].fleet_number,
            startedAt: segmentsResult.rows[0].started_at,
          }
        : null;

      services.push({
        ...s,
        initiated_by: initiatedBy,
        segments: segmentsResult.rows,
        handovers: handoversResult.rows,
      });
    }

    return res.json(services);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao listar historico detalhado." });
  }
});

router.get("/today-planned", async (req, res) => {
  try {
    await ensurePlannedServiceLocationColumns();
    await ensureDriverNotificationsTable();

    const result = await db.query(
      `SELECT
         dr.id AS roster_id,
         dr.service_date,
         dr.status AS roster_status,
         ps.id AS planned_service_id,
         ps.service_code,
         ps.line_code,
         COALESCE(NULLIF(TRIM(ps.start_location), ''), '-') AS start_location,
         COALESCE(NULLIF(TRIM(ps.end_location), ''), '-') AS end_location,
         ps.kms_carga,
         ps.fleet_number,
         ps.plate_number,
         ps.service_schedule,
         EXISTS (
           SELECT 1
           FROM driver_notifications dn
           WHERE dn.driver_id = dr.driver_id
             AND dn.roster_id = dr.id
             AND dn.notification_type = 'roster_assigned'
         ) AS is_roster_changed
       FROM daily_roster dr
       JOIN planned_services ps ON ps.id = dr.planned_service_id
       WHERE dr.driver_id = $1
         AND dr.service_date = CURRENT_DATE
         AND COALESCE(ps.kms_carga, 0) > 0
       ORDER BY ps.service_schedule ASC`,
      [req.user.id]
    );
    return res.json(result.rows);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao listar servicos planeados de hoje." });
  }
});

router.get("/pending-handover", async (req, res) => {
  try {
    const result = await db.query(
      `SELECT
         h.id AS handover_id,
         h.service_id,
         h.reason,
         h.notes,
         h.from_fleet_number,
         h.to_fleet_number,
         h.created_at,
         u.name AS from_driver_name,
         s.line_code,
         s.service_schedule
       FROM service_handover_events h
       JOIN users u ON u.id = h.from_driver_id
       JOIN services s ON s.id = h.service_id
       WHERE h.to_driver_id = $1
         AND h.status = 'pending'
       ORDER BY h.created_at DESC`,
      [req.user.id]
    );
    return res.json(result.rows);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao listar handovers pendentes." });
  }
});

router.post("/start", async (req, res) => {
  const { plateNumber, serviceSchedule, lineCode, fleetNumber, plannedServiceId } = req.body;

  try {
    // Do not block trip start if DB user has limited DDL permissions in production.
    // Odometer enrichment is best-effort.
    let odometerFeaturesReady = true;
    try {
      await ensureServiceVehicleOdometerColumns();
    } catch (_schemaError) {
      odometerFeaturesReady = false;
    }
    const active = await db.query(
      "SELECT id FROM services WHERE driver_id = $1 AND status = 'in_progress' LIMIT 1",
      [req.user.id]
    );

    if (active.rowCount > 0) {
      return res.status(409).json({ message: "Ja existe uma viagem em curso." });
    }

    let header = { plateNumber, serviceSchedule, lineCode, fleetNumber };
    let validPlannedServiceId = null;

    if (plannedServiceId) {
      const plannedResult = await db.query(
        `SELECT
           ps.id,
           ps.plate_number,
           ps.service_schedule,
           ps.line_code,
           ps.fleet_number
         FROM daily_roster dr
         JOIN planned_services ps ON ps.id = dr.planned_service_id
         WHERE dr.driver_id = $1
           AND dr.service_date = CURRENT_DATE
           AND dr.planned_service_id = $2
         LIMIT 1`,
        [req.user.id, plannedServiceId]
      );

      if (plannedResult.rowCount === 0) {
        return res.status(404).json({ message: "Servico planeado nao encontrado para hoje." });
      }

      const planned = plannedResult.rows[0];
      header = {
        plateNumber: planned.plate_number,
        serviceSchedule: planned.service_schedule,
        lineCode: planned.line_code,
        // Keep planned service data, but allow fleet override on operational swap.
        fleetNumber: fleetNumber || planned.fleet_number,
      };
      validPlannedServiceId = planned.id;
    }

    if (!header.plateNumber || !header.serviceSchedule || !header.lineCode || !header.fleetNumber) {
      return res.status(400).json({
        message: "Preencha chapa, horario, linha e numero de frota.",
      });
    }

    const gtfsTrip = await findBestTripForLine(header.lineCode, header.serviceSchedule);
    let vehicleOdometerStartKm = null;
    if (odometerFeaturesReady) {
      try {
        const vehicleMetaRes = await db.query(
          `SELECT current_odometer_km
           FROM tracker_devices
           WHERE (LOWER(TRIM(COALESCE(fleet_number, ''))) = LOWER(TRIM($1))
                  OR LOWER(TRIM(COALESCE(plate_number, ''))) = LOWER(TRIM($2)))
           ORDER BY updated_at DESC
           LIMIT 1`,
          [String(header.fleetNumber || ""), String(header.plateNumber || "")]
        );
        vehicleOdometerStartKm = vehicleMetaRes.rowCount ? Number(vehicleMetaRes.rows[0].current_odometer_km) : null;
      } catch (_odometerLookupError) {
        vehicleOdometerStartKm = null;
      }
    }

    let result;
    if (odometerFeaturesReady) {
      try {
        result = await db.query(
          `INSERT INTO services (
             driver_id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number, status,
             vehicle_odometer_start_km
           )
           VALUES ($1, $2, $3, $4, $5, $6, $7, 'in_progress', $8)
           RETURNING id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number, status, started_at`,
          [
            req.user.id,
            validPlannedServiceId,
            gtfsTrip?.trip_id || null,
            header.plateNumber,
            header.serviceSchedule,
            header.lineCode,
            header.fleetNumber,
            Number.isFinite(vehicleOdometerStartKm) ? vehicleOdometerStartKm : null,
          ]
        );
      } catch (_insertWithOdometerError) {
        odometerFeaturesReady = false;
      }
    }
    if (!result) {
      result = await db.query(
        `INSERT INTO services (driver_id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number, status)
         VALUES ($1, $2, $3, $4, $5, $6, $7, 'in_progress')
         RETURNING id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number, status, started_at`,
        [
          req.user.id,
          validPlannedServiceId,
          gtfsTrip?.trip_id || null,
          header.plateNumber,
          header.serviceSchedule,
          header.lineCode,
          header.fleetNumber,
        ]
      );
    }

    await db.query(
      `INSERT INTO service_segments (service_id, driver_id, fleet_number, status)
       VALUES ($1, $2, $3, 'in_progress')`,
      [result.rows[0].id, req.user.id, header.fleetNumber]
    );

    if (validPlannedServiceId) {
      await db.query(
        `UPDATE daily_roster
         SET status = 'in_progress'
         WHERE driver_id = $1
           AND planned_service_id = $2
           AND service_date = CURRENT_DATE`,
        [req.user.id, validPlannedServiceId]
      );
    }

    return res.status(201).json(result.rows[0]);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao iniciar viagem." });
  }
});

router.post("/:serviceId/points", async (req, res) => {
  const { serviceId } = req.params;
  const normalized = normalizeIncomingPoint(req.body);
  if (!normalized.ok) {
    return res.status(400).json({ message: normalized.error });
  }

  try {
    await ensureServicePointsQualityColumns();
    const ownerCheck = await db.query(
      `SELECT id, status, gtfs_trip_id, line_code, service_schedule, planned_service_id, driver_id, started_at
       FROM services
       WHERE id = $1 AND driver_id = $2 AND status = 'in_progress'`,
      [serviceId, req.user.id]
    );

    if (ownerCheck.rowCount === 0) {
      return res.status(404).json({ message: "Viagem ativa nao encontrada." });
    }

    const activeSegment = await getActiveSegment(serviceId);
    if (!activeSegment) {
      return res.status(409).json({ message: "Servico sem segmento ativo." });
    }

    await insertServicePoint(serviceId, activeSegment.id, normalized.point);
    const routeCheck = await updateServiceRouteCheck(serviceId, ownerCheck.rows[0].gtfs_trip_id, normalized.point);
    const autoClose = await maybeAutoCompleteServiceAtLastStop(ownerCheck.rows[0], normalized.point);

    return res.status(201).json({
      message: autoClose.autoClosed ? "Ponto registado. Serviço encerrado automaticamente na última paragem." : "Ponto registado.",
      acceptedPoint: {
        source: normalized.point.source,
        accuracyM: normalized.point.accuracyM,
        speedKmh: normalized.point.speedKmh,
        headingDeg: normalized.point.headingDeg,
      },
      routeCheck: {
        deviationMeters: routeCheck.deviationMeters,
        isOffRoute: routeCheck.isOffRoute,
        thresholdMeters: 150,
      },
      autoClosed: autoClose.autoClosed,
      autoCloseReason: autoClose.reason || null,
    });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao guardar ponto GPS." });
  }
});

router.post("/:serviceId/points/batch", async (req, res) => {
  const { serviceId } = req.params;
  const points = Array.isArray(req.body?.points) ? req.body.points : null;
  if (!points || points.length === 0) {
    return res.status(400).json({ message: "Indique points com pelo menos 1 registo." });
  }
  if (points.length > 500) {
    return res.status(400).json({ message: "Batch demasiado grande. Maximo: 500 pontos por pedido." });
  }

  try {
    await ensureServicePointsQualityColumns();
    const ownerCheck = await db.query(
      `SELECT id, status, gtfs_trip_id, line_code, service_schedule, planned_service_id, driver_id, started_at
       FROM services
       WHERE id = $1 AND driver_id = $2 AND status = 'in_progress'`,
      [serviceId, req.user.id]
    );
    if (ownerCheck.rowCount === 0) {
      return res.status(404).json({ message: "Viagem ativa nao encontrada." });
    }

    const activeSegment = await getActiveSegment(serviceId);
    if (!activeSegment) {
      return res.status(409).json({ message: "Servico sem segmento ativo." });
    }

    let acceptedCount = 0;
    const rejected = [];
    let lastAcceptedPoint = null;

    for (let i = 0; i < points.length; i += 1) {
      const normalized = normalizeIncomingPoint(points[i]);
      if (!normalized.ok) {
        rejected.push({ index: i, message: normalized.error });
        continue;
      }
      await insertServicePoint(serviceId, activeSegment.id, normalized.point);
      acceptedCount += 1;
      lastAcceptedPoint = normalized.point;
    }

    let routeCheck = { deviationMeters: null, isOffRoute: false, thresholdMeters: 150 };
    if (lastAcceptedPoint) {
      const checked = await updateServiceRouteCheck(serviceId, ownerCheck.rows[0].gtfs_trip_id, lastAcceptedPoint);
      routeCheck = {
        deviationMeters: checked.deviationMeters,
        isOffRoute: checked.isOffRoute,
        thresholdMeters: 150,
      };
    }
    const autoClose = lastAcceptedPoint
      ? await maybeAutoCompleteServiceAtLastStop(ownerCheck.rows[0], lastAcceptedPoint)
      : { autoClosed: false };

    return res.status(201).json({
      message: autoClose.autoClosed
        ? "Batch processado. Serviço encerrado automaticamente na última paragem."
        : "Batch processado.",
      acceptedCount,
      rejectedCount: rejected.length,
      rejected,
      routeCheck,
      autoClosed: autoClose.autoClosed,
      autoCloseReason: autoClose.reason || null,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao guardar batch de pontos GPS." });
  }
});

router.get("/:serviceId/reference-route", async (req, res) => {
  const { serviceId } = req.params;
  try {
    const serviceResult = await db.query(
      `SELECT id, gtfs_trip_id, line_code, service_schedule
       FROM services
       WHERE id = $1 AND driver_id = $2`,
      [serviceId, req.user.id]
    );
    if (serviceResult.rowCount === 0) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }

    const service = serviceResult.rows[0];
    let tripId = service.gtfs_trip_id;
    if (!tripId) {
      const bestTrip = await findBestTripForLine(service.line_code, service.service_schedule);
      tripId = bestTrip?.trip_id || null;
      if (tripId) {
        await db.query(`UPDATE services SET gtfs_trip_id = $1 WHERE id = $2 AND gtfs_trip_id IS NULL`, [
          tripId,
          serviceId,
        ]);
      }
    }
    if (!tripId) {
      return res.status(404).json({ message: "Sem rota GTFS para esta linha/horario." });
    }

    const shapePoints = await getShapePointsByTripId(tripId);
    const stops = await getStopsByTripId(tripId);
    return res.json({
      tripId,
      lineCode: service.line_code,
      points: shapePoints,
      stops,
    });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao obter rota de referencia GTFS." });
  }
});

router.get("/reference-route-preview/by-header", async (req, res) => {
  const lineCode = String(req.query.lineCode || "").trim();
  const serviceSchedule = String(req.query.serviceSchedule || "").trim();
  if (!lineCode || !serviceSchedule) {
    return res.status(400).json({ message: "Indique lineCode e serviceSchedule." });
  }
  try {
    const bestTrip = await findBestTripForLine(lineCode, serviceSchedule);
    const tripId = bestTrip?.trip_id || null;
    if (!tripId) {
      return res.status(404).json({ message: "Sem rota GTFS para esta linha/horario." });
    }
    const shapePoints = await getShapePointsByTripId(tripId);
    const stops = await getStopsByTripId(tripId);
    return res.json({
      tripId,
      lineCode,
      serviceSchedule,
      points: shapePoints,
      stops,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao obter rota de referencia." });
  }
});

router.get("/:serviceId/announcement-state", async (req, res) => {
  const serviceId = Number(req.params.serviceId);
  const preannounceMeters = Math.max(80, Math.min(800, Number(req.query.preannounceMeters) || 300));
  const arrivalMeters = Math.max(20, Math.min(180, Number(req.query.arrivalMeters) || 60));
  if (!Number.isFinite(serviceId) || serviceId <= 0) {
    return res.status(400).json({ message: "serviceId invalido." });
  }
  try {
    await ensureServiceStopProgressTable();

    const serviceRes = await db.query(
      `SELECT id, driver_id, gtfs_trip_id, line_code, service_schedule, status
       FROM services
       WHERE id = $1`,
      [serviceId]
    );
    if (!serviceRes.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    const service = serviceRes.rows[0];
    if (!canReadServiceByRole(req.user, service.driver_id)) {
      return res.status(403).json({ message: "Sem permissao para consultar este servico." });
    }

    let tripId = service.gtfs_trip_id;
    if (!tripId) {
      const bestTrip = await findBestTripForLine(service.line_code, service.service_schedule);
      tripId = bestTrip?.trip_id || null;
      if (tripId) {
        await db.query(`UPDATE services SET gtfs_trip_id = $1 WHERE id = $2 AND gtfs_trip_id IS NULL`, [tripId, serviceId]);
      }
    }
    if (!tripId) {
      return res.status(404).json({ message: "Sem rota/paragens GTFS para esta linha/horario." });
    }

    const latestPointRes = await db.query(
      `SELECT lat, lng, captured_at, speed_kmh
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at DESC
       LIMIT 1`,
      [serviceId]
    );
    if (!latestPointRes.rowCount) {
      return res.json({
        serviceId,
        tripId,
        status: service.status,
        hasGpsFix: false,
        nextStop: null,
        currentStop: null,
        triggers: {
          announceNextStop: false,
          announceArrivedStop: false,
        },
      });
    }
    const latestPoint = latestPointRes.rows[0];
    const stops = (await getStopsByTripId(tripId))
      .map((s) => ({
        stop_id: s.stop_id,
        stop_name: s.stop_name,
        stop_sequence: normalizeStopSequence(s.stop_sequence),
        lat: Number(s.lat),
        lng: Number(s.lng),
      }))
      .filter((s) => s.stop_sequence != null && Number.isFinite(s.lat) && Number.isFinite(s.lng))
      .sort((a, b) => a.stop_sequence - b.stop_sequence);
    if (!stops.length) {
      return res.status(404).json({ message: "Sem paragens GTFS para este serviço." });
    }

    const progressRes = await db.query(
      `SELECT
         last_passed_stop_id, last_passed_stop_sequence, last_passed_at,
         last_announced_stop_id, last_announced_stop_sequence, last_announcement_type, last_announced_at
       FROM service_stop_progress
       WHERE service_id = $1`,
      [serviceId]
    );
    const progress = progressRes.rows[0] || {};
    let lastPassedSeq = normalizeStopSequence(progress.last_passed_stop_sequence) || 0;

    const candidateStops = stops.filter((s) => s.stop_sequence > lastPassedSeq);
    let currentStop = null;
    if (candidateStops.length) {
      const firstUpcoming = candidateStops[0];
      const d = geoDistanceMeters(latestPoint.lat, latestPoint.lng, firstUpcoming.lat, firstUpcoming.lng);
      if (d != null && d <= arrivalMeters) {
        currentStop = { ...firstUpcoming, distance_m: Math.round(d) };
        lastPassedSeq = firstUpcoming.stop_sequence;
        await db.query(
          `INSERT INTO service_stop_progress (service_id, last_passed_stop_id, last_passed_stop_sequence, last_passed_at, updated_at)
           VALUES ($1, $2, $3, NOW(), NOW())
           ON CONFLICT (service_id) DO UPDATE
             SET last_passed_stop_id = EXCLUDED.last_passed_stop_id,
                 last_passed_stop_sequence = EXCLUDED.last_passed_stop_sequence,
                 last_passed_at = EXCLUDED.last_passed_at,
                 updated_at = NOW()`,
          [serviceId, firstUpcoming.stop_id || null, firstUpcoming.stop_sequence]
        );
      }
    }

    const nextStopRaw = stops.find((s) => s.stop_sequence > lastPassedSeq) || null;
    const nextDistance = nextStopRaw
      ? geoDistanceMeters(latestPoint.lat, latestPoint.lng, nextStopRaw.lat, nextStopRaw.lng)
      : null;
    const speedKmh = Number(latestPoint.speed_kmh);
    const etaSeconds =
      nextDistance != null && Number.isFinite(speedKmh) && speedKmh > 2
        ? Math.round(nextDistance / ((speedKmh * 1000) / 3600))
        : null;

    const lastAnnouncedSeq = normalizeStopSequence(progress.last_announced_stop_sequence);
    const lastAnnouncementType = String(progress.last_announcement_type || "").trim().toLowerCase();
    const announceNextStop =
      !!nextStopRaw &&
      nextDistance != null &&
      nextDistance <= preannounceMeters &&
      !(lastAnnouncedSeq === nextStopRaw.stop_sequence && lastAnnouncementType === "next");
    const announceArrivedStop =
      !!currentStop &&
      !(lastAnnouncedSeq === currentStop.stop_sequence && lastAnnouncementType === "arrived");

    return res.json({
      serviceId,
      tripId,
      status: service.status,
      hasGpsFix: true,
      gps: {
        lat: Number(latestPoint.lat),
        lng: Number(latestPoint.lng),
        capturedAt: latestPoint.captured_at,
        speedKmh: Number.isFinite(speedKmh) ? speedKmh : null,
      },
      currentStop: currentStop
        ? {
            stopId: currentStop.stop_id,
            stopName: currentStop.stop_name,
            stopSequence: currentStop.stop_sequence,
            distanceM: currentStop.distance_m,
          }
        : null,
      nextStop: nextStopRaw
        ? {
            stopId: nextStopRaw.stop_id,
            stopName: nextStopRaw.stop_name,
            stopSequence: nextStopRaw.stop_sequence,
            distanceM: nextDistance == null ? null : Math.round(nextDistance),
            etaSeconds,
          }
        : null,
      thresholds: {
        preannounceMeters,
        arrivalMeters,
      },
      triggers: {
        announceNextStop,
        announceArrivedStop,
      },
      suggestedAnnouncements: {
        nextStopText: nextStopRaw ? `Próxima paragem: ${nextStopRaw.stop_name || "paragem sem nome"}.` : null,
        arrivedStopText: currentStop ? `Paragem: ${currentStop.stop_name || "paragem sem nome"}.` : null,
      },
      progress: {
        lastPassedStopSequence: lastPassedSeq || null,
        lastAnnouncedStopSequence: lastAnnouncedSeq || null,
        lastAnnouncementType: lastAnnouncementType || null,
        lastAnnouncedAt: progress.last_announced_at || null,
      },
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao obter estado de anúncio de paragens." });
  }
});

router.post("/:serviceId/announcement-mark", async (req, res) => {
  const serviceId = Number(req.params.serviceId);
  const stopId = String(req.body?.stopId || "").trim() || null;
  const stopSequence = normalizeStopSequence(req.body?.stopSequence);
  const announcementType = String(req.body?.announcementType || "")
    .trim()
    .toLowerCase();
  if (!Number.isFinite(serviceId) || serviceId <= 0) {
    return res.status(400).json({ message: "serviceId invalido." });
  }
  if (!stopId && !Number.isFinite(stopSequence)) {
    return res.status(400).json({ message: "Indique stopId ou stopSequence." });
  }
  if (!["next", "arrived"].includes(announcementType)) {
    return res.status(400).json({ message: "announcementType invalido. Use next ou arrived." });
  }
  try {
    await ensureServiceStopProgressTable();
    const ownerRes = await db.query(`SELECT id, driver_id FROM services WHERE id = $1`, [serviceId]);
    if (!ownerRes.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    if (!canReadServiceByRole(req.user, ownerRes.rows[0].driver_id)) {
      return res.status(403).json({ message: "Sem permissao para marcar anuncio deste servico." });
    }

    await db.query(
      `INSERT INTO service_stop_progress (
         service_id, last_announced_stop_id, last_announced_stop_sequence, last_announcement_type, last_announced_at, updated_at
       ) VALUES ($1, $2, $3, $4, NOW(), NOW())
       ON CONFLICT (service_id) DO UPDATE
         SET last_announced_stop_id = EXCLUDED.last_announced_stop_id,
             last_announced_stop_sequence = EXCLUDED.last_announced_stop_sequence,
             last_announcement_type = EXCLUDED.last_announcement_type,
             last_announced_at = EXCLUDED.last_announced_at,
             updated_at = NOW()`,
      [serviceId, stopId, Number.isFinite(stopSequence) ? stopSequence : null, announcementType]
    );
    return res.json({ message: "Anúncio registado.", serviceId, stopId, stopSequence, announcementType });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao registar anúncio." });
  }
});

router.post("/:serviceId/handover", async (req, res) => {
  const { serviceId } = req.params;
  const { toMechanicNumber, toFleetNumber, reason, notes, handoverLocationText } = req.body;

  if (!toMechanicNumber || !reason) {
    return res.status(400).json({ message: "Informe motorista destino e motivo da troca." });
  }

  try {
    const serviceResult = await db.query(
      `SELECT id, driver_id, fleet_number, status, line_code, service_schedule
       FROM services
       WHERE id = $1 AND driver_id = $2`,
      [serviceId, req.user.id]
    );

    if (serviceResult.rowCount === 0) {
      return res.status(404).json({ message: "Servico nao encontrado para handover." });
    }
    if (serviceResult.rows[0].status !== "in_progress") {
      return res.status(409).json({ message: "Servico nao esta em curso." });
    }

    const toDriverResult = await db.query(
      `SELECT id, name
       FROM users
       WHERE mechanic_number = $1 AND role = 'driver'`,
      [toMechanicNumber]
    );
    if (toDriverResult.rowCount === 0) {
      return res.status(404).json({ message: "Motorista destino nao encontrado." });
    }

    const toDriver = toDriverResult.rows[0];
    if (toDriver.id === req.user.id) {
      return res.status(400).json({ message: "Motorista destino deve ser diferente do atual." });
    }

    const activeOther = await db.query(
      `SELECT id FROM services
       WHERE driver_id = $1 AND status = 'in_progress'
       LIMIT 1`,
      [toDriver.id]
    );
    if (activeOther.rowCount > 0) {
      return res.status(409).json({ message: "Motorista destino ja tem servico em curso." });
    }

    const closedSegment = await closeActiveSegment(serviceId);

    const latestPointResult = await db.query(
      `SELECT lat, lng
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at DESC
       LIMIT 1`,
      [serviceId]
    );
    const latestPoint = latestPointResult.rows[0] || {};

    const handover = await db.query(
      `INSERT INTO service_handover_events (
         service_id, from_segment_id, from_driver_id, to_driver_id,
         from_fleet_number, to_fleet_number, reason, notes,
         handover_lat, handover_lng, handover_location_text, status
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'pending')
       RETURNING id, service_id, reason, status, to_driver_id, to_fleet_number`,
      [
        serviceId,
        closedSegment?.id || null,
        req.user.id,
        toDriver.id,
        serviceResult.rows[0].fleet_number,
        toFleetNumber || serviceResult.rows[0].fleet_number,
        reason,
        notes || null,
        latestPoint.lat || null,
        latestPoint.lng || null,
        handoverLocationText || null,
      ]
    );

    await db.query(
      `UPDATE services
       SET status = 'awaiting_handover'
       WHERE id = $1`,
      [serviceId]
    );

    await ensureDriverNotificationsTable();
    await db.query(
      `INSERT INTO driver_notifications (driver_id, title, message, notification_type, roster_id)
       VALUES ($1, $2, $3, 'service_handover_pending', NULL)`,
      [
        toDriver.id,
        "Transferência de serviço pendente",
        `Tem um serviço pendente para assumir: Serviço ${serviceId} | Linha ${serviceResult.rows[0].line_code || "-"} | ${serviceResult.rows[0].service_schedule || "-"} | Motivo: ${reason}`,
      ]
    );

    return res.status(201).json({
      message: "Handover criado. Servico aguardando continuidade.",
      handover: handover.rows[0],
    });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao criar handover." });
  }
});

router.post("/:serviceId/resume", async (req, res) => {
  const { serviceId } = req.params;
  const { handoverId } = req.body;

  if (!handoverId) {
    return res.status(400).json({ message: "handoverId obrigatorio." });
  }

  try {
    const handoverResult = await db.query(
      `SELECT id, service_id, to_driver_id, to_fleet_number, status
       FROM service_handover_events
       WHERE id = $1 AND service_id = $2`,
      [handoverId, serviceId]
    );
    if (handoverResult.rowCount === 0) {
      return res.status(404).json({ message: "Handover nao encontrado." });
    }

    const handover = handoverResult.rows[0];
    if (handover.status !== "pending") {
      return res.status(409).json({ message: "Handover ja processado." });
    }
    if (handover.to_driver_id !== req.user.id) {
      return res.status(403).json({ message: "Este handover nao pertence ao motorista autenticado." });
    }

    const activeOwn = await db.query(
      `SELECT id FROM services
       WHERE driver_id = $1 AND status = 'in_progress'
       LIMIT 1`,
      [req.user.id]
    );
    if (activeOwn.rowCount > 0) {
      return res.status(409).json({ message: "Ja existe servico em curso para este motorista." });
    }

    const serviceResult = await db.query(
      `SELECT id, status
       FROM services
       WHERE id = $1`,
      [serviceId]
    );
    if (serviceResult.rowCount === 0) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    if (serviceResult.rows[0].status !== "awaiting_handover") {
      return res.status(409).json({ message: "Servico nao esta em estado de handover." });
    }

    await db.query(
      `INSERT INTO service_segments (service_id, driver_id, fleet_number, status)
       VALUES ($1, $2, $3, 'in_progress')`,
      [serviceId, req.user.id, handover.to_fleet_number]
    );

    const updatedService = await db.query(
      `UPDATE services
       SET driver_id = $2,
           fleet_number = $3,
           status = 'in_progress'
       WHERE id = $1
       RETURNING id, plate_number, service_schedule, line_code, fleet_number, status, started_at`,
      [serviceId, req.user.id, handover.to_fleet_number]
    );

    await db.query(
      `UPDATE service_handover_events
       SET status = 'completed',
           completed_at = NOW()
       WHERE id = $1`,
      [handoverId]
    );

    return res.json({
      message: "Servico retomado com sucesso.",
      service: updatedService.rows[0],
    });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao retomar servico." });
  }
});

router.post("/:serviceId/end", async (req, res) => {
  const { serviceId } = req.params;
  const vehicleOdometerEndRaw = req.body?.vehicleOdometerKm;
  const vehicleOdometerEndKm =
    vehicleOdometerEndRaw == null || vehicleOdometerEndRaw === "" ? null : Number(vehicleOdometerEndRaw);
  if (vehicleOdometerEndKm != null && (!Number.isFinite(vehicleOdometerEndKm) || vehicleOdometerEndKm < 0)) {
    return res.status(400).json({ message: "vehicleOdometerKm inválido." });
  }

  try {
    await ensureServiceVehicleOdometerColumns();
    const serviceResult = await db.query(
      `SELECT id, status, planned_service_id, started_at, fleet_number, plate_number, vehicle_odometer_start_km
       FROM services
       WHERE id = $1 AND driver_id = $2`,
      [serviceId, req.user.id]
    );

    if (serviceResult.rowCount === 0) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }

    if (serviceResult.rows[0].status !== "in_progress") {
      return res.status(409).json({ message: "Servico ja finalizado." });
    }

    await closeActiveSegment(serviceId);

    const pointsResult = await db.query(
      `SELECT lat, lng, captured_at, accuracy_m, speed_kmh
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at ASC`,
      [serviceId]
    );

    const points = pointsResult.rows.map((p) => ({
      lat: Number(p.lat),
      lng: Number(p.lng),
      capturedAt: p.captured_at,
      accuracyM: p.accuracy_m == null ? null : Number(p.accuracy_m),
      speedKmh: p.speed_kmh == null ? null : Number(p.speed_kmh),
    }));

    const totalKmGps = calculatePathDistance(points);
    const totalKm = await resolveTotalKmWithPlannedFallback(
      totalKmGps,
      serviceResult.rows[0].planned_service_id
    );
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
    const vehicleOdometerStartKm = Number(serviceResult.rows[0].vehicle_odometer_start_km);
    const odometerDeltaKm =
      Number.isFinite(vehicleOdometerStartKm) && Number.isFinite(vehicleOdometerEndKm)
        ? Number((vehicleOdometerEndKm - vehicleOdometerStartKm).toFixed(3))
        : null;
    const odometerVsGpsDiffKm =
      Number.isFinite(odometerDeltaKm) && Number.isFinite(Number(totalKm))
        ? Number((odometerDeltaKm - Number(totalKm)).toFixed(3))
        : null;

    const updated = await db.query(
      `UPDATE services
       SET status = 'completed',
           ended_at = NOW(),
           total_km = $2,
           route_geojson = $3,
           vehicle_odometer_end_km = COALESCE($4, vehicle_odometer_end_km),
           vehicle_odometer_delta_km = COALESCE($5, vehicle_odometer_delta_km),
           vehicle_odometer_vs_gps_diff_km = COALESCE($6, vehicle_odometer_vs_gps_diff_km),
           close_mode = 'manual'
       WHERE id = $1
       RETURNING id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number,
                 status, started_at, ended_at, total_km, route_geojson, route_deviation_m, is_off_route,
                 vehicle_odometer_start_km, vehicle_odometer_end_km, vehicle_odometer_delta_km, vehicle_odometer_vs_gps_diff_km, close_mode`,
      [serviceId, totalKm, JSON.stringify(routeGeoJSON), vehicleOdometerEndKm, odometerDeltaKm, odometerVsGpsDiffKm]
    );
    if (Number.isFinite(vehicleOdometerEndKm)) {
      await db.query(
        `UPDATE tracker_devices
         SET current_odometer_km = $1,
             current_odometer_updated_at = NOW(),
             updated_at = NOW()
         WHERE LOWER(TRIM(COALESCE(fleet_number, ''))) = LOWER(TRIM($2))
            OR LOWER(TRIM(COALESCE(plate_number, ''))) = LOWER(TRIM($3))`,
        [vehicleOdometerEndKm, String(serviceResult.rows[0].fleet_number || ""), String(serviceResult.rows[0].plate_number || "")]
      );
      await db.query(
        `INSERT INTO vehicle_odometer_logs (fleet_number, plate_number, odometer_km, captured_at, source)
         VALUES ($1, $2, $3, NOW(), 'manual_service_end')`,
        [String(serviceResult.rows[0].fleet_number || "") || null, String(serviceResult.rows[0].plate_number || "") || null, vehicleOdometerEndKm]
      );
    }

    if (serviceResult.rows[0]?.planned_service_id) {
      let rosterDay = await getRosterServiceDateForExecution(
        req.user.id,
        serviceResult.rows[0].planned_service_id,
        serviceResult.rows[0].started_at
      );
      if (!rosterDay) {
        const rosterDayRes = await db.query(
          `SELECT ($1::timestamptz AT TIME ZONE 'Europe/Lisbon')::date AS d`,
          [serviceResult.rows[0].started_at]
        );
        rosterDay = rosterDayRes.rows[0]?.d;
      }
      if (rosterDay) {
        await db.query(
          `UPDATE daily_roster
           SET status = 'completed'
           WHERE driver_id = $1
             AND planned_service_id = $2
             AND service_date = $3`,
          [req.user.id, serviceResult.rows[0].planned_service_id, rosterDay]
        );
      }
    }

    return res.json({
      ...updated.rows[0],
      kmComparison: {
        appTotalKm: Number(totalKm),
        odometerStartKm: Number.isFinite(vehicleOdometerStartKm) ? vehicleOdometerStartKm : null,
        odometerEndKm: Number.isFinite(vehicleOdometerEndKm) ? vehicleOdometerEndKm : null,
        odometerDeltaKm: Number.isFinite(odometerDeltaKm) ? odometerDeltaKm : null,
        odometerVsGpsDiffKm: Number.isFinite(odometerVsGpsDiffKm) ? odometerVsGpsDiffKm : null,
      },
    });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao finalizar viagem." });
  }
});

router.post("/:serviceId/cancel", async (req, res) => {
  const { serviceId } = req.params;

  try {
    const serviceResult = await db.query(
      `SELECT id, status, planned_service_id
       FROM services
       WHERE id = $1 AND driver_id = $2`,
      [serviceId, req.user.id]
    );

    if (serviceResult.rowCount === 0) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }

    if (serviceResult.rows[0].status !== "in_progress") {
      return res.status(409).json({ message: "Apenas servicos em curso podem ser anulados." });
    }

    await closeActiveSegment(serviceId, "cancelled");

    const pointsResult = await db.query(
      `SELECT lat, lng, captured_at, accuracy_m, speed_kmh
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at ASC`,
      [serviceId]
    );

    const points = pointsResult.rows.map((p) => ({
      lat: Number(p.lat),
      lng: Number(p.lng),
      capturedAt: p.captured_at,
      accuracyM: p.accuracy_m == null ? null : Number(p.accuracy_m),
      speedKmh: p.speed_kmh == null ? null : Number(p.speed_kmh),
    }));
    const totalKm = calculatePathDistance(points);
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

    const updated = await db.query(
      `UPDATE services
       SET status = 'cancelled',
           ended_at = NOW(),
           total_km = $2,
           route_geojson = $3
       WHERE id = $1
       RETURNING id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number,
                 status, started_at, ended_at, total_km, route_geojson, route_deviation_m, is_off_route`,
      [serviceId, totalKm, JSON.stringify(routeGeoJSON)]
    );

    if (serviceResult.rows[0]?.planned_service_id) {
      await db.query(
        `UPDATE daily_roster
         SET status = 'pending'
         WHERE driver_id = $1
           AND planned_service_id = $2
           AND service_date = CURRENT_DATE`,
        [req.user.id, serviceResult.rows[0].planned_service_id]
      );
    }

    return res.json({
      message: "Viagem anulada com sucesso. Pode selecionar outro servico.",
      service: updated.rows[0],
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao anular viagem." });
  }
});

router.get("/:serviceId", async (req, res) => {
  const { serviceId } = req.params;
  try {
    const result = await db.query(
      `SELECT id, plate_number, service_schedule, line_code, fleet_number, status,
              started_at, ended_at, total_km, route_geojson, route_deviation_m, is_off_route, gtfs_trip_id
       FROM services
       WHERE id = $1 AND driver_id = $2`,
      [serviceId, req.user.id]
    );

    if (result.rowCount === 0) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }

    return res.json(result.rows[0]);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao obter servico." });
  }
});

module.exports = router;
