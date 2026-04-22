const express = require("express");
const db = require("../db");
const authMiddleware = require("../middleware/auth");
const { requireRoles } = require("../middleware/roles");
const { minDistanceToPolylineMeters } = require("../utils/distance");
const { getShapePointsByTripId } = require("../utils/gtfsTripResolve");

const router = express.Router();
let trackerTablesEnsured = false;
let servicePointsQualityColumnsEnsured = false;

async function ensureTrackerTables() {
  if (trackerTablesEnsured) return;
  await db.query(
    `CREATE TABLE IF NOT EXISTS tracker_devices (
      id BIGSERIAL PRIMARY KEY,
      imei VARCHAR(40) UNIQUE NOT NULL,
      fleet_number VARCHAR(50),
      plate_number VARCHAR(50),
      provider VARCHAR(40) NOT NULL DEFAULT 'teltonika',
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_tracker_devices_fleet
     ON tracker_devices(fleet_number)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_tracker_devices_plate
     ON tracker_devices(plate_number)`
  );
  trackerTablesEnsured = true;
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

function trackerAuth(req, res, next) {
  const expectedToken = String(process.env.TELTONIKA_WEBHOOK_TOKEN || "").trim();
  if (!expectedToken) {
    return res.status(503).json({ message: "Integração Teltonika não configurada no servidor." });
  }
  const authHeader = String(req.headers.authorization || "").trim();
  const bearerMatch = authHeader.match(/^Bearer\s+(.+)$/i);
  const presentedToken = bearerMatch ? bearerMatch[1] : String(req.headers["x-integration-token"] || "").trim();
  if (!presentedToken || presentedToken !== expectedToken) {
    return res.status(401).json({ message: "Token de integração inválido." });
  }
  return next();
}

function normalizeImei(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits || null;
}

function normalizeTrackerEvent(raw) {
  const imei = normalizeImei(raw?.imei || raw?.deviceImei || raw?.device_id || raw?.deviceId);
  const lat = Number(raw?.lat ?? raw?.latitude);
  const lng = Number(raw?.lng ?? raw?.lon ?? raw?.longitude);
  if (!imei) return { ok: false, error: "IMEI em falta no evento." };
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return { ok: false, error: "Lat/Lng inválidos no evento." };

  const capturedAtRaw = raw?.capturedAt || raw?.timestamp || raw?.time || null;
  const speedKmhRaw = Number(raw?.speedKmh ?? raw?.speed ?? raw?.speed_kmh);
  const headingDegRaw = Number(raw?.headingDeg ?? raw?.heading ?? raw?.course);
  const accuracyMRaw = Number(raw?.accuracyM ?? raw?.accuracy ?? raw?.hdopMeters);
  const fleetNumberRaw = String(raw?.fleetNumber || raw?.fleet || "").trim();
  const plateNumberRaw = String(raw?.plateNumber || raw?.plate || "").trim();

  return {
    ok: true,
    event: {
      imei,
      lat,
      lng,
      capturedAt: capturedAtRaw || null,
      speedKmh: Number.isFinite(speedKmhRaw) ? speedKmhRaw : null,
      headingDeg: Number.isFinite(headingDegRaw) ? headingDegRaw : null,
      accuracyM: Number.isFinite(accuracyMRaw) ? accuracyMRaw : null,
      fleetNumber: fleetNumberRaw || null,
      plateNumber: plateNumberRaw || null,
      source: "tracker",
    },
  };
}

async function resolveDeviceMapping(event) {
  const byImei = await db.query(
    `SELECT imei, fleet_number, plate_number, is_active
     FROM tracker_devices
     WHERE imei = $1`,
    [event.imei]
  );
  if (byImei.rowCount && !byImei.rows[0].is_active) return null;
  if (byImei.rowCount) return byImei.rows[0];

  if (!event.fleetNumber && !event.plateNumber) return null;
  await db.query(
    `INSERT INTO tracker_devices (imei, fleet_number, plate_number, provider, is_active, updated_at)
     VALUES ($1, $2, $3, 'teltonika', TRUE, NOW())
     ON CONFLICT (imei) DO UPDATE
     SET fleet_number = COALESCE(EXCLUDED.fleet_number, tracker_devices.fleet_number),
         plate_number = COALESCE(EXCLUDED.plate_number, tracker_devices.plate_number),
         updated_at = NOW()`,
    [event.imei, event.fleetNumber, event.plateNumber]
  );
  return {
    imei: event.imei,
    fleet_number: event.fleetNumber,
    plate_number: event.plateNumber,
    is_active: true,
  };
}

async function resolveActiveServiceForDevice(device) {
  const fleetNumber = String(device?.fleet_number || "").trim();
  const plateNumber = String(device?.plate_number || "").trim();
  if (!fleetNumber && !plateNumber) return null;
  const result = await db.query(
    `SELECT id, gtfs_trip_id
     FROM services
     WHERE status = 'in_progress'
       AND (
         ($1 <> '' AND LOWER(TRIM(fleet_number)) = LOWER(TRIM($1)))
         OR ($2 <> '' AND LOWER(TRIM(plate_number)) = LOWER(TRIM($2)))
       )
     ORDER BY started_at DESC
     LIMIT 1`,
    [fleetNumber, plateNumber]
  );
  return result.rows[0] || null;
}

async function getActiveSegment(serviceId) {
  const result = await db.query(
    `SELECT id
     FROM service_segments
     WHERE service_id = $1 AND status = 'in_progress'
     ORDER BY started_at DESC
     LIMIT 1`,
    [serviceId]
  );
  return result.rows[0] || null;
}

async function insertTrackerPoint(serviceId, serviceSegmentId, point) {
  await db.query(
    `INSERT INTO service_points (
       service_id, service_segment_id, lat, lng, captured_at,
       accuracy_m, speed_kmh, heading_deg, point_source
     )
     VALUES ($1, $2, $3, $4, COALESCE($5::timestamptz, NOW()), $6, $7, $8, 'tracker')`,
    [serviceId, serviceSegmentId, point.lat, point.lng, point.capturedAt, point.accuracyM, point.speedKmh, point.headingDeg]
  );
}

async function updateRouteDeviation(serviceId, gtfsTripId, point) {
  if (!gtfsTripId) return;
  const shapePoints = await getShapePointsByTripId(gtfsTripId);
  if (shapePoints.length < 2) return;
  const deviationMeters = minDistanceToPolylineMeters({ lat: point.lat, lng: point.lng }, shapePoints);
  const isOffRoute = deviationMeters > 150;
  await db.query(
    `UPDATE services
     SET route_deviation_m = $2,
         is_off_route = $3
     WHERE id = $1`,
    [serviceId, deviationMeters, isOffRoute]
  );
}

router.get("/teltonika/devices", authMiddleware, requireRoles("supervisor", "admin"), async (_req, res) => {
  try {
    await ensureTrackerTables();
    const result = await db.query(
      `SELECT imei, fleet_number, plate_number, provider, is_active, created_at, updated_at
       FROM tracker_devices
       ORDER BY updated_at DESC`
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar dispositivos tracker." });
  }
});

router.post("/teltonika/devices", authMiddleware, requireRoles("supervisor", "admin"), async (req, res) => {
  const imei = normalizeImei(req.body?.imei);
  const fleetNumber = String(req.body?.fleetNumber || "").trim() || null;
  const plateNumber = String(req.body?.plateNumber || "").trim() || null;
  const isActive = req.body?.isActive !== false;
  if (!imei) return res.status(400).json({ message: "IMEI inválido." });
  if (!fleetNumber && !plateNumber) {
    return res.status(400).json({ message: "Indique fleetNumber ou plateNumber." });
  }
  try {
    await ensureTrackerTables();
    const result = await db.query(
      `INSERT INTO tracker_devices (imei, fleet_number, plate_number, provider, is_active, updated_at)
       VALUES ($1, $2, $3, 'teltonika', $4, NOW())
       ON CONFLICT (imei) DO UPDATE
       SET fleet_number = EXCLUDED.fleet_number,
           plate_number = EXCLUDED.plate_number,
           is_active = EXCLUDED.is_active,
           updated_at = NOW()
       RETURNING imei, fleet_number, plate_number, provider, is_active, created_at, updated_at`,
      [imei, fleetNumber, plateNumber, isActive]
    );
    return res.status(201).json(result.rows[0]);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao guardar dispositivo tracker." });
  }
});

router.post("/teltonika/events", trackerAuth, async (req, res) => {
  const incomingEvents = Array.isArray(req.body?.events) ? req.body.events : [req.body];
  if (!incomingEvents.length) {
    return res.status(400).json({ message: "Sem eventos para processar." });
  }

  try {
    await ensureTrackerTables();
    await ensureServicePointsQualityColumns();

    let accepted = 0;
    const rejected = [];

    for (let i = 0; i < incomingEvents.length; i += 1) {
      const normalized = normalizeTrackerEvent(incomingEvents[i]);
      if (!normalized.ok) {
        rejected.push({ index: i, message: normalized.error });
        continue;
      }

      const mapping = await resolveDeviceMapping(normalized.event);
      if (!mapping) {
        rejected.push({ index: i, message: "Sem mapeamento ativo para o IMEI recebido." });
        continue;
      }

      const service = await resolveActiveServiceForDevice(mapping);
      if (!service) {
        rejected.push({ index: i, message: "Sem serviço em curso para a viatura do tracker." });
        continue;
      }

      const activeSegment = await getActiveSegment(service.id);
      if (!activeSegment) {
        rejected.push({ index: i, message: "Serviço sem segmento ativo." });
        continue;
      }

      await insertTrackerPoint(service.id, activeSegment.id, normalized.event);
      await updateRouteDeviation(service.id, service.gtfs_trip_id, normalized.event);
      accepted += 1;
    }

    return res.status(202).json({
      message: "Eventos Teltonika processados.",
      accepted,
      rejectedCount: rejected.length,
      rejected,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao processar eventos Teltonika." });
  }
});

module.exports = router;
