const express = require("express");
const db = require("../db");
const authMiddleware = require("../middleware/auth");
const { requireRoles } = require("../middleware/roles");
const XLSX = require("xlsx");
const { calculatePathDistance, minDistanceToPolylineMeters, haversineMeters } = require("../utils/distance");
const { findBestTripForLine, getShapePointsByTripId, getStopsByTripId } = require("../utils/gtfsTripResolve");
const { resolveTotalKmWithPlannedFallback } = require("../utils/plannedKmFallback");
const { getRosterServiceDateForExecution } = require("../utils/rosterServiceDate");

const router = express.Router();
let trackerTablesEnsured = false;
let servicePointsQualityColumnsEnsured = false;
let deadheadTablesEnsured = false;
let serviceOdometerColumnsEnsured = false;
let odometerLogsTableEnsured = false;
let serviceStopProgressEnsured = false;

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
      install_odometer_km NUMERIC(12,1),
      current_odometer_km NUMERIC(12,1),
      current_odometer_updated_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `ALTER TABLE tracker_devices
       ADD COLUMN IF NOT EXISTS install_odometer_km NUMERIC(12,1)`
  );
  await db.query(
    `ALTER TABLE tracker_devices
       ADD COLUMN IF NOT EXISTS current_odometer_km NUMERIC(12,1)`
  );
  await db.query(
    `ALTER TABLE tracker_devices
       ADD COLUMN IF NOT EXISTS current_odometer_updated_at TIMESTAMPTZ`
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

async function ensureDeadheadTables() {
  if (deadheadTablesEnsured) return;
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
  await db.query(
    `CREATE TABLE IF NOT EXISTS deadhead_points (
      id BIGSERIAL PRIMARY KEY,
      movement_id BIGINT NOT NULL REFERENCES deadhead_movements(id) ON DELETE CASCADE,
      lat NUMERIC(10,7) NOT NULL,
      lng NUMERIC(10,7) NOT NULL,
      captured_at TIMESTAMPTZ NOT NULL,
      speed_kmh NUMERIC(8,2),
      heading_deg NUMERIC(6,2),
      accuracy_m NUMERIC(8,2)
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_deadhead_movements_open
     ON deadhead_movements(imei, open_state, updated_at DESC)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_deadhead_movements_started
     ON deadhead_movements(started_at DESC)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_deadhead_points_movement
     ON deadhead_points(movement_id, captured_at ASC)`
  );
  deadheadTablesEnsured = true;
}

async function ensureServiceOdometerColumns() {
  if (serviceOdometerColumnsEnsured) return;
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
       ADD COLUMN IF NOT EXISTS close_mode VARCHAR(30)`
  );
  serviceOdometerColumnsEnsured = true;
}

async function ensureOdometerLogsTable() {
  if (odometerLogsTableEnsured) return;
  await db.query(
    `CREATE TABLE IF NOT EXISTS vehicle_odometer_logs (
      id BIGSERIAL PRIMARY KEY,
      imei VARCHAR(40),
      fleet_number VARCHAR(50),
      plate_number VARCHAR(50),
      odometer_km NUMERIC(12,1) NOT NULL,
      captured_at TIMESTAMPTZ NOT NULL,
      source VARCHAR(30) NOT NULL DEFAULT 'tracker',
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_vehicle_odometer_logs_captured
     ON vehicle_odometer_logs(captured_at DESC)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_vehicle_odometer_logs_fleet
     ON vehicle_odometer_logs(fleet_number, captured_at DESC)`
  );
  odometerLogsTableEnsured = true;
}

async function ensureServiceStopProgressTable() {
  if (serviceStopProgressEnsured) return;
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
  serviceStopProgressEnsured = true;
}

function csvEscape(value) {
  const raw = value == null ? "" : String(value);
  return `"${raw.replace(/"/g, '""')}"`;
}

function buildDateBounds(rawFrom, rawTo) {
  const from = String(rawFrom || "").trim();
  const to = String(rawTo || "").trim();
  const fromTs = from ? `${from}T00:00:00` : "";
  const toTs = to ? `${to}T23:59:59.999` : "";
  return { fromTs, toTs };
}

async function registerVehicleOdometerLog(mapping, event) {
  const odometerKm = Number(event?.odometerKm);
  if (!Number.isFinite(odometerKm) || odometerKm < 0) return;
  await ensureTrackerTables();
  await ensureOdometerLogsTable();
  const imei = String(mapping?.imei || event?.imei || "").trim() || null;
  const fleetNumber = String(mapping?.fleet_number || event?.fleetNumber || "").trim() || null;
  const plateNumber = String(mapping?.plate_number || event?.plateNumber || "").trim() || null;
  const capturedAt = parseCapturedAtIso(event?.capturedAt).toISOString();
  await db.query(
    `INSERT INTO vehicle_odometer_logs (imei, fleet_number, plate_number, odometer_km, captured_at, source)
     VALUES ($1, $2, $3, $4, $5, 'tracker')`,
    [imei, fleetNumber, plateNumber, odometerKm, capturedAt]
  );
  if (imei) {
    await db.query(
      `UPDATE tracker_devices
       SET current_odometer_km = $2,
           current_odometer_updated_at = NOW(),
           updated_at = NOW()
       WHERE imei = $1`,
      [imei, odometerKm]
    );
  }
}

function parseCapturedAtIso(value) {
  const parsed = value ? new Date(value) : new Date();
  return Number.isFinite(parsed.getTime()) ? parsed : new Date();
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

async function closeOpenDeadheadMovementByDevice(mapping, endedAt) {
  const fleetNumber = String(mapping?.fleet_number || "").trim();
  const plateNumber = String(mapping?.plate_number || "").trim();
  const imei = String(mapping?.imei || "").trim();
  if (!imei && !fleetNumber && !plateNumber) return;
  await db.query(
    `UPDATE deadhead_movements
     SET open_state = FALSE,
         ended_at = GREATEST(ended_at, $2::timestamptz),
         updated_at = NOW()
     WHERE open_state = TRUE
       AND (
         ($1 <> '' AND imei = $1)
         OR ($3 <> '' AND LOWER(TRIM(COALESCE(fleet_number, ''))) = LOWER(TRIM($3)))
         OR ($4 <> '' AND LOWER(TRIM(COALESCE(plate_number, ''))) = LOWER(TRIM($4)))
       )`,
    [imei, endedAt.toISOString(), fleetNumber, plateNumber]
  );
}

async function upsertDeadheadMovementPoint(mapping, point) {
  await ensureDeadheadTables();
  const imei = String(mapping?.imei || "").trim();
  if (!imei) return;
  const fleetNumber = String(mapping?.fleet_number || "").trim() || null;
  const plateNumber = String(mapping?.plate_number || "").trim() || null;
  const capturedAt = parseCapturedAtIso(point.capturedAt);
  const splitGapMs = 20 * 60 * 1000;
  const movingSpeedThreshold = 2.5;
  const hasMovingEvidence = Number(point.speedKmh) > movingSpeedThreshold;
  if (!hasMovingEvidence) {
    return;
  }

  const openRes = await db.query(
    `SELECT id, ended_at, end_lat, end_lng
     FROM deadhead_movements
     WHERE imei = $1
       AND open_state = TRUE
     ORDER BY updated_at DESC
     LIMIT 1`,
    [imei]
  );

  let movement = openRes.rows[0] || null;
  if (movement) {
    const lastAt = movement.ended_at ? new Date(movement.ended_at) : null;
    const gapMs = lastAt && Number.isFinite(lastAt.getTime()) ? capturedAt.getTime() - lastAt.getTime() : 0;
    if (gapMs > splitGapMs) {
      await db.query(
        `UPDATE deadhead_movements
         SET open_state = FALSE,
             updated_at = NOW()
         WHERE id = $1`,
        [movement.id]
      );
      movement = null;
    }
  }

  if (!movement) {
    const created = await db.query(
      `INSERT INTO deadhead_movements (
         imei, fleet_number, plate_number,
         started_at, ended_at, start_lat, start_lng, end_lat, end_lng,
         total_km, points_count, open_state, updated_at
       )
       VALUES ($1, $2, $3, $4, $4, $5, $6, $5, $6, 0, 0, TRUE, NOW())
       RETURNING id, ended_at, end_lat, end_lng`,
      [imei, fleetNumber, plateNumber, capturedAt.toISOString(), point.lat, point.lng]
    );
    movement = created.rows[0];
  }

  await db.query(
    `INSERT INTO deadhead_points (
       movement_id, lat, lng, captured_at, speed_kmh, heading_deg, accuracy_m
     )
     VALUES ($1, $2, $3, $4, $5, $6, $7)`,
    [
      movement.id,
      point.lat,
      point.lng,
      capturedAt.toISOString(),
      Number.isFinite(Number(point.speedKmh)) ? Number(point.speedKmh) : null,
      Number.isFinite(Number(point.headingDeg)) ? Number(point.headingDeg) : null,
      Number.isFinite(Number(point.accuracyM)) ? Number(point.accuracyM) : null,
    ]
  );

  const prevLat = Number(movement.end_lat);
  const prevLng = Number(movement.end_lng);
  const incrementalKm =
    Number.isFinite(prevLat) && Number.isFinite(prevLng)
      ? Number((haversineMeters(prevLat, prevLng, Number(point.lat), Number(point.lng)) / 1000).toFixed(3))
      : 0;

  await db.query(
    `UPDATE deadhead_movements
     SET ended_at = $2,
         end_lat = $3,
         end_lng = $4,
         total_km = GREATEST(0, COALESCE(total_km, 0) + $5),
         points_count = COALESCE(points_count, 0) + 1,
         fleet_number = COALESCE($6, fleet_number),
         plate_number = COALESCE($7, plate_number),
         updated_at = NOW()
     WHERE id = $1`,
    [movement.id, capturedAt.toISOString(), point.lat, point.lng, incrementalKm, fleetNumber, plateNumber]
  );
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
  const odometerKmRaw = Number(
    raw?.odometerKm ??
      raw?.odometer_km ??
      raw?.odometer ??
      raw?.totalOdometerKm ??
      raw?.mileageKm ??
      raw?.canOdometerKm
  );

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
      odometerKm: Number.isFinite(odometerKmRaw) && odometerKmRaw >= 0 ? odometerKmRaw : null,
      source: "tracker",
    },
  };
}

function toSignedInt32(value) {
  return value > 0x7fffffff ? value - 0x100000000 : value;
}

function decodeCodec8AvlRecords(payloadBuffer) {
  const packet = Buffer.isBuffer(payloadBuffer) ? payloadBuffer : Buffer.from(payloadBuffer || []);
  if (packet.length < 16) throw new Error("Pacote Codec8 demasiado curto.");

  const preamble = packet.readUInt32BE(0);
  if (preamble !== 0) throw new Error("Preamble inválido no pacote Codec8.");

  const dataFieldLength = packet.readUInt32BE(4);
  const expectedTotalLength = 8 + dataFieldLength + 4;
  if (packet.length < expectedTotalLength) {
    throw new Error("Pacote Codec8 incompleto para o tamanho declarado.");
  }

  const data = packet.subarray(8, 8 + dataFieldLength);
  if (data.length < 3) throw new Error("Campo de dados Codec8 inválido.");

  let offset = 0;
  const codecId = data.readUInt8(offset);
  offset += 1;
  if (codecId !== 0x08) throw new Error("Codec não suportado. Esperado Codec8 (0x08).");

  const recordCount1 = data.readUInt8(offset);
  offset += 1;
  const records = [];

  for (let i = 0; i < recordCount1; i += 1) {
    if (offset + 8 + 1 + 15 > data.length) {
      throw new Error("Record Codec8 truncado.");
    }

    const timestampMs = Number(data.readBigUInt64BE(offset));
    offset += 8;
    const priority = data.readUInt8(offset);
    offset += 1;

    const lngRaw = toSignedInt32(data.readUInt32BE(offset));
    offset += 4;
    const latRaw = toSignedInt32(data.readUInt32BE(offset));
    offset += 4;
    const altitude = data.readInt16BE(offset);
    offset += 2;
    const angle = data.readUInt16BE(offset);
    offset += 2;
    const satellites = data.readUInt8(offset);
    offset += 1;
    const speed = data.readUInt16BE(offset);
    offset += 2;

    if (offset + 2 > data.length) throw new Error("Bloco IO Codec8 truncado.");
    const eventIoId = data.readUInt8(offset);
    offset += 1;
    const totalIo = data.readUInt8(offset);
    offset += 1;

    const ioValues = {};
    const parseIoGroup = (valueSize) => {
      if (offset + 1 > data.length) throw new Error("Contador IO Codec8 truncado.");
      const count = data.readUInt8(offset);
      offset += 1;
      for (let j = 0; j < count; j += 1) {
        if (offset + 1 + valueSize > data.length) throw new Error("Elemento IO Codec8 truncado.");
        const id = data.readUInt8(offset);
        offset += 1;
        let value;
        if (valueSize === 1) value = data.readUInt8(offset);
        else if (valueSize === 2) value = data.readUInt16BE(offset);
        else if (valueSize === 4) value = data.readUInt32BE(offset);
        else value = Number(data.readBigUInt64BE(offset));
        offset += valueSize;
        ioValues[id] = value;
      }
      return count;
    };

    const n1 = parseIoGroup(1);
    const n2 = parseIoGroup(2);
    const n4 = parseIoGroup(4);
    const n8 = parseIoGroup(8);
    const groupedTotal = n1 + n2 + n4 + n8;
    if (groupedTotal !== totalIo) {
      // Alguns gateways enviam total inconsistente; mantemos parsing tolerante.
    }

    records.push({
      timestampMs,
      priority,
      lat: latRaw / 10000000,
      lng: lngRaw / 10000000,
      altitude,
      angle,
      satellites,
      speedKmh: speed,
      eventIoId,
      ioValues,
    });
  }

  if (offset + 1 > data.length) throw new Error("Contador final de records em falta.");
  const recordCount2 = data.readUInt8(offset);
  offset += 1;
  if (recordCount1 !== recordCount2) {
    throw new Error("Número de records Codec8 inconsistente.");
  }
  if (offset !== data.length) {
    throw new Error("Dados extra inesperados no campo Codec8.");
  }

  return records;
}

function parseCodec8Input(req) {
  const contentType = String(req.headers["content-type"] || "").toLowerCase();
  const imei = normalizeImei(req.query?.imei || req.headers["x-device-imei"] || req.body?.imei);
  if (!imei) {
    return { ok: false, error: "IMEI obrigatório (query ?imei=..., header x-device-imei ou body.imei)." };
  }

  let payloadBuffer = null;
  if (Buffer.isBuffer(req.body) && req.body.length) {
    payloadBuffer = req.body;
  } else if (typeof req.body?.payloadHex === "string") {
    const cleanHex = req.body.payloadHex.replace(/\s+/g, "");
    if (!/^[0-9a-fA-F]+$/.test(cleanHex) || cleanHex.length % 2 !== 0) {
      return { ok: false, error: "payloadHex inválido." };
    }
    payloadBuffer = Buffer.from(cleanHex, "hex");
  } else if (typeof req.body?.payloadBase64 === "string") {
    try {
      payloadBuffer = Buffer.from(req.body.payloadBase64, "base64");
    } catch (_error) {
      return { ok: false, error: "payloadBase64 inválido." };
    }
  } else if (typeof req.body === "string" && req.body.trim()) {
    const compact = req.body.trim();
    if (contentType.includes("text/plain") && /^[0-9a-fA-F]+$/.test(compact) && compact.length % 2 === 0) {
      payloadBuffer = Buffer.from(compact, "hex");
    }
  }

  if (!payloadBuffer || !payloadBuffer.length) {
    return {
      ok: false,
      error: "Payload Codec8 em falta. Envie binary (application/octet-stream), payloadHex ou payloadBase64.",
    };
  }

  let records;
  try {
    records = decodeCodec8AvlRecords(payloadBuffer);
  } catch (error) {
    return { ok: false, error: `Codec8 inválido: ${error.message}` };
  }

  const events = records.map((record) => ({
    imei,
    lat: record.lat,
    lng: record.lng,
    capturedAt: Number.isFinite(record.timestampMs) ? new Date(record.timestampMs).toISOString() : null,
    speedKmh: record.speedKmh,
    headingDeg: record.angle,
    satellites: record.satellites,
    priority: record.priority,
    altitudeM: record.altitude,
    codec: "codec8",
    source: "tracker",
  }));

  return { ok: true, events, recordsCount: records.length };
}

async function processTeltonikaEvents(incomingEvents) {
  await ensureTrackerTables();
  await ensureServicePointsQualityColumns();
  await ensureDeadheadTables();
  await ensureOdometerLogsTable();
  await ensureServiceOdometerColumns();

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
    await registerVehicleOdometerLog(mapping, normalized.event);

    const service = await resolveActiveServiceForDevice(mapping);
    if (!service) {
      await upsertDeadheadMovementPoint(mapping, normalized.event);
      accepted += 1;
      continue;
    }

    await closeOpenDeadheadMovementByDevice(mapping, parseCapturedAtIso(normalized.event.capturedAt));

    const activeSegment = await getActiveSegment(service.id);
    if (!activeSegment) {
      rejected.push({ index: i, message: "Serviço sem segmento ativo." });
      continue;
    }

    await insertTrackerPoint(service.id, activeSegment.id, normalized.event);
    await updateRouteDeviation(service.id, service.gtfs_trip_id, normalized.event);
    await maybeAutoCompleteServiceAtLastStopFromTracker(service, normalized.event);
    accepted += 1;
  }

  return {
    accepted,
    rejectedCount: rejected.length,
    rejected,
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
  if (byImei.rowCount) {
    const current = byImei.rows[0];
    const incomingFleet = String(event.fleetNumber || "").trim() || null;
    const incomingPlate = String(event.plateNumber || "").trim() || null;
    const currentFleet = String(current.fleet_number || "").trim() || null;
    const currentPlate = String(current.plate_number || "").trim() || null;

    const fleetChanged = incomingFleet && incomingFleet !== currentFleet;
    const plateChanged = incomingPlate && incomingPlate !== currentPlate;

    if (fleetChanged || plateChanged) {
      const updated = await db.query(
        `UPDATE tracker_devices
         SET fleet_number = COALESCE($2, fleet_number),
             plate_number = COALESCE($3, plate_number),
             updated_at = NOW()
         WHERE imei = $1
         RETURNING imei, fleet_number, plate_number, is_active`,
        [event.imei, incomingFleet, incomingPlate]
      );
      return updated.rows[0] || current;
    }

    return current;
  }

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
  const result = fleetNumber
    ? await db.query(
        `SELECT id, gtfs_trip_id, line_code, service_schedule, planned_service_id, driver_id, started_at, status
         FROM services
         WHERE status = 'in_progress'
           AND LOWER(TRIM(fleet_number)) = LOWER(TRIM($1))
         ORDER BY started_at DESC
         LIMIT 1`,
        [fleetNumber]
      )
    : await db.query(
        `SELECT id, gtfs_trip_id, line_code, service_schedule, planned_service_id, driver_id, started_at, status
         FROM services
         WHERE status = 'in_progress'
           AND LOWER(TRIM(plate_number)) = LOWER(TRIM($1))
         ORDER BY started_at DESC
         LIMIT 1`,
        [plateNumber]
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

async function maybeAutoCompleteServiceAtLastStopFromTracker(service, point) {
  if (!service?.id || service?.status !== "in_progress") return false;
  await ensureServiceStopProgressTable();
  let tripId = service.gtfs_trip_id || null;
  if (!tripId && service.line_code && service.service_schedule) {
    const best = await findBestTripForLine(service.line_code, service.service_schedule);
    tripId = best?.trip_id || null;
    if (tripId) {
      await db.query(`UPDATE services SET gtfs_trip_id = $1 WHERE id = $2 AND gtfs_trip_id IS NULL`, [tripId, service.id]);
    }
  }
  if (!tripId) return false;
  const stops = (await getStopsByTripId(tripId))
    .map((s) => ({
      stop_id: s.stop_id,
      stop_sequence: Number(s.stop_sequence),
      lat: Number(s.lat),
      lng: Number(s.lng),
    }))
    .filter((s) => Number.isFinite(s.stop_sequence) && Number.isFinite(s.lat) && Number.isFinite(s.lng))
    .sort((a, b) => a.stop_sequence - b.stop_sequence);
  if (!stops.length) return false;
  const lastStop = stops[stops.length - 1];
  const distanceToLast = geoDistanceMeters(point.lat, point.lng, lastStop.lat, lastStop.lng);
  if (distanceToLast == null || distanceToLast > 60) return false;

  await db.query(
    `INSERT INTO service_stop_progress (service_id, last_passed_stop_id, last_passed_stop_sequence, last_passed_at, updated_at)
     VALUES ($1, $2, $3, NOW(), NOW())
     ON CONFLICT (service_id) DO UPDATE
       SET last_passed_stop_id = EXCLUDED.last_passed_stop_id,
           last_passed_stop_sequence = EXCLUDED.last_passed_stop_sequence,
           last_passed_at = EXCLUDED.last_passed_at,
           updated_at = NOW()`,
    [service.id, lastStop.stop_id || null, Number(lastStop.stop_sequence)]
  );

  const activeSegment = await getActiveSegment(service.id);
  if (activeSegment) {
    const segPointsResult = await db.query(
      `SELECT lat, lng, captured_at
       FROM service_points
       WHERE service_segment_id = $1
       ORDER BY captured_at ASC`,
      [activeSegment.id]
    );
    const segPoints = segPointsResult.rows.map((p) => ({ lat: Number(p.lat), lng: Number(p.lng), capturedAt: p.captured_at }));
    const kmSegment = calculatePathDistance(segPoints);
    await db.query(
      `UPDATE service_segments
       SET status = 'completed',
           ended_at = NOW(),
           km_segment = $2
       WHERE id = $1`,
      [activeSegment.id, kmSegment]
    );
  }

  const pointsResult = await db.query(
    `SELECT lat, lng, captured_at, accuracy_m, speed_kmh
     FROM service_points
     WHERE service_id = $1
     ORDER BY captured_at ASC`,
    [service.id]
  );
  const points = pointsResult.rows.map((p) => ({
    lat: Number(p.lat),
    lng: Number(p.lng),
    capturedAt: p.captured_at,
    accuracyM: p.accuracy_m == null ? null : Number(p.accuracy_m),
    speedKmh: p.speed_kmh == null ? null : Number(p.speed_kmh),
  }));
  const totalKm = await resolveTotalKmWithPlannedFallback(calculatePathDistance(points), service.planned_service_id);
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
    [service.id, totalKm, JSON.stringify(routeGeoJSON)]
  );
  if (!updated.rowCount) return false;
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
  return true;
}

router.get("/teltonika/devices", authMiddleware, requireRoles("supervisor", "admin"), async (_req, res) => {
  try {
    await ensureTrackerTables();
    await ensureOdometerLogsTable();
    const result = await db.query(
      `SELECT imei, fleet_number, plate_number, provider, is_active,
              install_odometer_km, current_odometer_km, current_odometer_updated_at,
              created_at, updated_at
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
  const installOdometerKmRaw = req.body?.installOdometerKm;
  const currentOdometerKmRaw = req.body?.currentOdometerKm;
  const installOdometerKm =
    installOdometerKmRaw == null || installOdometerKmRaw === "" ? null : Number(installOdometerKmRaw);
  const currentOdometerKm =
    currentOdometerKmRaw == null || currentOdometerKmRaw === "" ? null : Number(currentOdometerKmRaw);
  if (!imei) return res.status(400).json({ message: "IMEI inválido." });
  if (!fleetNumber) {
    return res.status(400).json({ message: "fleetNumber é obrigatório para integração por viatura/frota." });
  }
  if (installOdometerKm != null && (!Number.isFinite(installOdometerKm) || installOdometerKm < 0)) {
    return res.status(400).json({ message: "installOdometerKm inválido." });
  }
  if (currentOdometerKm != null && (!Number.isFinite(currentOdometerKm) || currentOdometerKm < 0)) {
    return res.status(400).json({ message: "currentOdometerKm inválido." });
  }
  try {
    await ensureTrackerTables();
    await ensureOdometerLogsTable();
    const result = await db.query(
      `INSERT INTO tracker_devices (
         imei, fleet_number, plate_number, provider, is_active,
         install_odometer_km, current_odometer_km, current_odometer_updated_at, updated_at
       )
       VALUES ($1, $2, $3, 'teltonika', $4, $5, $6, CASE WHEN $6 IS NULL THEN NULL ELSE NOW() END, NOW())
       ON CONFLICT (imei) DO UPDATE
       SET fleet_number = EXCLUDED.fleet_number,
           plate_number = EXCLUDED.plate_number,
           is_active = EXCLUDED.is_active,
           install_odometer_km = COALESCE(EXCLUDED.install_odometer_km, tracker_devices.install_odometer_km),
           current_odometer_km = COALESCE(EXCLUDED.current_odometer_km, tracker_devices.current_odometer_km),
           current_odometer_updated_at = CASE
             WHEN EXCLUDED.current_odometer_km IS NULL THEN tracker_devices.current_odometer_updated_at
             ELSE NOW()
           END,
           updated_at = NOW()
       RETURNING imei, fleet_number, plate_number, provider, is_active,
                 install_odometer_km, current_odometer_km, current_odometer_updated_at,
                 created_at, updated_at`,
      [imei, fleetNumber, plateNumber, isActive, installOdometerKm, currentOdometerKm]
    );
    const saved = result.rows[0];
    if (Number.isFinite(currentOdometerKm)) {
      await db.query(
        `INSERT INTO vehicle_odometer_logs (imei, fleet_number, plate_number, odometer_km, captured_at, source)
         VALUES ($1, $2, $3, $4, NOW(), 'manual')`,
        [saved.imei || null, saved.fleet_number || null, saved.plate_number || null, currentOdometerKm]
      );
    }
    return res.status(201).json(saved);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao guardar dispositivo tracker." });
  }
});

router.patch("/teltonika/devices/:imei/odometer", authMiddleware, requireRoles("supervisor", "admin"), async (req, res) => {
  const imei = normalizeImei(req.params.imei);
  const installRaw = req.body?.installOdometerKm;
  const currentRaw = req.body?.currentOdometerKm;
  const installOdometerKm = installRaw == null || installRaw === "" ? null : Number(installRaw);
  const currentOdometerKm = currentRaw == null || currentRaw === "" ? null : Number(currentRaw);
  if (!imei) return res.status(400).json({ message: "IMEI inválido." });
  if (installOdometerKm != null && (!Number.isFinite(installOdometerKm) || installOdometerKm < 0)) {
    return res.status(400).json({ message: "installOdometerKm inválido." });
  }
  if (currentOdometerKm != null && (!Number.isFinite(currentOdometerKm) || currentOdometerKm < 0)) {
    return res.status(400).json({ message: "currentOdometerKm inválido." });
  }
  if (installOdometerKm == null && currentOdometerKm == null) {
    return res.status(400).json({ message: "Indique installOdometerKm e/ou currentOdometerKm." });
  }
  try {
    await ensureTrackerTables();
    const result = await db.query(
      `UPDATE tracker_devices
       SET install_odometer_km = COALESCE($2, install_odometer_km),
           current_odometer_km = COALESCE($3, current_odometer_km),
           current_odometer_updated_at = CASE WHEN $3 IS NULL THEN current_odometer_updated_at ELSE NOW() END,
           updated_at = NOW()
       WHERE imei = $1
       RETURNING imei, fleet_number, plate_number, provider, is_active,
                 install_odometer_km, current_odometer_km, current_odometer_updated_at,
                 created_at, updated_at`,
      [imei, installOdometerKm, currentOdometerKm]
    );
    if (!result.rowCount) {
      return res.status(404).json({ message: "Dispositivo não encontrado." });
    }
    const saved = result.rows[0];
    if (Number.isFinite(currentOdometerKm)) {
      await db.query(
        `INSERT INTO vehicle_odometer_logs (imei, fleet_number, plate_number, odometer_km, captured_at, source)
         VALUES ($1, $2, $3, $4, NOW(), 'manual')`,
        [saved.imei || null, saved.fleet_number || null, saved.plate_number || null, currentOdometerKm]
      );
    }
    return res.json(saved);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar quilometragem da viatura." });
  }
});

router.get("/deadhead-movements", authMiddleware, requireRoles("supervisor", "admin"), async (req, res) => {
  const { fromTs, toTs } = buildDateBounds(req.query?.from, req.query?.to);
  const fleetNumber = String(req.query?.fleetNumber || "").trim();
  try {
    await ensureDeadheadTables();
    const result = await db.query(
      `SELECT id, imei, fleet_number, plate_number, started_at, ended_at,
              start_lat, start_lng, end_lat, end_lng, total_km, points_count
       FROM deadhead_movements
       WHERE ($1 = '' OR started_at >= $1::timestamptz)
         AND ($2 = '' OR ended_at <= $2::timestamptz)
         AND ($3 = '' OR LOWER(TRIM(COALESCE(fleet_number, ''))) = LOWER(TRIM($3)))
       ORDER BY started_at DESC
       LIMIT 500`,
      [fromTs, toTs, fleetNumber]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar movimentos em vazio." });
  }
});

router.get("/deadhead-movements/export.csv", authMiddleware, requireRoles("supervisor", "admin"), async (req, res) => {
  const { fromTs, toTs } = buildDateBounds(req.query?.from, req.query?.to);
  const fleetNumber = String(req.query?.fleetNumber || "").trim();
  try {
    await ensureDeadheadTables();
    const result = await db.query(
      `SELECT id, imei, fleet_number, plate_number, started_at, ended_at, total_km, points_count
       FROM deadhead_movements
       WHERE ($1 = '' OR started_at >= $1::timestamptz)
         AND ($2 = '' OR ended_at <= $2::timestamptz)
         AND ($3 = '' OR LOWER(TRIM(COALESCE(fleet_number, ''))) = LOWER(TRIM($3)))
       ORDER BY started_at DESC`,
      [fromTs, toTs, fleetNumber]
    );
    const header = ["id", "imei", "frota", "matricula", "inicio", "fim", "km", "pontos"];
    const rows = result.rows.map((r) =>
      [r.id, r.imei, r.fleet_number, r.plate_number, r.started_at, r.ended_at, r.total_km, r.points_count].map(csvEscape).join(",")
    );
    const csv = [header.map(csvEscape).join(","), ...rows].join("\n");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=movimentos_vazio.csv");
    return res.status(200).send(`\uFEFF${csv}`);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar vazios CSV." });
  }
});

router.get("/deadhead-movements/export.xlsx", authMiddleware, requireRoles("supervisor", "admin"), async (req, res) => {
  const { fromTs, toTs } = buildDateBounds(req.query?.from, req.query?.to);
  const fleetNumber = String(req.query?.fleetNumber || "").trim();
  try {
    await ensureDeadheadTables();
    const result = await db.query(
      `SELECT id, imei, fleet_number, plate_number, started_at, ended_at, total_km, points_count
       FROM deadhead_movements
       WHERE ($1 = '' OR started_at >= $1::timestamptz)
         AND ($2 = '' OR ended_at <= $2::timestamptz)
         AND ($3 = '' OR LOWER(TRIM(COALESCE(fleet_number, ''))) = LOWER(TRIM($3)))
       ORDER BY started_at DESC`,
      [fromTs, toTs, fleetNumber]
    );
    const rows = result.rows.map((r) => ({
      id: r.id,
      imei: r.imei || "",
      frota: r.fleet_number || "",
      matricula: r.plate_number || "",
      inicio: r.started_at || "",
      fim: r.ended_at || "",
      km: r.total_km == null ? "" : Number(r.total_km),
      pontos: r.points_count == null ? "" : Number(r.points_count),
    }));
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, "vazios");
    const buffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", "attachment; filename=movimentos_vazio.xlsx");
    return res.status(200).send(buffer);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar vazios Excel." });
  }
});

router.get("/odometer-reconciliation/daily", authMiddleware, requireRoles("supervisor", "admin"), async (req, res) => {
  const date = String(req.query?.date || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(date)) {
    return res.status(400).json({ message: "Data inválida. Use YYYY-MM-DD." });
  }
  try {
    await ensureDeadheadTables();
    await ensureServiceOdometerColumns();
    await ensureOdometerLogsTable();
    const result = await db.query(
      `WITH day AS (
         SELECT $1::date AS d
       ),
       load_km AS (
         SELECT
           COALESCE(NULLIF(TRIM(s.fleet_number), ''), 'SEM_FROTA') AS fleet_key,
           MIN(NULLIF(TRIM(s.plate_number), '')) AS plate_number,
           COALESCE(SUM(COALESCE(s.total_km, 0)), 0)::numeric(12,3) AS app_km_load,
           MIN(s.vehicle_odometer_start_km) AS odometer_min_start,
           MAX(s.vehicle_odometer_end_km) AS odometer_max_end
         FROM services s
         CROSS JOIN day
         WHERE LOWER(TRIM(COALESCE(s.status::text, ''))) = 'completed'
           AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d
         GROUP BY COALESCE(NULLIF(TRIM(s.fleet_number), ''), 'SEM_FROTA')
       ),
       deadhead_km AS (
         SELECT
           COALESCE(NULLIF(TRIM(dm.fleet_number), ''), 'SEM_FROTA') AS fleet_key,
           MIN(NULLIF(TRIM(dm.plate_number), '')) AS plate_number,
           COALESCE(SUM(COALESCE(dm.total_km, 0)), 0)::numeric(12,3) AS app_km_deadhead
         FROM deadhead_movements dm
         CROSS JOIN day
         WHERE ((dm.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d
         GROUP BY COALESCE(NULLIF(TRIM(dm.fleet_number), ''), 'SEM_FROTA')
       ),
       odometer_logs_day AS (
         SELECT
           COALESCE(NULLIF(TRIM(v.fleet_number), ''), 'SEM_FROTA') AS fleet_key,
           MIN(NULLIF(TRIM(v.plate_number), '')) AS plate_number,
           MIN(v.captured_at) AS first_captured_at,
           MAX(v.captured_at) AS last_captured_at
         FROM vehicle_odometer_logs v
         CROSS JOIN day
         WHERE ((v.captured_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d
         GROUP BY COALESCE(NULLIF(TRIM(v.fleet_number), ''), 'SEM_FROTA')
       ),
       odometer_bounds AS (
         SELECT
           o.fleet_key,
           o.plate_number,
           first_log.odometer_km AS odometer_first_km,
           last_log.odometer_km AS odometer_last_km
         FROM odometer_logs_day o
         LEFT JOIN LATERAL (
           SELECT odometer_km
           FROM vehicle_odometer_logs
           WHERE COALESCE(NULLIF(TRIM(fleet_number), ''), 'SEM_FROTA') = o.fleet_key
             AND captured_at = o.first_captured_at
           ORDER BY id ASC
           LIMIT 1
         ) first_log ON TRUE
         LEFT JOIN LATERAL (
           SELECT odometer_km
           FROM vehicle_odometer_logs
           WHERE COALESCE(NULLIF(TRIM(fleet_number), ''), 'SEM_FROTA') = o.fleet_key
             AND captured_at = o.last_captured_at
           ORDER BY id DESC
           LIMIT 1
         ) last_log ON TRUE
       ),
       keys AS (
         SELECT fleet_key FROM load_km
         UNION
         SELECT fleet_key FROM deadhead_km
         UNION
         SELECT fleet_key FROM odometer_bounds
       )
       SELECT
         day.d::text AS report_date,
         k.fleet_key AS fleet_number,
         COALESCE(l.plate_number, d.plate_number, o.plate_number, '') AS plate_number,
         COALESCE(l.app_km_load, 0)::numeric(12,3) AS app_km_load,
         COALESCE(d.app_km_deadhead, 0)::numeric(12,3) AS app_km_deadhead,
         (COALESCE(l.app_km_load, 0) + COALESCE(d.app_km_deadhead, 0))::numeric(12,3) AS app_km_total,
         CASE
           WHEN o.odometer_first_km IS NOT NULL AND o.odometer_last_km IS NOT NULL
           THEN (o.odometer_last_km - o.odometer_first_km)::numeric(12,3)
           WHEN l.odometer_min_start IS NOT NULL AND l.odometer_max_end IS NOT NULL
           THEN (l.odometer_max_end - l.odometer_min_start)::numeric(12,3)
           ELSE NULL
         END AS odometer_km_day,
         CASE
           WHEN o.odometer_first_km IS NOT NULL AND o.odometer_last_km IS NOT NULL
           THEN ((o.odometer_last_km - o.odometer_first_km) - (COALESCE(l.app_km_load, 0) + COALESCE(d.app_km_deadhead, 0)))::numeric(12,3)
           WHEN l.odometer_min_start IS NOT NULL AND l.odometer_max_end IS NOT NULL
           THEN ((l.odometer_max_end - l.odometer_min_start) - (COALESCE(l.app_km_load, 0) + COALESCE(d.app_km_deadhead, 0)))::numeric(12,3)
           ELSE NULL
         END AS odometer_vs_app_diff_km
       FROM keys k
       CROSS JOIN day
       LEFT JOIN load_km l ON l.fleet_key = k.fleet_key
       LEFT JOIN deadhead_km d ON d.fleet_key = k.fleet_key
       LEFT JOIN odometer_bounds o ON o.fleet_key = k.fleet_key
       ORDER BY k.fleet_key`,
      [date]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao gerar relatório de conciliação." });
  }
});

router.get("/deadhead-movements/:movementId", authMiddleware, requireRoles("supervisor", "admin"), async (req, res) => {
  const movementId = Number(req.params.movementId);
  if (!Number.isFinite(movementId)) {
    return res.status(400).json({ message: "movementId inválido." });
  }
  try {
    await ensureDeadheadTables();
    const movementRes = await db.query(
      `SELECT id, imei, fleet_number, plate_number, started_at, ended_at,
              start_lat, start_lng, end_lat, end_lng, total_km, points_count
       FROM deadhead_movements
       WHERE id = $1
       LIMIT 1`,
      [movementId]
    );
    if (!movementRes.rowCount) {
      return res.status(404).json({ message: "Movimento em vazio não encontrado." });
    }
    const pointsRes = await db.query(
      `SELECT lat, lng, captured_at, speed_kmh, heading_deg, accuracy_m
       FROM deadhead_points
       WHERE movement_id = $1
       ORDER BY captured_at ASC`,
      [movementId]
    );
    const coordinates = pointsRes.rows.map((p) => [Number(p.lng), Number(p.lat)]);
    return res.json({
      ...movementRes.rows[0],
      route_geojson: {
        type: "Feature",
        geometry: {
          type: "LineString",
          coordinates,
        },
        properties: {
          points: coordinates.length,
        },
      },
      points: pointsRes.rows,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao obter detalhe do vazio." });
  }
});

router.post("/teltonika/events", trackerAuth, async (req, res) => {
  const incomingEvents = Array.isArray(req.body?.events) ? req.body.events : [req.body];
  if (!incomingEvents.length) {
    return res.status(400).json({ message: "Sem eventos para processar." });
  }

  try {
    const result = await processTeltonikaEvents(incomingEvents);

    return res.status(202).json({
      message: "Eventos Teltonika processados.",
      accepted: result.accepted,
      rejectedCount: result.rejectedCount,
      rejected: result.rejected,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao processar eventos Teltonika." });
  }
});

router.post(
  "/teltonika/codec8",
  trackerAuth,
  express.raw({ type: ["application/octet-stream", "application/teltonika-codec8"], limit: "1mb" }),
  async (req, res) => {
    const parsed = parseCodec8Input(req);
    if (!parsed.ok) return res.status(400).json({ message: parsed.error });
    if (!parsed.events.length) {
      return res.status(400).json({ message: "Pacote Codec8 sem records." });
    }

    try {
      const result = await processTeltonikaEvents(parsed.events);
      return res.status(202).json({
        message: "Pacote Codec8 processado.",
        records: parsed.recordsCount,
        accepted: result.accepted,
        rejectedCount: result.rejectedCount,
        rejected: result.rejected,
      });
    } catch (_error) {
      return res.status(500).json({ message: "Erro ao processar pacote Codec8." });
    }
  }
);

module.exports = {
  router,
  normalizeImei,
  decodeCodec8AvlRecords,
  processTeltonikaEvents,
};
