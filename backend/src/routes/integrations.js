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
        `SELECT id, gtfs_trip_id
         FROM services
         WHERE status = 'in_progress'
           AND LOWER(TRIM(fleet_number)) = LOWER(TRIM($1))
         ORDER BY started_at DESC
         LIMIT 1`,
        [fleetNumber]
      )
    : await db.query(
        `SELECT id, gtfs_trip_id
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
  if (!fleetNumber) {
    return res.status(400).json({ message: "fleetNumber é obrigatório para integração por viatura/frota." });
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
