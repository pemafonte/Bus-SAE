const express = require("express");
const db = require("../db");
const authMiddleware = require("../middleware/auth");
const { calculatePathDistance, minDistanceToPolylineMeters } = require("../utils/distance");

const router = express.Router();
router.use(authMiddleware);

async function ensurePlannedServiceLocationColumns() {
  await db.query(
    `ALTER TABLE planned_services
       ADD COLUMN IF NOT EXISTS start_location VARCHAR(120)`
  );
  await db.query(
    `ALTER TABLE planned_services
       ADD COLUMN IF NOT EXISTS end_location VARCHAR(120)`
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

async function closeActiveSegment(serviceId) {
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
     SET status = 'completed',
         ended_at = NOW(),
         km_segment = $2
     WHERE id = $1`,
    [activeSegment.id, kmSegment]
  );

  return { ...activeSegment, kmSegment };
}

function parseServiceScheduleStartMinutes(serviceSchedule) {
  if (!serviceSchedule) return null;
  const text = String(serviceSchedule).trim();
  const firstChunk = text.includes("-") ? text.split("-")[0] : text;
  const match = firstChunk.match(/(\d{1,2}):(\d{2})/);
  if (!match) return null;
  const hh = Number(match[1]);
  const mm = Number(match[2]);
  if (Number.isNaN(hh) || Number.isNaN(mm)) return null;
  return hh * 60 + mm;
}

/** Intervalo [start,end) em minutos desde meia-noite; suporta janelas que atravessam meia-noite. */
function parseServiceScheduleRangeMinutes(serviceSchedule) {
  const text = String(serviceSchedule || "").trim();
  if (!text) return null;
  const rangeMatch = text.match(/(\d{1,2})\s*:\s*(\d{2})\s*-\s*(\d{1,2})\s*:\s*(\d{2})/);
  if (rangeMatch) {
    let start = Number(rangeMatch[1]) * 60 + Number(rangeMatch[2]);
    let end = Number(rangeMatch[3]) * 60 + Number(rangeMatch[4]);
    if (Number.isNaN(start) || Number.isNaN(end)) return null;
    if (end <= start) end += 24 * 60;
    return { start, end };
  }
  const start = parseServiceScheduleStartMinutes(text);
  if (start == null) return null;
  return { start, end: start + 4 * 60 };
}

function scheduleRangesOverlap(a, b) {
  if (!a || !b) return false;
  return a.start < b.end && b.start < a.end;
}

function parseGtfsTimeToMinutes(gtfsTime) {
  if (!gtfsTime || !String(gtfsTime).includes(":")) return null;
  const [h, m] = String(gtfsTime).split(":");
  const hh = Number(h);
  const mm = Number(m);
  if (Number.isNaN(hh) || Number.isNaN(mm)) return null;
  return hh * 60 + mm;
}

function resolveGtfsRouteCandidates(lineCode) {
  const raw = String(lineCode || "").trim();
  if (!raw) return [];
  const candidates = [raw];
  // Ex: 1014 / 1024 -> linha GTFS base "4"
  if (/^\d{4}$/.test(raw)) {
    candidates.push(raw.slice(-1));
  }
  return [...new Set(candidates)];
}

function resolveDirectionHint(lineCode) {
  const raw = String(lineCode || "").trim();
  // Heuristica para codigos internos de 4 digitos:
  // penultimo digito 1/2 costuma representar sentido (0/1 no GTFS).
  if (/^\d{4}$/.test(raw)) {
    const penultimate = Number(raw.charAt(2));
    if (penultimate === 1) return 0;
    if (penultimate === 2) return 1;
  }
  return null;
}

async function findBestTripForLine(lineCode, serviceSchedule) {
  if (!lineCode) return null;
  const targetMinutes = parseServiceScheduleStartMinutes(serviceSchedule);
  const routeCandidates = resolveGtfsRouteCandidates(lineCode);
  const directionHint = resolveDirectionHint(lineCode);
  const tripsResult = await db.query(
    `SELECT
       t.trip_id,
       t.shape_id,
       t.direction_id,
       MIN(st.departure_time) AS first_departure_time
     FROM gtfs_trips t
     JOIN gtfs_routes r ON r.route_id = t.route_id
     LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
     WHERE r.route_short_name = ANY($1::text[])
       AND ($2::int IS NULL OR t.direction_id = $2)
     GROUP BY t.trip_id, t.shape_id, t.direction_id`,
    [routeCandidates, directionHint]
  );
  if (!tripsResult.rows.length) return null;

  let selected = tripsResult.rows[0];
  let selectedDiff = Number.POSITIVE_INFINITY;

  if (targetMinutes != null) {
    for (const trip of tripsResult.rows) {
      const tripMinutes = parseGtfsTimeToMinutes(trip.first_departure_time);
      if (tripMinutes == null) continue;
      const diff = Math.abs(tripMinutes - targetMinutes);
      if (diff < selectedDiff) {
        selectedDiff = diff;
        selected = trip;
      }
    }
  }

  return selected;
}

async function getShapePointsByTripId(tripId) {
  if (!tripId) return [];
  const shapeResult = await db.query(
    `SELECT s.shape_pt_lat, s.shape_pt_lon
     FROM gtfs_shapes s
     JOIN gtfs_trips t ON t.shape_id = s.shape_id
     WHERE t.trip_id = $1
     ORDER BY s.shape_pt_sequence ASC`,
    [tripId]
  );
  return shapeResult.rows.map((p) => ({
    lat: Number(p.shape_pt_lat),
    lng: Number(p.shape_pt_lon),
  }));
}

async function getStopsByTripId(tripId) {
  if (!tripId) return [];
  const stopsResult = await db.query(
    `SELECT
       st.stop_id,
       s.stop_name,
       s.stop_lat,
       s.stop_lon,
       st.stop_sequence,
       st.arrival_time,
       st.departure_time
     FROM gtfs_stop_times st
     JOIN gtfs_stops s ON s.stop_id = st.stop_id
     WHERE st.trip_id = $1
     ORDER BY st.stop_sequence ASC`,
    [tripId]
  );
  return stopsResult.rows.map((row) => ({
    stopId: row.stop_id,
    stopName: row.stop_name || row.stop_id,
    lat: Number(row.stop_lat),
    lng: Number(row.stop_lon),
    sequence: row.stop_sequence,
    arrivalTime: row.arrival_time || null,
    departureTime: row.departure_time || null,
  }));
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

    const result = await db.query(
      `SELECT
         dr.id AS roster_id,
         dr.service_date,
         dr.status AS roster_status,
         ps.id AS planned_service_id,
         ps.service_code,
         ps.line_code,
         ps.start_location,
         ps.end_location,
         ps.fleet_number,
         ps.plate_number,
         ps.service_schedule
       FROM daily_roster dr
       JOIN planned_services ps ON ps.id = dr.planned_service_id
       WHERE dr.driver_id = $1
         AND dr.service_date = CURRENT_DATE
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

    const result = await db.query(
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
  const { lat, lng, capturedAt } = req.body;

  if (typeof lat !== "number" || typeof lng !== "number") {
    return res.status(400).json({ message: "Latitude e longitude invalidas." });
  }

  try {
    const ownerCheck = await db.query(
      `SELECT id, status, gtfs_trip_id FROM services
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

    await db.query(
      `INSERT INTO service_points (service_id, service_segment_id, lat, lng, captured_at)
       VALUES ($1, $2, $3, $4, COALESCE($5::timestamptz, NOW()))`,
      [serviceId, activeSegment.id, lat, lng, capturedAt || null]
    );

    let deviationMeters = null;
    let isOffRoute = false;
    if (ownerCheck.rows[0].gtfs_trip_id) {
      const shapePoints = await getShapePointsByTripId(ownerCheck.rows[0].gtfs_trip_id);
      if (shapePoints.length >= 2) {
        deviationMeters = minDistanceToPolylineMeters({ lat, lng }, shapePoints);
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

    return res.status(201).json({
      message: "Ponto registado.",
      routeCheck: {
        deviationMeters,
        isOffRoute,
        thresholdMeters: 150,
      },
    });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao guardar ponto GPS." });
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

router.post("/:serviceId/handover", async (req, res) => {
  const { serviceId } = req.params;
  const { toMechanicNumber, toFleetNumber, reason, notes, handoverLocationText } = req.body;

  if (!toMechanicNumber || !reason) {
    return res.status(400).json({ message: "Informe motorista destino e motivo da troca." });
  }

  try {
    const serviceResult = await db.query(
      `SELECT id, driver_id, fleet_number, status
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

  try {
    const serviceResult = await db.query(
      `SELECT id, status, planned_service_id FROM services
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
       SET status = 'completed',
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
         SET status = 'completed'
         WHERE driver_id = $1
           AND planned_service_id = $2
           AND service_date = CURRENT_DATE`,
        [req.user.id, serviceResult.rows[0].planned_service_id]
      );
    }

    return res.json(updated.rows[0]);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao finalizar viagem." });
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
