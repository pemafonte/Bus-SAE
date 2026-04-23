const express = require("express");
const db = require("../db");
const GtfsRealtimeBindings = require("gtfs-realtime-bindings");
const { findBestTripForLine, getStopsByTripId } = require("../utils/gtfsTripResolve");

const router = express.Router();

let stopProgressTableAvailable = null;
let conflictAlertsTableAvailable = null;

function gtfsRtNowTs() {
  return Math.floor(Date.now() / 1000);
}

async function tableExists(tableName) {
  const result = await db.query(
    `SELECT 1
     FROM information_schema.tables
     WHERE table_schema = 'public'
       AND table_name = $1
     LIMIT 1`,
    [tableName]
  );
  return result.rowCount > 0;
}

function parseGtfsTimeToParts(gtfsTime) {
  const text = String(gtfsTime || "").trim();
  const match = text.match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;
  const hh = Number(match[1]);
  const mm = Number(match[2]);
  const ss = Number(match[3] || 0);
  if (!Number.isFinite(hh) || !Number.isFinite(mm) || !Number.isFinite(ss)) return null;
  return { hh, mm, ss };
}

function gtfsStopTimeToEpochSeconds(gtfsTime, startedAt) {
  const parts = parseGtfsTimeToParts(gtfsTime);
  if (!parts || !startedAt) return null;
  const base = new Date(startedAt);
  if (Number.isNaN(base.getTime())) return null;
  const d = new Date(base);
  d.setHours(0, 0, 0, 0);
  const dayOffset = Math.floor(parts.hh / 24);
  const hh = parts.hh % 24;
  d.setDate(d.getDate() + dayOffset);
  d.setHours(hh, parts.mm, parts.ss, 0);
  return Math.floor(d.getTime() / 1000);
}

function maybeRequireGtfsRtToken(req, res) {
  const expected = String(process.env.GTFS_RT_TOKEN || "").trim();
  if (!expected) return true;
  const auth = String(req.headers.authorization || "").trim();
  const bearer = auth.toLowerCase().startsWith("bearer ") ? auth.slice(7).trim() : "";
  const q = String(req.query.token || "").trim();
  if (bearer === expected || q === expected) return true;
  res.status(401).json({ message: "Token GTFS-RT inválido." });
  return false;
}

function toFeedEntity(id, payloadFieldName, payloadValue) {
  const entity = new GtfsRealtimeBindings.transit_realtime.FeedEntity();
  entity.id = String(id);
  entity[payloadFieldName] = payloadValue;
  return entity;
}

function finalizeFeed(entities) {
  const feed = new GtfsRealtimeBindings.transit_realtime.FeedMessage();
  feed.header = new GtfsRealtimeBindings.transit_realtime.FeedHeader();
  feed.header.gtfsRealtimeVersion = "2.0";
  feed.header.incrementality = GtfsRealtimeBindings.transit_realtime.FeedHeader.Incrementality.FULL_DATASET;
  feed.header.timestamp = gtfsRtNowTs();
  feed.entity = entities;
  return feed;
}

function respondFeed(req, res, feed) {
  const wantsJson = req.path.endsWith(".json") || String(req.query.format || "").toLowerCase() === "json";
  if (wantsJson) {
    return res.json(
      GtfsRealtimeBindings.transit_realtime.FeedMessage.toObject(feed, {
        longs: Number,
        enums: String,
      })
    );
  }
  const buffer = GtfsRealtimeBindings.transit_realtime.FeedMessage.encode(feed).finish();
  res.setHeader("Content-Type", "application/x-protobuf");
  return res.status(200).send(Buffer.from(buffer));
}

async function loadLiveServicesBase() {
  const result = await db.query(
    `SELECT
       s.id,
       s.driver_id,
       s.gtfs_trip_id,
       s.line_code,
       s.fleet_number,
       s.plate_number,
       s.service_schedule,
       s.status,
       s.started_at,
       u.name AS driver_name,
       lp.lat,
       lp.lng,
       lp.captured_at,
       lp.speed_kmh,
       lp.heading_deg,
       gt.route_id
     FROM services s
     JOIN users u ON u.id = s.driver_id
     LEFT JOIN LATERAL (
       SELECT sp.lat, sp.lng, sp.captured_at, sp.speed_kmh, sp.heading_deg
       FROM service_points sp
       WHERE sp.service_id = s.id
       ORDER BY sp.captured_at DESC
       LIMIT 1
     ) lp ON true
     LEFT JOIN gtfs_trips gt ON gt.trip_id = s.gtfs_trip_id
     WHERE s.status IN ('in_progress', 'awaiting_handover')
     ORDER BY s.started_at DESC`
  );
  return result.rows;
}

async function handleVehiclePositions(req, res) {
  if (!maybeRequireGtfsRtToken(req, res)) return;
  try {
    const rows = await loadLiveServicesBase();
    const entities = [];
    for (const row of rows) {
      if (row.lat == null || row.lng == null) continue;
      if (!row.gtfs_trip_id) {
        const best = await findBestTripForLine(row.line_code, row.service_schedule);
        if (best?.trip_id) row.gtfs_trip_id = best.trip_id;
      }
      const vp = new GtfsRealtimeBindings.transit_realtime.VehiclePosition();
      vp.trip = new GtfsRealtimeBindings.transit_realtime.TripDescriptor();
      vp.trip.tripId = String(row.gtfs_trip_id || "");
      if (row.route_id) vp.trip.routeId = String(row.route_id);
      vp.vehicle = new GtfsRealtimeBindings.transit_realtime.VehicleDescriptor();
      vp.vehicle.id = String(row.fleet_number || row.plate_number || row.id);
      vp.vehicle.label = String(row.fleet_number || row.plate_number || row.id);
      if (row.plate_number) vp.vehicle.licensePlate = String(row.plate_number);
      vp.position = new GtfsRealtimeBindings.transit_realtime.Position();
      vp.position.latitude = Number(row.lat);
      vp.position.longitude = Number(row.lng);
      if (Number.isFinite(Number(row.heading_deg))) vp.position.bearing = Number(row.heading_deg);
      if (Number.isFinite(Number(row.speed_kmh))) vp.position.speed = Number(row.speed_kmh) / 3.6;
      vp.currentStatus =
        String(row.status) === "awaiting_handover"
          ? GtfsRealtimeBindings.transit_realtime.VehiclePosition.VehicleStopStatus.STOPPED_AT
          : GtfsRealtimeBindings.transit_realtime.VehiclePosition.VehicleStopStatus.IN_TRANSIT_TO;
      vp.timestamp = row.captured_at ? Math.floor(new Date(row.captured_at).getTime() / 1000) : gtfsRtNowTs();
      entities.push(toFeedEntity(`vp_${row.id}`, "vehicle", vp));
    }
    return respondFeed(req, res, finalizeFeed(entities));
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao gerar GTFS-RT VehiclePositions." });
  }
}

async function handleTripUpdates(req, res) {
  if (!maybeRequireGtfsRtToken(req, res)) return;
  try {
    if (stopProgressTableAvailable == null) {
      stopProgressTableAvailable = await tableExists("service_stop_progress");
    }
    const rows = await loadLiveServicesBase();
    const entities = [];
    for (const row of rows) {
      let tripId = row.gtfs_trip_id;
      if (!tripId) {
        const best = await findBestTripForLine(row.line_code, row.service_schedule);
        tripId = best?.trip_id || null;
      }
      if (!tripId) continue;
      const stops = await getStopsByTripId(tripId);
      if (!Array.isArray(stops) || !stops.length) continue;
      let lastPassedSeq = 0;
      if (stopProgressTableAvailable) {
        const prog = await db.query(
          `SELECT last_passed_stop_sequence
           FROM service_stop_progress
           WHERE service_id = $1`,
          [row.id]
        );
        if (prog.rowCount > 0) {
          lastPassedSeq = Number(prog.rows[0].last_passed_stop_sequence || 0);
        }
      }
      const nextStop = stops.find((s) => Number(s.sequence) > Number(lastPassedSeq || 0)) || null;
      const nextScheduled = nextStop ? nextStop.departureTime || nextStop.arrivalTime : null;
      const schedTs = gtfsStopTimeToEpochSeconds(nextScheduled, row.started_at);
      const nowTs = gtfsRtNowTs();
      const delaySecs = schedTs ? nowTs - schedTs : null;

      const tu = new GtfsRealtimeBindings.transit_realtime.TripUpdate();
      tu.trip = new GtfsRealtimeBindings.transit_realtime.TripDescriptor();
      tu.trip.tripId = String(tripId);
      if (row.route_id) tu.trip.routeId = String(row.route_id);
      if (row.status === "awaiting_handover") {
        tu.trip.scheduleRelationship = GtfsRealtimeBindings.transit_realtime.TripDescriptor.ScheduleRelationship.SCHEDULED;
      }
      tu.vehicle = new GtfsRealtimeBindings.transit_realtime.VehicleDescriptor();
      tu.vehicle.id = String(row.fleet_number || row.plate_number || row.id);
      tu.vehicle.label = String(row.fleet_number || row.plate_number || row.id);
      if (delaySecs != null) tu.delay = delaySecs;
      tu.timestamp = row.captured_at ? Math.floor(new Date(row.captured_at).getTime() / 1000) : nowTs;

      if (nextStop) {
        const stu = new GtfsRealtimeBindings.transit_realtime.TripUpdate.StopTimeUpdate();
        stu.stopSequence = Number(nextStop.sequence);
        if (nextStop.stopId) stu.stopId = String(nextStop.stopId);
        if (delaySecs != null) {
          stu.arrival = new GtfsRealtimeBindings.transit_realtime.TripUpdate.StopTimeEvent();
          stu.arrival.delay = delaySecs;
          stu.departure = new GtfsRealtimeBindings.transit_realtime.TripUpdate.StopTimeEvent();
          stu.departure.delay = delaySecs;
        }
        tu.stopTimeUpdate = [stu];
      }
      entities.push(toFeedEntity(`tu_${row.id}`, "tripUpdate", tu));
    }
    return respondFeed(req, res, finalizeFeed(entities));
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao gerar GTFS-RT TripUpdates." });
  }
}

async function handleServiceAlerts(req, res) {
  if (!maybeRequireGtfsRtToken(req, res)) return;
  try {
    if (conflictAlertsTableAvailable == null) {
      conflictAlertsTableAvailable = await tableExists("supervisor_conflict_alerts");
    }
    const entities = [];

    const handoverRes = await db.query(
      `SELECT h.id, h.service_id, h.reason, h.notes, h.created_at
       FROM service_handover_events h
       WHERE LOWER(TRIM(h.status::text)) = 'pending'
       ORDER BY h.created_at DESC
       LIMIT 100`
    );
    for (const row of handoverRes.rows) {
      const alert = new GtfsRealtimeBindings.transit_realtime.Alert();
      alert.cause = GtfsRealtimeBindings.transit_realtime.Alert.Cause.UNKNOWN_CAUSE;
      alert.effect = GtfsRealtimeBindings.transit_realtime.Alert.Effect.DETOUR;
      alert.headerText = new GtfsRealtimeBindings.transit_realtime.TranslatedString();
      alert.headerText.translation = [
        { text: `Transferência pendente no serviço ${row.service_id}`, language: "pt" },
      ];
      alert.descriptionText = new GtfsRealtimeBindings.transit_realtime.TranslatedString();
      alert.descriptionText.translation = [
        {
          text: `Motivo: ${row.reason || "-"}${row.notes ? ` | Notas: ${row.notes}` : ""}`,
          language: "pt",
        },
      ];
      const informed = new GtfsRealtimeBindings.transit_realtime.EntitySelector();
      informed.trip = new GtfsRealtimeBindings.transit_realtime.TripDescriptor();
      informed.trip.tripId = String(row.service_id);
      alert.informedEntity = [informed];
      entities.push(toFeedEntity(`alert_handover_${row.id}`, "alert", alert));
    }

    if (conflictAlertsTableAvailable) {
      const conflictRes = await db.query(
        `SELECT id, notes, line_code, created_at
         FROM supervisor_conflict_alerts
         ORDER BY created_at DESC
         LIMIT 100`
      );
      for (const row of conflictRes.rows) {
        const alert = new GtfsRealtimeBindings.transit_realtime.Alert();
        alert.cause = GtfsRealtimeBindings.transit_realtime.Alert.Cause.UNKNOWN_CAUSE;
        alert.effect = GtfsRealtimeBindings.transit_realtime.Alert.Effect.SIGNIFICANT_DELAYS;
        alert.headerText = new GtfsRealtimeBindings.transit_realtime.TranslatedString();
        alert.headerText.translation = [
          { text: `Conflito operacional ${row.line_code ? `na linha ${row.line_code}` : ""}`.trim(), language: "pt" },
        ];
        alert.descriptionText = new GtfsRealtimeBindings.transit_realtime.TranslatedString();
        alert.descriptionText.translation = [{ text: String(row.notes || "Conflito operacional registado."), language: "pt" }];
        entities.push(toFeedEntity(`alert_conflict_${row.id}`, "alert", alert));
      }
    }
    return respondFeed(req, res, finalizeFeed(entities));
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao gerar GTFS-RT ServiceAlerts." });
  }
}

router.get("/vehicle-positions.pb", handleVehiclePositions);
router.get("/vehicle-positions.json", handleVehiclePositions);
router.get("/trip-updates.pb", handleTripUpdates);
router.get("/trip-updates.json", handleTripUpdates);
router.get("/service-alerts.pb", handleServiceAlerts);
router.get("/service-alerts.json", handleServiceAlerts);

module.exports = router;

