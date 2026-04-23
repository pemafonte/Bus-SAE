const express = require("express");
const AdmZip = require("adm-zip");
const { parse } = require("csv-parse/sync");
const db = require("../db");
const authMiddleware = require("../middleware/auth");
const { requireRoles } = require("../middleware/roles");

const router = express.Router();
router.use(authMiddleware);
router.use(requireRoles("supervisor", "admin"));

function readZipCsv(zip, fileName) {
  const entry = zip.getEntry(fileName);
  if (!entry) return [];
  const content = entry.getData().toString("utf8");
  return parse(content, { columns: true, skip_empty_lines: true, bom: true, trim: true });
}

function toInt(value) {
  const n = Number.parseInt(value, 10);
  return Number.isNaN(n) ? null : n;
}

function toFloat(value) {
  const n = Number.parseFloat(value);
  return Number.isNaN(n) ? null : n;
}

async function ensureGtfsEditorIndexes() {
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_trips_route_id
     ON gtfs_trips(route_id)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip_seq
     ON gtfs_stop_times(trip_id, stop_sequence)`
  );
}

router.post("/import", async (req, res) => {
  const { fileBase64 } = req.body;
  if (!fileBase64) {
    return res.status(400).json({ message: "Envie fileBase64 com o zip GTFS." });
  }

  try {
    const zipBuffer = Buffer.from(fileBase64, "base64");
    const zip = new AdmZip(zipBuffer);

    const routes = readZipCsv(zip, "routes.txt");
    const trips = readZipCsv(zip, "trips.txt");
    const shapes = readZipCsv(zip, "shapes.txt");
    const stops = readZipCsv(zip, "stops.txt");
    const stopTimes = readZipCsv(zip, "stop_times.txt");

    if (!routes.length || !trips.length || !shapes.length) {
      return res.status(400).json({
        message: "GTFS incompleto. Necessario: routes.txt, trips.txt e shapes.txt.",
      });
    }

    await db.query("BEGIN");
    await db.query("TRUNCATE gtfs_stop_times, gtfs_shapes, gtfs_trips, gtfs_stops, gtfs_routes RESTART IDENTITY CASCADE");

    for (const row of routes) {
      await db.query(
        `INSERT INTO gtfs_routes (route_id, route_short_name, route_long_name)
         VALUES ($1, $2, $3)
         ON CONFLICT (route_id) DO UPDATE
         SET route_short_name = EXCLUDED.route_short_name,
             route_long_name = EXCLUDED.route_long_name`,
        [row.route_id, row.route_short_name || null, row.route_long_name || null]
      );
    }

    for (const row of trips) {
      await db.query(
        `INSERT INTO gtfs_trips (trip_id, route_id, service_id, trip_headsign, direction_id, shape_id)
         VALUES ($1, $2, $3, $4, $5, $6)
         ON CONFLICT (trip_id) DO UPDATE
         SET route_id = EXCLUDED.route_id,
             service_id = EXCLUDED.service_id,
             trip_headsign = EXCLUDED.trip_headsign,
             direction_id = EXCLUDED.direction_id,
             shape_id = EXCLUDED.shape_id`,
        [
          row.trip_id,
          row.route_id,
          row.service_id || null,
          row.trip_headsign || null,
          row.direction_id != null && row.direction_id !== "" ? Number(row.direction_id) : null,
          row.shape_id || null,
        ]
      );
    }

    for (const row of shapes) {
      const lat = toFloat(row.shape_pt_lat);
      const lon = toFloat(row.shape_pt_lon);
      const seq = toInt(row.shape_pt_sequence);
      if (!row.shape_id || lat == null || lon == null || seq == null) continue;
      await db.query(
        `INSERT INTO gtfs_shapes (shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence)
         VALUES ($1, $2, $3, $4)`,
        [row.shape_id, lat, lon, seq]
      );
    }

    for (const row of stops) {
      const lat = toFloat(row.stop_lat);
      const lon = toFloat(row.stop_lon);
      if (!row.stop_id || lat == null || lon == null) continue;
      await db.query(
        `INSERT INTO gtfs_stops (stop_id, stop_name, stop_lat, stop_lon)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (stop_id) DO UPDATE
         SET stop_name = EXCLUDED.stop_name,
             stop_lat = EXCLUDED.stop_lat,
             stop_lon = EXCLUDED.stop_lon`,
        [row.stop_id, row.stop_name || null, lat, lon]
      );
    }

    for (const row of stopTimes) {
      const seq = toInt(row.stop_sequence);
      if (!row.trip_id || seq == null) continue;
      await db.query(
        `INSERT INTO gtfs_stop_times (trip_id, arrival_time, departure_time, stop_id, stop_sequence)
         VALUES ($1, $2, $3, $4, $5)`,
        [row.trip_id, row.arrival_time || null, row.departure_time || null, row.stop_id || null, seq]
      );
    }

    await db.query("COMMIT");
    return res.json({
      message: "GTFS importado com sucesso.",
      counts: {
        routes: routes.length,
        trips: trips.length,
        shapes: shapes.length,
        stops: stops.length,
        stopTimes: stopTimes.length,
      },
    });
  } catch (error) {
    await db.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao importar GTFS.", error: error.message });
  }
});

router.get("/status", async (_req, res) => {
  try {
    const counts = await db.query(
      `SELECT
         (SELECT COUNT(*)::int FROM gtfs_routes) AS routes,
         (SELECT COUNT(*)::int FROM gtfs_trips) AS trips,
         (SELECT COUNT(*)::int FROM gtfs_shapes) AS shapes,
         (SELECT COUNT(*)::int FROM gtfs_stops) AS stops,
         (SELECT COUNT(*)::int FROM gtfs_stop_times) AS stop_times`
    );
    return res.json({
      persisted: true,
      counts: counts.rows[0],
    });
  } catch (error) {
    return res.status(500).json({ message: "Erro ao consultar estado GTFS." });
  }
});

router.get("/editor/lines", async (_req, res) => {
  try {
    await ensureGtfsEditorIndexes();
    const result = await db.query(
      `SELECT
         r.route_id,
         COALESCE(r.route_short_name, '') AS route_short_name,
         COALESCE(r.route_long_name, '') AS route_long_name,
         COUNT(DISTINCT t.trip_id)::int AS trips_count,
         COUNT(DISTINCT st.stop_id)::int AS stops_count
       FROM gtfs_routes r
       LEFT JOIN gtfs_trips t ON t.route_id = r.route_id
       LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
       GROUP BY r.route_id, r.route_short_name, r.route_long_name
       ORDER BY NULLIF(r.route_short_name, '') ASC NULLS LAST, r.route_id ASC`
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar linhas GTFS." });
  }
});

router.get("/editor/trips", async (req, res) => {
  const routeId = String(req.query.routeId || "").trim();
  if (!routeId) return res.status(400).json({ message: "Indique routeId." });
  try {
    const result = await db.query(
      `SELECT
         t.trip_id,
         t.route_id,
         COALESCE(t.trip_headsign, '') AS trip_headsign,
         t.direction_id,
         t.service_id,
         COUNT(st.stop_id)::int AS stops_count
       FROM gtfs_trips t
       LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
       WHERE t.route_id = $1
       GROUP BY t.trip_id, t.route_id, t.trip_headsign, t.direction_id, t.service_id
       ORDER BY t.trip_id ASC`,
      [routeId]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar viagens GTFS." });
  }
});

router.get("/editor/trip-stops", async (req, res) => {
  const tripId = String(req.query.tripId || "").trim();
  if (!tripId) return res.status(400).json({ message: "Indique tripId." });
  try {
    const tripRes = await db.query(
      `SELECT trip_id, route_id, trip_headsign, direction_id, service_id
       FROM gtfs_trips
       WHERE trip_id = $1`,
      [tripId]
    );
    if (!tripRes.rowCount) {
      return res.status(404).json({ message: "Trip GTFS não encontrada." });
    }
    const stopsRes = await db.query(
      `SELECT
         st.stop_sequence,
         st.arrival_time,
         st.departure_time,
         st.stop_id,
         s.stop_name,
         s.stop_lat,
         s.stop_lon
       FROM gtfs_stop_times st
       LEFT JOIN gtfs_stops s ON s.stop_id = st.stop_id
       WHERE st.trip_id = $1
       ORDER BY st.stop_sequence ASC`,
      [tripId]
    );
    return res.json({ trip: tripRes.rows[0], stops: stopsRes.rows });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar paragens da viagem GTFS." });
  }
});

router.post("/editor/trip-stops", async (req, res) => {
  const tripId = String(req.body?.tripId || "").trim();
  const stopIdRaw = String(req.body?.stopId || "").trim();
  const stopName = String(req.body?.stopName || "").trim();
  const stopLat = toFloat(req.body?.stopLat);
  const stopLon = toFloat(req.body?.stopLon);
  const arrivalTime = String(req.body?.arrivalTime || "").trim() || null;
  const departureTime = String(req.body?.departureTime || "").trim() || null;
  const requestedSequence = toInt(req.body?.stopSequence);

  if (!tripId) return res.status(400).json({ message: "Indique tripId." });
  if (!stopIdRaw && (!stopName || stopLat == null || stopLon == null)) {
    return res.status(400).json({ message: "Indique stopId existente ou stopName+stopLat+stopLon para criar nova paragem." });
  }

  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    const tripRes = await client.query(`SELECT trip_id FROM gtfs_trips WHERE trip_id = $1 FOR UPDATE`, [tripId]);
    if (!tripRes.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Trip GTFS não encontrada." });
    }

    let stopId = stopIdRaw;
    if (stopId) {
      const stopRes = await client.query(`SELECT stop_id FROM gtfs_stops WHERE stop_id = $1`, [stopId]);
      if (!stopRes.rowCount) {
        await client.query("ROLLBACK");
        return res.status(404).json({ message: "stopId não existe em gtfs_stops." });
      }
    } else {
      stopId = `custom_${Date.now()}`;
      await client.query(
        `INSERT INTO gtfs_stops (stop_id, stop_name, stop_lat, stop_lon)
         VALUES ($1, $2, $3, $4)`,
        [stopId, stopName, stopLat, stopLon]
      );
    }

    const maxSeqRes = await client.query(
      `SELECT COALESCE(MAX(stop_sequence), 0)::int AS max_seq
       FROM gtfs_stop_times
       WHERE trip_id = $1`,
      [tripId]
    );
    const maxSeq = Number(maxSeqRes.rows[0]?.max_seq || 0);
    let seq = Number.isFinite(requestedSequence) && requestedSequence > 0 ? requestedSequence : maxSeq + 1;
    if (seq < 1) seq = 1;
    if (seq <= maxSeq) {
      await client.query(
        `UPDATE gtfs_stop_times
         SET stop_sequence = stop_sequence + 1
         WHERE trip_id = $1
           AND stop_sequence >= $2`,
        [tripId, seq]
      );
    }

    await client.query(
      `INSERT INTO gtfs_stop_times (trip_id, arrival_time, departure_time, stop_id, stop_sequence)
       VALUES ($1, $2, $3, $4, $5)`,
      [tripId, arrivalTime, departureTime, stopId, seq]
    );

    await client.query("COMMIT");
    return res.json({ message: "Paragem adicionada à trip GTFS.", tripId, stopId, stopSequence: seq });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao adicionar paragem na trip GTFS." });
  } finally {
    client.release();
  }
});

router.delete("/editor/trip-stops", async (req, res) => {
  const tripId = String(req.query.tripId || req.body?.tripId || "").trim();
  const stopSequence = toInt(req.query.stopSequence || req.body?.stopSequence);
  if (!tripId || !Number.isFinite(stopSequence) || stopSequence <= 0) {
    return res.status(400).json({ message: "Indique tripId e stopSequence válidos." });
  }

  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    const delRes = await client.query(
      `DELETE FROM gtfs_stop_times
       WHERE trip_id = $1
         AND stop_sequence = $2
       RETURNING trip_id`,
      [tripId, stopSequence]
    );
    if (!delRes.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Paragem não encontrada para remoção." });
    }

    await client.query(
      `WITH ordered AS (
         SELECT ctid, ROW_NUMBER() OVER (ORDER BY stop_sequence ASC) AS new_seq
         FROM gtfs_stop_times
         WHERE trip_id = $1
       )
       UPDATE gtfs_stop_times st
       SET stop_sequence = o.new_seq
       FROM ordered o
       WHERE st.ctid = o.ctid`,
      [tripId]
    );

    await client.query("COMMIT");
    return res.json({ message: "Paragem removida e sequência normalizada.", tripId, removedStopSequence: stopSequence });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao remover paragem da trip GTFS." });
  } finally {
    client.release();
  }
});

module.exports = router;
