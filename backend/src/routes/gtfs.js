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

module.exports = router;
