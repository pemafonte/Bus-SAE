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

function normalizeFeedKey(value) {
  return String(value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "")
    .slice(0, 80);
}

function scopedId(feedKey, rawId) {
  const cleaned = String(rawId || "").trim();
  if (!cleaned) return null;
  return `${feedKey}::${cleaned}`;
}

function parseGtfsDate(raw) {
  const text = String(raw || "").trim();
  if (!/^\d{8}$/.test(text)) return null;
  return `${text.slice(0, 4)}-${text.slice(4, 6)}-${text.slice(6, 8)}`;
}

function formatGtfsDate(raw) {
  const text = String(raw || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return "";
  return `${text.slice(0, 4)}${text.slice(5, 7)}${text.slice(8, 10)}`;
}

function csvEscape(value) {
  const raw = value == null ? "" : String(value);
  if (!/[",\n]/.test(raw)) return raw;
  return `"${raw.replace(/"/g, '""')}"`;
}

function toCsv(rows, columns) {
  const header = columns.map((c) => csvEscape(c)).join(",");
  const body = rows.map((row) => columns.map((c) => csvEscape(row[c])).join(",")).join("\n");
  return `${header}\n${body}\n`;
}

async function ensureGtfsMultiFeedInfra() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS gtfs_feeds (
      feed_key VARCHAR(80) PRIMARY KEY,
      feed_name VARCHAR(160) NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      source_filename VARCHAR(255),
      routes_count INT NOT NULL DEFAULT 0,
      trips_count INT NOT NULL DEFAULT 0,
      shapes_count INT NOT NULL DEFAULT 0,
      stops_count INT NOT NULL DEFAULT 0,
      stop_times_count INT NOT NULL DEFAULT 0,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_imported_at TIMESTAMPTZ
    )`
  );
  await db.query(`ALTER TABLE gtfs_routes ADD COLUMN IF NOT EXISTS feed_key VARCHAR(80) NOT NULL DEFAULT 'default'`);
  await db.query(`ALTER TABLE gtfs_trips ADD COLUMN IF NOT EXISTS feed_key VARCHAR(80) NOT NULL DEFAULT 'default'`);
  await db.query(`ALTER TABLE gtfs_shapes ADD COLUMN IF NOT EXISTS feed_key VARCHAR(80) NOT NULL DEFAULT 'default'`);
  await db.query(`ALTER TABLE gtfs_stops ADD COLUMN IF NOT EXISTS feed_key VARCHAR(80) NOT NULL DEFAULT 'default'`);
  await db.query(`ALTER TABLE gtfs_stop_times ADD COLUMN IF NOT EXISTS feed_key VARCHAR(80) NOT NULL DEFAULT 'default'`);
  await db.query(
    `INSERT INTO gtfs_feeds (feed_key, feed_name, is_active, source_filename, last_imported_at)
     VALUES ('default', 'Default GTFS', TRUE, 'legacy', NOW())
     ON CONFLICT (feed_key) DO NOTHING`
  );
  await db.query(`ALTER TABLE gtfs_feeds ADD COLUMN IF NOT EXISTS gtfs_effective_from DATE`);
  await db.query(`ALTER TABLE gtfs_feeds ADD COLUMN IF NOT EXISTS calendar_effective_from DATE`);
  await db.query(
    `CREATE TABLE IF NOT EXISTS gtfs_calendars (
      feed_key VARCHAR(80) NOT NULL DEFAULT 'default',
      service_id VARCHAR(120) NOT NULL,
      monday INT NOT NULL DEFAULT 0,
      tuesday INT NOT NULL DEFAULT 0,
      wednesday INT NOT NULL DEFAULT 0,
      thursday INT NOT NULL DEFAULT 0,
      friday INT NOT NULL DEFAULT 0,
      saturday INT NOT NULL DEFAULT 0,
      sunday INT NOT NULL DEFAULT 0,
      start_date DATE NOT NULL,
      end_date DATE NOT NULL,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      PRIMARY KEY (feed_key, service_id),
      FOREIGN KEY (feed_key) REFERENCES gtfs_feeds(feed_key) ON DELETE CASCADE
    )`
  );
  await db.query(
    `CREATE TABLE IF NOT EXISTS gtfs_calendar_dates (
      id BIGSERIAL PRIMARY KEY,
      feed_key VARCHAR(80) NOT NULL DEFAULT 'default',
      service_id VARCHAR(120) NOT NULL,
      calendar_date DATE NOT NULL,
      exception_type INT NOT NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (feed_key, service_id, calendar_date),
      FOREIGN KEY (feed_key) REFERENCES gtfs_feeds(feed_key) ON DELETE CASCADE
    )`
  );
}

async function ensureGtfsEditorIndexes() {
  await ensureGtfsMultiFeedInfra();
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_trips_route_id
     ON gtfs_trips(route_id)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip_seq
     ON gtfs_stop_times(trip_id, stop_sequence)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_routes_feed
     ON gtfs_routes(feed_key)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_trips_feed
     ON gtfs_trips(feed_key)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_feeds_active
     ON gtfs_feeds(is_active, updated_at DESC)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_calendars_feed_service
     ON gtfs_calendars(feed_key, service_id)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_calendar_dates_feed_service
     ON gtfs_calendar_dates(feed_key, service_id, calendar_date)`
  );
}

router.post("/import", async (req, res) => {
  const { fileBase64 } = req.body || {};
  const feedNameRaw = String(req.body?.feedName || "").trim();
  const feedKeyRaw = String(req.body?.feedKey || "").trim();
  const sourceFilename = String(req.body?.fileName || "").trim() || null;
  const replaceFeed = req.body?.replaceFeed !== false;
  if (!fileBase64) {
    return res.status(400).json({ message: "Envie fileBase64 com o zip GTFS." });
  }

  const fallbackKey = sourceFilename ? sourceFilename.replace(/\.[^.]+$/, "") : "";
  const feedKey = normalizeFeedKey(feedKeyRaw || fallbackKey || "default");
  if (!feedKey) {
    return res.status(400).json({ message: "Indique um feedKey valido (letras, numeros, _ ou -)." });
  }
  const feedName = feedNameRaw || feedKey;

  try {
    await ensureGtfsEditorIndexes();
    const zipBuffer = Buffer.from(fileBase64, "base64");
    const zip = new AdmZip(zipBuffer);
    const routes = readZipCsv(zip, "routes.txt");
    const trips = readZipCsv(zip, "trips.txt");
    const shapes = readZipCsv(zip, "shapes.txt");
    const stops = readZipCsv(zip, "stops.txt");
    const stopTimes = readZipCsv(zip, "stop_times.txt");
    const calendars = readZipCsv(zip, "calendar.txt");
    const calendarDates = readZipCsv(zip, "calendar_dates.txt");

    if (!routes.length || !trips.length || !shapes.length) {
      return res.status(400).json({
        message: "GTFS incompleto. Necessario: routes.txt, trips.txt e shapes.txt.",
      });
    }

    await db.query("BEGIN");
    await db.query(
      `INSERT INTO gtfs_feeds (feed_key, feed_name, is_active, source_filename, updated_at, last_imported_at)
       VALUES ($1, $2, TRUE, $3, NOW(), NOW())
       ON CONFLICT (feed_key) DO UPDATE
       SET feed_name = EXCLUDED.feed_name,
           source_filename = EXCLUDED.source_filename,
           updated_at = NOW(),
           last_imported_at = NOW()`,
      [feedKey, feedName, sourceFilename]
    );

    if (replaceFeed) {
      await db.query(`DELETE FROM gtfs_stop_times WHERE feed_key = $1`, [feedKey]);
      await db.query(`DELETE FROM gtfs_calendar_dates WHERE feed_key = $1`, [feedKey]);
      await db.query(`DELETE FROM gtfs_calendars WHERE feed_key = $1`, [feedKey]);
      await db.query(`DELETE FROM gtfs_shapes WHERE feed_key = $1`, [feedKey]);
      await db.query(`DELETE FROM gtfs_trips WHERE feed_key = $1`, [feedKey]);
      await db.query(`DELETE FROM gtfs_stops WHERE feed_key = $1`, [feedKey]);
      await db.query(`DELETE FROM gtfs_routes WHERE feed_key = $1`, [feedKey]);
    }

    let insertedRoutes = 0;
    let insertedTrips = 0;
    let insertedShapes = 0;
    let insertedStops = 0;
    let insertedStopTimes = 0;
    let insertedCalendars = 0;
    let insertedCalendarDates = 0;

    for (const row of routes) {
      const routeId = scopedId(feedKey, row.route_id);
      if (!routeId) continue;
      await db.query(
        `INSERT INTO gtfs_routes (route_id, feed_key, route_short_name, route_long_name)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (route_id) DO UPDATE
         SET feed_key = EXCLUDED.feed_key,
             route_short_name = EXCLUDED.route_short_name,
             route_long_name = EXCLUDED.route_long_name`,
        [routeId, feedKey, row.route_short_name || null, row.route_long_name || null]
      );
      insertedRoutes += 1;
    }

    for (const row of trips) {
      const tripId = scopedId(feedKey, row.trip_id);
      const routeId = scopedId(feedKey, row.route_id);
      const shapeId = scopedId(feedKey, row.shape_id);
      if (!tripId || !routeId) continue;
      await db.query(
        `INSERT INTO gtfs_trips (trip_id, feed_key, route_id, service_id, trip_headsign, direction_id, shape_id)
         VALUES ($1, $2, $3, $4, $5, $6, $7)
         ON CONFLICT (trip_id) DO UPDATE
         SET feed_key = EXCLUDED.feed_key,
             route_id = EXCLUDED.route_id,
             service_id = EXCLUDED.service_id,
             trip_headsign = EXCLUDED.trip_headsign,
             direction_id = EXCLUDED.direction_id,
             shape_id = EXCLUDED.shape_id`,
        [
          tripId,
          feedKey,
          routeId,
          row.service_id || null,
          row.trip_headsign || null,
          row.direction_id != null && row.direction_id !== "" ? Number(row.direction_id) : null,
          shapeId,
        ]
      );
      insertedTrips += 1;
    }

    for (const row of shapes) {
      const shapeId = scopedId(feedKey, row.shape_id);
      const lat = toFloat(row.shape_pt_lat);
      const lon = toFloat(row.shape_pt_lon);
      const seq = toInt(row.shape_pt_sequence);
      if (!shapeId || lat == null || lon == null || seq == null) continue;
      await db.query(
        `INSERT INTO gtfs_shapes (feed_key, shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence)
         VALUES ($1, $2, $3, $4, $5)`,
        [feedKey, shapeId, lat, lon, seq]
      );
      insertedShapes += 1;
    }

    for (const row of stops) {
      const stopId = scopedId(feedKey, row.stop_id);
      const lat = toFloat(row.stop_lat);
      const lon = toFloat(row.stop_lon);
      if (!stopId || lat == null || lon == null) continue;
      await db.query(
        `INSERT INTO gtfs_stops (stop_id, feed_key, stop_name, stop_lat, stop_lon)
         VALUES ($1, $2, $3, $4, $5)
         ON CONFLICT (stop_id) DO UPDATE
         SET feed_key = EXCLUDED.feed_key,
             stop_name = EXCLUDED.stop_name,
             stop_lat = EXCLUDED.stop_lat,
             stop_lon = EXCLUDED.stop_lon`,
        [stopId, feedKey, row.stop_name || null, lat, lon]
      );
      insertedStops += 1;
    }

    for (const row of stopTimes) {
      const tripId = scopedId(feedKey, row.trip_id);
      const stopId = scopedId(feedKey, row.stop_id);
      const seq = toInt(row.stop_sequence);
      if (!tripId || seq == null) continue;
      await db.query(
        `INSERT INTO gtfs_stop_times (feed_key, trip_id, arrival_time, departure_time, stop_id, stop_sequence)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [feedKey, tripId, row.arrival_time || null, row.departure_time || null, stopId || null, seq]
      );
      insertedStopTimes += 1;
    }

    for (const row of calendars) {
      const serviceId = scopedId(feedKey, row.service_id);
      const startDate = parseGtfsDate(row.start_date);
      const endDate = parseGtfsDate(row.end_date);
      if (!serviceId || !startDate || !endDate) continue;
      await db.query(
        `INSERT INTO gtfs_calendars (
           feed_key, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date, is_active, updated_at
         )
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, TRUE, NOW())
         ON CONFLICT (feed_key, service_id) DO UPDATE
         SET monday = EXCLUDED.monday,
             tuesday = EXCLUDED.tuesday,
             wednesday = EXCLUDED.wednesday,
             thursday = EXCLUDED.thursday,
             friday = EXCLUDED.friday,
             saturday = EXCLUDED.saturday,
             sunday = EXCLUDED.sunday,
             start_date = EXCLUDED.start_date,
             end_date = EXCLUDED.end_date,
             is_active = TRUE,
             updated_at = NOW()`,
        [
          feedKey,
          serviceId,
          toInt(row.monday) || 0,
          toInt(row.tuesday) || 0,
          toInt(row.wednesday) || 0,
          toInt(row.thursday) || 0,
          toInt(row.friday) || 0,
          toInt(row.saturday) || 0,
          toInt(row.sunday) || 0,
          startDate,
          endDate,
        ]
      );
      insertedCalendars += 1;
    }

    for (const row of calendarDates) {
      const serviceId = scopedId(feedKey, row.service_id);
      const calendarDate = parseGtfsDate(row.date);
      const exceptionType = toInt(row.exception_type);
      if (!serviceId || !calendarDate || !exceptionType) continue;
      await db.query(
        `INSERT INTO gtfs_calendar_dates (feed_key, service_id, calendar_date, exception_type)
         VALUES ($1, $2, $3, $4)
         ON CONFLICT (feed_key, service_id, calendar_date) DO UPDATE
         SET exception_type = EXCLUDED.exception_type`,
        [feedKey, serviceId, calendarDate, exceptionType]
      );
      insertedCalendarDates += 1;
    }

    await db.query(
      `UPDATE gtfs_feeds
       SET routes_count = $2,
           trips_count = $3,
           shapes_count = $4,
           stops_count = $5,
           stop_times_count = $6,
           updated_at = NOW(),
           last_imported_at = NOW()
       WHERE feed_key = $1`,
      [feedKey, insertedRoutes, insertedTrips, insertedShapes, insertedStops, insertedStopTimes]
    );

    await db.query("COMMIT");
    return res.json({
      message: "GTFS importado com sucesso.",
      feed: { feedKey, feedName, replaceFeed },
      counts: {
        routes: insertedRoutes,
        trips: insertedTrips,
        shapes: insertedShapes,
        stops: insertedStops,
        stopTimes: insertedStopTimes,
        calendars: insertedCalendars,
        calendarDates: insertedCalendarDates,
      },
    });
  } catch (error) {
    await db.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao importar GTFS.", error: error.message });
  }
});

router.get("/status", async (_req, res) => {
  try {
    await ensureGtfsEditorIndexes();
    const counts = await db.query(
      `SELECT
         (SELECT COUNT(*)::int FROM gtfs_routes) AS routes,
         (SELECT COUNT(*)::int FROM gtfs_trips) AS trips,
         (SELECT COUNT(*)::int FROM gtfs_shapes) AS shapes,
         (SELECT COUNT(*)::int FROM gtfs_stops) AS stops,
         (SELECT COUNT(*)::int FROM gtfs_stop_times) AS stop_times,
         (SELECT COUNT(*)::int FROM gtfs_feeds WHERE is_active = TRUE) AS active_feeds`
    );
    const feeds = await db.query(
      `SELECT feed_key, feed_name, is_active, gtfs_effective_from, calendar_effective_from, source_filename, routes_count, trips_count, stops_count, updated_at
       FROM gtfs_feeds
       ORDER BY is_active DESC, updated_at DESC, feed_key ASC`
    );
    return res.json({ persisted: true, counts: counts.rows[0], feeds: feeds.rows });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao consultar estado GTFS." });
  }
});

router.get("/feeds", async (_req, res) => {
  try {
    await ensureGtfsEditorIndexes();
    const result = await db.query(
      `SELECT feed_key, feed_name, is_active, gtfs_effective_from, calendar_effective_from, source_filename, routes_count, trips_count, shapes_count, stops_count, stop_times_count, updated_at, last_imported_at
       FROM gtfs_feeds
       ORDER BY is_active DESC, updated_at DESC, feed_key ASC`
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar feeds GTFS." });
  }
});

router.patch("/feeds/:feedKey", async (req, res) => {
  const feedKey = normalizeFeedKey(req.params.feedKey);
  if (!feedKey) return res.status(400).json({ message: "feedKey invalido." });
  const hasIsActive = typeof req.body?.isActive === "boolean";
  const hasName = req.body?.feedName != null;
  const hasGtfsEffectiveFrom = req.body?.gtfsEffectiveFrom !== undefined;
  const hasCalendarEffectiveFrom = req.body?.calendarEffectiveFrom !== undefined;
  if (!hasIsActive && !hasName && !hasGtfsEffectiveFrom && !hasCalendarEffectiveFrom) {
    return res.status(400).json({ message: "Nada para atualizar." });
  }
  try {
    await ensureGtfsEditorIndexes();
    if (hasIsActive && req.body.isActive === false) {
      const activeCount = await db.query(`SELECT COUNT(*)::int AS c FROM gtfs_feeds WHERE is_active = TRUE`);
      if (Number(activeCount.rows[0]?.c || 0) <= 1) {
        return res.status(400).json({ message: "Tem de existir pelo menos um feed GTFS ativo." });
      }
    }
    const result = await db.query(
      `UPDATE gtfs_feeds
       SET is_active = COALESCE($2::boolean, is_active),
           feed_name = COALESCE(NULLIF($3::text, ''), feed_name),
           gtfs_effective_from = COALESCE($4::date, gtfs_effective_from),
           calendar_effective_from = COALESCE($5::date, calendar_effective_from),
           updated_at = NOW()
       WHERE feed_key = $1
       RETURNING feed_key, feed_name, is_active, gtfs_effective_from, calendar_effective_from, source_filename, routes_count, trips_count, shapes_count, stops_count, stop_times_count, updated_at, last_imported_at`,
      [
        feedKey,
        hasIsActive ? req.body.isActive : null,
        hasName ? String(req.body.feedName).trim() : null,
        hasGtfsEffectiveFrom && req.body.gtfsEffectiveFrom ? String(req.body.gtfsEffectiveFrom) : null,
        hasCalendarEffectiveFrom && req.body.calendarEffectiveFrom ? String(req.body.calendarEffectiveFrom) : null,
      ]
    );
    if (!result.rowCount) return res.status(404).json({ message: "Feed GTFS nao encontrado." });
    return res.json({ message: "Feed GTFS atualizado.", feed: result.rows[0] });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar feed GTFS." });
  }
});

router.get("/feeds/:feedKey/calendars", async (req, res) => {
  const feedKey = normalizeFeedKey(req.params.feedKey);
  if (!feedKey) return res.status(400).json({ message: "feedKey invalido." });
  try {
    await ensureGtfsEditorIndexes();
    const result = await db.query(
      `SELECT
         service_id,
         monday, tuesday, wednesday, thursday, friday, saturday, sunday,
         start_date, end_date, is_active, updated_at
       FROM gtfs_calendars
       WHERE feed_key = $1
       ORDER BY service_id ASC`,
      [feedKey]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar calendários GTFS." });
  }
});

router.patch("/feeds/:feedKey/calendars/:serviceId", async (req, res) => {
  const feedKey = normalizeFeedKey(req.params.feedKey);
  const serviceId = String(req.params.serviceId || "").trim();
  if (!feedKey || !serviceId) return res.status(400).json({ message: "feedKey/serviceId inválidos." });
  try {
    await ensureGtfsEditorIndexes();
    const hasAnyField =
      req.body?.startDate !== undefined ||
      req.body?.endDate !== undefined ||
      req.body?.isActive !== undefined ||
      req.body?.monday !== undefined ||
      req.body?.tuesday !== undefined ||
      req.body?.wednesday !== undefined ||
      req.body?.thursday !== undefined ||
      req.body?.friday !== undefined ||
      req.body?.saturday !== undefined ||
      req.body?.sunday !== undefined;
    if (!hasAnyField) return res.status(400).json({ message: "Nada para atualizar no calendário." });

    const result = await db.query(
      `UPDATE gtfs_calendars
       SET start_date = COALESCE($3::date, start_date),
           end_date = COALESCE($4::date, end_date),
           is_active = COALESCE($5::boolean, is_active),
           monday = COALESCE($6::int, monday),
           tuesday = COALESCE($7::int, tuesday),
           wednesday = COALESCE($8::int, wednesday),
           thursday = COALESCE($9::int, thursday),
           friday = COALESCE($10::int, friday),
           saturday = COALESCE($11::int, saturday),
           sunday = COALESCE($12::int, sunday),
           updated_at = NOW()
       WHERE feed_key = $1
         AND service_id = $2
       RETURNING service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date, is_active, updated_at`,
      [
        feedKey,
        serviceId,
        req.body?.startDate || null,
        req.body?.endDate || null,
        typeof req.body?.isActive === "boolean" ? req.body.isActive : null,
        req.body?.monday != null ? toInt(req.body.monday) : null,
        req.body?.tuesday != null ? toInt(req.body.tuesday) : null,
        req.body?.wednesday != null ? toInt(req.body.wednesday) : null,
        req.body?.thursday != null ? toInt(req.body.thursday) : null,
        req.body?.friday != null ? toInt(req.body.friday) : null,
        req.body?.saturday != null ? toInt(req.body.saturday) : null,
        req.body?.sunday != null ? toInt(req.body.sunday) : null,
      ]
    );
    if (!result.rowCount) return res.status(404).json({ message: "Serviço de calendário não encontrado." });
    return res.json({ message: "Calendário atualizado.", calendar: result.rows[0] });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar calendário GTFS." });
  }
});

router.get("/feeds/:feedKey/export.zip", async (req, res) => {
  const feedKey = normalizeFeedKey(req.params.feedKey);
  if (!feedKey) return res.status(400).json({ message: "feedKey invalido." });
  try {
    await ensureGtfsEditorIndexes();
    const [routes, trips, shapes, stops, stopTimes, calendars, calendarDates] = await Promise.all([
      db.query(
        `SELECT
           replace(route_id, $1 || '::', '') AS route_id,
           route_short_name, route_long_name
         FROM gtfs_routes
         WHERE feed_key = $1
         ORDER BY route_id ASC`,
        [feedKey]
      ),
      db.query(
        `SELECT
           replace(trip_id, $1 || '::', '') AS trip_id,
           replace(route_id, $1 || '::', '') AS route_id,
           service_id, trip_headsign, direction_id,
           replace(shape_id, $1 || '::', '') AS shape_id
         FROM gtfs_trips
         WHERE feed_key = $1
         ORDER BY trip_id ASC`,
        [feedKey]
      ),
      db.query(
        `SELECT
           replace(shape_id, $1 || '::', '') AS shape_id,
           shape_pt_lat, shape_pt_lon, shape_pt_sequence
         FROM gtfs_shapes
         WHERE feed_key = $1
         ORDER BY shape_id ASC, shape_pt_sequence ASC`,
        [feedKey]
      ),
      db.query(
        `SELECT
           replace(stop_id, $1 || '::', '') AS stop_id,
           stop_name, stop_lat, stop_lon
         FROM gtfs_stops
         WHERE feed_key = $1
         ORDER BY stop_id ASC`,
        [feedKey]
      ),
      db.query(
        `SELECT
           replace(trip_id, $1 || '::', '') AS trip_id,
           arrival_time, departure_time,
           replace(stop_id, $1 || '::', '') AS stop_id,
           stop_sequence
         FROM gtfs_stop_times
         WHERE feed_key = $1
         ORDER BY trip_id ASC, stop_sequence ASC`,
        [feedKey]
      ),
      db.query(
        `SELECT
           replace(service_id, $1 || '::', '') AS service_id,
           monday, tuesday, wednesday, thursday, friday, saturday, sunday,
           start_date, end_date, is_active
         FROM gtfs_calendars
         WHERE feed_key = $1
           AND is_active = TRUE
         ORDER BY service_id ASC`,
        [feedKey]
      ),
      db.query(
        `SELECT
           replace(service_id, $1 || '::', '') AS service_id,
           calendar_date, exception_type
         FROM gtfs_calendar_dates
         WHERE feed_key = $1
         ORDER BY service_id ASC, calendar_date ASC`,
        [feedKey]
      ),
    ]);

    const zip = new AdmZip();
    zip.addFile(
      "routes.txt",
      Buffer.from(toCsv(routes.rows, ["route_id", "route_short_name", "route_long_name"]), "utf8")
    );
    zip.addFile(
      "trips.txt",
      Buffer.from(
        toCsv(trips.rows, ["route_id", "service_id", "trip_id", "trip_headsign", "direction_id", "shape_id"]),
        "utf8"
      )
    );
    zip.addFile(
      "shapes.txt",
      Buffer.from(toCsv(shapes.rows, ["shape_id", "shape_pt_lat", "shape_pt_lon", "shape_pt_sequence"]), "utf8")
    );
    zip.addFile("stops.txt", Buffer.from(toCsv(stops.rows, ["stop_id", "stop_name", "stop_lat", "stop_lon"]), "utf8"));
    zip.addFile(
      "stop_times.txt",
      Buffer.from(toCsv(stopTimes.rows, ["trip_id", "arrival_time", "departure_time", "stop_id", "stop_sequence"]), "utf8")
    );
    zip.addFile(
      "calendar.txt",
      Buffer.from(
        toCsv(
          calendars.rows.map((r) => ({
            ...r,
            start_date: formatGtfsDate(r.start_date),
            end_date: formatGtfsDate(r.end_date),
          })),
          ["service_id", "monday", "tuesday", "wednesday", "thursday", "friday", "saturday", "sunday", "start_date", "end_date"]
        ),
        "utf8"
      )
    );
    zip.addFile(
      "calendar_dates.txt",
      Buffer.from(
        toCsv(
          calendarDates.rows.map((r) => ({
            ...r,
            date: formatGtfsDate(r.calendar_date),
          })),
          ["service_id", "date", "exception_type"]
        ),
        "utf8"
      )
    );

    const fileBuffer = zip.toBuffer();
    res.setHeader("Content-Type", "application/zip");
    res.setHeader("Content-Disposition", `attachment; filename=gtfs_${feedKey}_modified.zip`);
    return res.status(200).send(fileBuffer);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar GTFS modificado." });
  }
});

router.get("/editor/lines", async (req, res) => {
  try {
    await ensureGtfsEditorIndexes();
    const feedKey = normalizeFeedKey(req.query.feedKey || "");
    const includeInactive = String(req.query.includeInactive || "").toLowerCase() === "true";
    const result = await db.query(
      `SELECT
         r.feed_key,
         r.route_id,
         COALESCE(r.route_short_name, '') AS route_short_name,
         COALESCE(r.route_long_name, '') AS route_long_name,
         COUNT(DISTINCT t.trip_id)::int AS trips_count,
         COUNT(DISTINCT st.stop_id)::int AS stops_count
       FROM gtfs_routes r
       JOIN gtfs_feeds gf ON gf.feed_key = r.feed_key
       LEFT JOIN gtfs_trips t ON t.route_id = r.route_id
       LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
       WHERE ($1::text = '' OR r.feed_key = $1)
         AND ($2::boolean = TRUE OR gf.is_active = TRUE)
       GROUP BY r.feed_key, r.route_id, r.route_short_name, r.route_long_name
       ORDER BY r.feed_key ASC, NULLIF(r.route_short_name, '') ASC NULLS LAST, r.route_id ASC`,
      [feedKey, includeInactive]
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
    await ensureGtfsEditorIndexes();
    const result = await db.query(
      `SELECT
         t.trip_id,
         t.feed_key,
         t.route_id,
         COALESCE(t.trip_headsign, '') AS trip_headsign,
         t.direction_id,
         t.service_id,
         COUNT(st.stop_id)::int AS stops_count
       FROM gtfs_trips t
       JOIN gtfs_feeds gf ON gf.feed_key = t.feed_key
       LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
       WHERE t.route_id = $1
         AND gf.is_active = TRUE
       GROUP BY t.trip_id, t.feed_key, t.route_id, t.trip_headsign, t.direction_id, t.service_id
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
    await ensureGtfsEditorIndexes();
    const tripRes = await db.query(
      `SELECT trip_id, feed_key, route_id, trip_headsign, direction_id, service_id
       FROM gtfs_trips
       WHERE trip_id = $1`,
      [tripId]
    );
    if (!tripRes.rowCount) return res.status(404).json({ message: "Trip GTFS não encontrada." });
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
  const applyScope = String(req.body?.applyScope || "trip").trim().toLowerCase();
  const stopIdRaw = String(req.body?.stopId || "").trim();
  const stopName = String(req.body?.stopName || "").trim();
  const stopLat = toFloat(req.body?.stopLat);
  const stopLon = toFloat(req.body?.stopLon);
  const arrivalTime = String(req.body?.arrivalTime || "").trim() || null;
  const departureTime = String(req.body?.departureTime || "").trim() || null;
  const requestedSequence = toInt(req.body?.stopSequence);

  if (!tripId) return res.status(400).json({ message: "Indique tripId." });
  if (applyScope !== "trip" && applyScope !== "route") {
    return res.status(400).json({ message: "applyScope inválido (use trip ou route)." });
  }
  if (!stopIdRaw && (!stopName || stopLat == null || stopLon == null)) {
    return res.status(400).json({ message: "Indique stopId existente ou stopName+stopLat+stopLon para criar nova paragem." });
  }

  const client = await db.pool.connect();
  try {
    await ensureGtfsEditorIndexes();
    await client.query("BEGIN");
    const tripRes = await client.query(
      `SELECT trip_id, feed_key, route_id
       FROM gtfs_trips
       WHERE trip_id = $1
       FOR UPDATE`,
      [tripId]
    );
    if (!tripRes.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Trip GTFS não encontrada." });
    }
    const tripFeedKey = String(tripRes.rows[0].feed_key || "default");
    const routeId = String(tripRes.rows[0].route_id || "");
    const targetTripsRes =
      applyScope === "route"
        ? await client.query(
            `SELECT trip_id
             FROM gtfs_trips
             WHERE feed_key = $1
               AND route_id = $2
             ORDER BY trip_id ASC
             FOR UPDATE`,
            [tripFeedKey, routeId]
          )
        : { rows: [{ trip_id: tripId }] };
    const targetTripIds = targetTripsRes.rows.map((r) => String(r.trip_id || "").trim()).filter(Boolean);
    if (!targetTripIds.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Não foram encontradas trips para atualizar." });
    }
    let stopId = stopIdRaw;

    if (stopId) {
      const scopedStopId = scopedId(tripFeedKey, stopId);
      const stopRes = await client.query(`SELECT stop_id FROM gtfs_stops WHERE stop_id = $1`, [scopedStopId]);
      if (!stopRes.rowCount) {
        await client.query("ROLLBACK");
        return res.status(404).json({ message: "stopId não existe em gtfs_stops." });
      }
      stopId = scopedStopId;
    } else {
      stopId = scopedId(tripFeedKey, `custom_${Date.now()}`);
      await client.query(
        `INSERT INTO gtfs_stops (stop_id, feed_key, stop_name, stop_lat, stop_lon)
         VALUES ($1, $2, $3, $4, $5)`,
        [stopId, tripFeedKey, stopName, stopLat, stopLon]
      );
    }

    let totalInserted = 0;
    for (const targetTripId of targetTripIds) {
      const maxSeqRes = await client.query(
        `SELECT COALESCE(MAX(stop_sequence), 0)::int AS max_seq
         FROM gtfs_stop_times
         WHERE trip_id = $1`,
        [targetTripId]
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
          [targetTripId, seq]
        );
      }
      await client.query(
        `INSERT INTO gtfs_stop_times (feed_key, trip_id, arrival_time, departure_time, stop_id, stop_sequence)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [tripFeedKey, targetTripId, arrivalTime, departureTime, stopId, seq]
      );
      totalInserted += 1;
    }

    await client.query("COMMIT");
    const msg =
      applyScope === "route"
        ? `Paragem adicionada em ${totalInserted} trips da carreira.`
        : "Paragem adicionada à trip GTFS.";
    return res.json({ message: msg, tripId, stopId, applyScope, affectedTrips: totalInserted });
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
  const applyScope = String(req.query.applyScope || req.body?.applyScope || "trip")
    .trim()
    .toLowerCase();
  if (!tripId || !Number.isFinite(stopSequence) || stopSequence <= 0) {
    return res.status(400).json({ message: "Indique tripId e stopSequence válidos." });
  }
  if (applyScope !== "trip" && applyScope !== "route") {
    return res.status(400).json({ message: "applyScope inválido (use trip ou route)." });
  }

  const client = await db.pool.connect();
  try {
    await client.query("BEGIN");
    const tripRes = await client.query(
      `SELECT trip_id, feed_key, route_id
       FROM gtfs_trips
       WHERE trip_id = $1
       FOR UPDATE`,
      [tripId]
    );
    if (!tripRes.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Trip GTFS não encontrada." });
    }
    const tripFeedKey = String(tripRes.rows[0].feed_key || "default");
    const routeId = String(tripRes.rows[0].route_id || "");
    const targetTripsRes =
      applyScope === "route"
        ? await client.query(
            `SELECT trip_id
             FROM gtfs_trips
             WHERE feed_key = $1
               AND route_id = $2
             ORDER BY trip_id ASC
             FOR UPDATE`,
            [tripFeedKey, routeId]
          )
        : { rows: [{ trip_id: tripId }] };
    const targetTripIds = targetTripsRes.rows.map((r) => String(r.trip_id || "").trim()).filter(Boolean);
    let totalAffected = 0;
    for (const targetTripId of targetTripIds) {
      const delRes = await client.query(
        `DELETE FROM gtfs_stop_times
         WHERE trip_id = $1
           AND stop_sequence = $2
         RETURNING trip_id`,
        [targetTripId, stopSequence]
      );
      if (!delRes.rowCount) continue;
      totalAffected += 1;
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
        [targetTripId]
      );
    }
    if (!totalAffected) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Paragem não encontrada para remoção." });
    }

    await client.query("COMMIT");
    const msg =
      applyScope === "route"
        ? `Paragem removida em ${totalAffected} trips da carreira e sequências normalizadas.`
        : "Paragem removida e sequência normalizada.";
    return res.json({
      message: msg,
      tripId,
      removedStopSequence: stopSequence,
      applyScope,
      affectedTrips: totalAffected,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao remover paragem da trip GTFS." });
  } finally {
    client.release();
  }
});

module.exports = router;
