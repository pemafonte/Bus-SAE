const db = require("../db");

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

function parseGtfsTimeToMinutes(gtfsTime) {
  if (!gtfsTime || !String(gtfsTime).includes(":")) return null;
  const [h, m] = String(gtfsTime).trim().split(":");
  const hh = Number(h);
  const mm = Number(m);
  if (Number.isNaN(hh) || Number.isNaN(mm)) return null;
  return hh * 60 + mm;
}

/** Última paragem do trip: minutos desde a meia-noite do «service day» GTFS (pode ultrapassar 24h). */
async function getTripTerminusMinutes(tripId) {
  if (!tripId) return null;
  const result = await db.query(
    `SELECT st.departure_time, st.arrival_time
     FROM gtfs_stop_times st
     WHERE st.trip_id = $1
     ORDER BY st.stop_sequence DESC NULLS LAST
     LIMIT 1`,
    [tripId]
  );
  if (!result.rowCount) return null;
  const row = result.rows[0];
  const raw =
    row.departure_time && String(row.departure_time).trim()
      ? row.departure_time
      : row.arrival_time && String(row.arrival_time).trim()
        ? row.arrival_time
        : null;
  return parseGtfsTimeToMinutes(raw);
}

function resolveGtfsRouteCandidates(lineCode) {
  const raw = String(lineCode || "").trim();
  if (!raw) return [];
  const set = new Set([raw]);
  const collapsed = raw.replace(/\s+/g, "");
  if (collapsed && collapsed !== raw) set.add(collapsed);
  const collapsedLower = collapsed.toLowerCase();
  if (collapsedLower !== collapsed) set.add(collapsedLower);
  if (/^\d{4}$/.test(collapsed)) {
    set.add(collapsed.slice(-1));
    set.add(collapsed.slice(-2));
    set.add(collapsed.slice(-3));
  }
  const digitsOnly = collapsed.replace(/\D/g, "");
  if (digitsOnly && digitsOnly !== collapsed) set.add(digitsOnly);
  if (digitsOnly.length > 1) {
    set.add(digitsOnly.replace(/^0+/, "") || "0");
  }
  const noLeadingZeros = collapsed.replace(/^0+/, "") || "0";
  if (noLeadingZeros !== collapsed) set.add(noLeadingZeros);
  const digitsOnlyLoose = raw.replace(/\D/g, "");
  if (digitsOnlyLoose) {
    set.add(digitsOnlyLoose);
    const noLeadingZerosDigits = digitsOnlyLoose.replace(/^0+/, "") || "0";
    set.add(noLeadingZerosDigits);
    for (let i = 1; i < digitsOnlyLoose.length; i += 1) {
      set.add(digitsOnlyLoose.slice(i));
    }
  }
  const alnumTokens = raw
    .split(/[^A-Za-z0-9]+/)
    .map((token) => token.trim())
    .filter(Boolean);
  alnumTokens.forEach((token) => {
    set.add(token);
    set.add(token.toLowerCase());
    const tokenDigits = token.replace(/\D/g, "");
    if (tokenDigits) set.add(tokenDigits.replace(/^0+/, "") || "0");
  });
  return [...set].filter(Boolean);
}

function resolveDirectionHint(lineCode) {
  const raw = String(lineCode || "").trim();
  if (/^\d{4}$/.test(raw)) {
    const penultimate = Number(raw.charAt(2));
    if (penultimate === 1) return 0;
    if (penultimate === 2) return 1;
  }
  return null;
}

function pickTripBySchedule(rows, targetMinutes) {
  if (!rows.length) return null;
  if (targetMinutes == null) {
    const sorted = [...rows].sort((a, b) => {
      const ta = parseGtfsTimeToMinutes(a.first_departure_time);
      const tb = parseGtfsTimeToMinutes(b.first_departure_time);
      if (ta == null && tb == null) return String(a.trip_id).localeCompare(String(b.trip_id));
      if (ta == null) return 1;
      if (tb == null) return -1;
      return ta - tb;
    });
    return sorted[0];
  }
  let selected = null;
  let selectedDiff = Number.POSITIVE_INFINITY;
  for (const trip of rows) {
    const tripMinutes = parseGtfsTimeToMinutes(trip.first_departure_time);
    if (tripMinutes == null) continue;
    const diff = Math.abs(tripMinutes - targetMinutes);
    if (diff < selectedDiff) {
      selectedDiff = diff;
      selected = trip;
    }
  }
  if (selected) return selected;
  return rows.sort((a, b) => String(a.trip_id).localeCompare(String(b.trip_id)))[0];
}

async function findBestTripForLine(lineCode, serviceSchedule) {
  if (!lineCode) return null;
  const targetMinutes = parseServiceScheduleStartMinutes(serviceSchedule);
  const routeCandidates = resolveGtfsRouteCandidates(lineCode);
  const directionHint = resolveDirectionHint(lineCode);

  const makeBaseSql = (useFeedFilter = true) => `
     SELECT
       t.trip_id,
       t.shape_id,
       t.direction_id,
       MIN(st.departure_time) AS first_departure_time
     FROM gtfs_trips t
     JOIN gtfs_routes r ON r.route_id = t.route_id
     ${useFeedFilter ? "JOIN gtfs_feeds gf ON gf.feed_key = t.feed_key AND gf.is_active = TRUE" : ""}
     LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
     WHERE (%WHERE%)
       AND ($2::int IS NULL OR t.direction_id = $2)
     GROUP BY t.trip_id, t.shape_id, t.direction_id`;
  const runLookup = async (baseSql) =>
    db.query(
      baseSql.replace(
        "%WHERE%",
        `(TRIM(COALESCE(r.route_short_name, '')) = ANY($1::text[])
          OR TRIM(COALESCE(r.route_id, '')) = ANY($1::text[])
          OR regexp_replace(TRIM(LOWER(COALESCE(r.route_short_name, ''))), '^0+', '', 'g') = ANY($1::text[])
          OR regexp_replace(TRIM(LOWER(COALESCE(r.route_id, ''))), '^0+', '', 'g') = ANY($1::text[]))`
      ),
      [routeCandidates, directionHint]
    );

  let baseSql = makeBaseSql(true);
  let tripsResult;
  try {
    tripsResult = await runLookup(baseSql);
  } catch (_error) {
    baseSql = makeBaseSql(false);
    tripsResult = await runLookup(baseSql);
  }

  if (!tripsResult.rows.length) {
    const norm = routeCandidates.map((c) =>
      String(c || "")
        .trim()
        .toLowerCase()
        .replace(/\s+/g, "")
    );
    const normUnique = [...new Set(norm.filter((c) => c.length >= 2))];
    if (normUnique.length) {
      tripsResult = await db.query(
        baseSql.replace(
          "%WHERE%",
          `regexp_replace(TRIM(LOWER(COALESCE(r.route_short_name, ''))), '[[:space:]]+', '', 'g') = ANY($1::text[])
           OR regexp_replace(TRIM(LOWER(COALESCE(r.route_id, ''))), '[[:space:]]+', '', 'g') = ANY($1::text[])`
        ),
        [normUnique, directionHint]
      );
    }
  }

  if (!tripsResult.rows.length) {
    const likeCandidates = [...new Set(routeCandidates)]
      .map((c) => String(c || "").trim())
      .filter((c) => c.length >= 1);
    if (likeCandidates.length) {
      tripsResult = await db.query(
        baseSql.replace(
          "%WHERE%",
          `EXISTS (
             SELECT 1
             FROM unnest($1::text[]) AS cand(value)
             WHERE TRIM(COALESCE(r.route_short_name, '')) ILIKE '%' || cand.value || '%'
                OR TRIM(COALESCE(r.route_id, '')) ILIKE '%' || cand.value || '%'
           )`
        ),
        [likeCandidates, directionHint]
      );
    }
  }

  if (!tripsResult.rows.length) return null;
  return pickTripBySchedule(tripsResult.rows, targetMinutes);
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

module.exports = {
  parseServiceScheduleStartMinutes,
  parseGtfsTimeToMinutes,
  getTripTerminusMinutes,
  findBestTripForLine,
  getShapePointsByTripId,
  getStopsByTripId,
};
