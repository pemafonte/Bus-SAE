const express = require("express");
const AdmZip = require("adm-zip");
const { parse } = require("csv-parse/sync");
const XLSX = require("xlsx");
const booleanPointInPolygon = require("@turf/boolean-point-in-polygon").default;
const { point, polygon, multiPolygon } = require("@turf/helpers");
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

function parseIsoDateInput(raw) {
  const text = String(raw || "").trim();
  if (!/^\d{4}-\d{2}-\d{2}$/.test(text)) return null;
  const date = new Date(`${text}T00:00:00.000Z`);
  if (Number.isNaN(date.getTime())) return null;
  if (date.toISOString().slice(0, 10) !== text) return null;
  return text;
}

function parseMunicipalHolidayInput(raw) {
  const text = String(raw || "").trim();
  if (!text) return null;
  let day;
  let month;
  if (/^\d{4}-\d{2}-\d{2}$/.test(text)) {
    month = Number(text.slice(5, 7));
    day = Number(text.slice(8, 10));
  } else if (/^\d{2}-\d{2}$/.test(text)) {
    day = Number(text.slice(0, 2));
    month = Number(text.slice(3, 5));
  } else {
    return null;
  }
  if (!Number.isFinite(day) || !Number.isFinite(month) || day < 1 || day > 31 || month < 1 || month > 12) {
    return null;
  }
  return { day, month, label: `${String(day).padStart(2, "0")}-${String(month).padStart(2, "0")}` };
}

function formatDateIsoUtc(date) {
  return date.toISOString().slice(0, 10);
}

function computeEasterSundayUtc(year) {
  const a = year % 19;
  const b = Math.floor(year / 100);
  const c = year % 100;
  const d = Math.floor(b / 4);
  const e = b % 4;
  const f = Math.floor((b + 8) / 25);
  const g = Math.floor((b - f + 1) / 3);
  const h = (19 * a + b - d - g + 15) % 30;
  const i = Math.floor(c / 4);
  const k = c % 4;
  const l = (32 + 2 * e + 2 * i - h - k) % 7;
  const m = Math.floor((a + 11 * h + 22 * l) / 451);
  const month = Math.floor((h + l - 7 * m + 114) / 31);
  const day = ((h + l - 7 * m + 114) % 31) + 1;
  return new Date(Date.UTC(year, month - 1, day));
}

function addDaysUtc(date, days) {
  const copy = new Date(date.getTime());
  copy.setUTCDate(copy.getUTCDate() + days);
  return copy;
}

function buildHolidayDateList(startIso, endIso, municipalHoliday) {
  const start = new Date(`${startIso}T00:00:00.000Z`);
  const end = new Date(`${endIso}T00:00:00.000Z`);
  const years = [];
  for (let y = start.getUTCFullYear(); y <= end.getUTCFullYear(); y += 1) years.push(y);
  const fixedNational = [
    [1, 1],
    [25, 4],
    [1, 5],
    [10, 6],
    [15, 8],
    [5, 10],
    [1, 11],
    [1, 12],
    [8, 12],
    [25, 12],
  ];
  const set = new Set();
  years.forEach((year) => {
    fixedNational.forEach(([day, month]) => {
      set.add(`${year}-${String(month).padStart(2, "0")}-${String(day).padStart(2, "0")}`);
    });
    const easter = computeEasterSundayUtc(year);
    set.add(formatDateIsoUtc(addDaysUtc(easter, -2))); // Sexta-feira Santa
    set.add(formatDateIsoUtc(addDaysUtc(easter, 60))); // Corpo de Deus
    if (municipalHoliday?.day && municipalHoliday?.month) {
      set.add(`${year}-${String(municipalHoliday.month).padStart(2, "0")}-${String(municipalHoliday.day).padStart(2, "0")}`);
    }
  });
  return Array.from(set)
    .filter((iso) => iso >= startIso && iso <= endIso)
    .sort();
}

async function resolveAnalyticsPeriod(feedKey, startDate, endDate, periodMode = "") {
  const feedEffectiveRes = await db.query(
    `SELECT
       GREATEST(
         COALESCE(gtfs_effective_from, DATE '1900-01-01'),
         COALESCE(calendar_effective_from, DATE '1900-01-01')
       )::date AS effective_start,
       gtfs_effective_from,
       calendar_effective_from,
       (
         SELECT MIN(c.start_date)::date
         FROM gtfs_calendars c
         WHERE c.feed_key = gf.feed_key
       ) AS min_calendar_start,
       (
         SELECT MAX(c.end_date)::date
         FROM gtfs_calendars c
         WHERE c.feed_key = gf.feed_key
       ) AS max_calendar_end,
       (
         SELECT MIN(cd.calendar_date)::date
         FROM gtfs_calendar_dates cd
         WHERE cd.feed_key = gf.feed_key
       ) AS min_calendar_date,
       (
         SELECT MAX(cd.calendar_date)::date
         FROM gtfs_calendar_dates cd
         WHERE cd.feed_key = gf.feed_key
       ) AS max_calendar_date
     FROM gtfs_feeds
     gf
     WHERE feed_key = $1
     LIMIT 1`,
    [feedKey]
  );
  const row = feedEffectiveRes.rows[0] || {};
  const todayIso = parseIsoDateInput(new Date().toISOString().slice(0, 10));
  const mode = String(periodMode || "").trim().toLowerCase();
  const gtfsEffectiveFrom = parseIsoDateInput(String(row.gtfs_effective_from || "").slice(0, 10));
  const calendarEffectiveFrom = parseIsoDateInput(String(row.calendar_effective_from || "").slice(0, 10));
  const minCalendarStart = parseIsoDateInput(String(row.min_calendar_start || "").slice(0, 10));
  const maxCalendarEnd = parseIsoDateInput(String(row.max_calendar_end || "").slice(0, 10));
  const minCalendarDate = parseIsoDateInput(String(row.min_calendar_date || "").slice(0, 10));
  const maxCalendarDate = parseIsoDateInput(String(row.max_calendar_date || "").slice(0, 10));
  const feedEffectiveStartRaw = parseIsoDateInput(String(row.effective_start || "").slice(0, 10));
  const feedEffectiveStart =
    feedEffectiveStartRaw && feedEffectiveStartRaw !== "1900-01-01"
      ? feedEffectiveStartRaw
      : gtfsEffectiveFrom || calendarEffectiveFrom || minCalendarStart || minCalendarDate || todayIso;
  const feedYear = Number(String(feedEffectiveStart).slice(0, 4)) || Number(String(todayIso).slice(0, 4)) || new Date().getFullYear();
  let baseStart = parseIsoDateInput(startDate) || null;
  if (!baseStart) {
    if (mode === "from_today") {
      baseStart = todayIso;
    } else if (mode === "full_year") {
      baseStart = `${feedYear}-01-01`;
    } else {
      baseStart = feedEffectiveStart;
    }
  }
  const resolvedEndRaw = parseIsoDateInput(endDate) || null;
  const defaultEndCandidate =
    mode === "full_year"
      ? `${String(Number(String(baseStart).slice(0, 4)) || feedYear)}-12-31`
      : addDaysUtc(new Date(`${baseStart}T00:00:00.000Z`), 364).toISOString().slice(0, 10);
  const hardMaxEnd = maxCalendarEnd || maxCalendarDate || null;
  const defaultEnd = hardMaxEnd && hardMaxEnd < defaultEndCandidate ? hardMaxEnd : defaultEndCandidate;
  const resolvedEnd = resolvedEndRaw || defaultEnd;
  return {
    startDate: baseStart,
    endDate: resolvedEnd,
    effectiveStartDate: feedEffectiveStart,
    gtfsEffectiveFrom: gtfsEffectiveFrom || null,
    calendarEffectiveFrom: calendarEffectiveFrom || null,
  };
}

function toUtcDate(iso) {
  return new Date(`${iso}T00:00:00.000Z`);
}

function isoFromDateUtc(date) {
  return date.toISOString().slice(0, 10);
}

function stripFeedPrefix(id, feedKey) {
  const raw = String(id || "").trim();
  const prefix = `${feedKey}::`;
  if (raw.startsWith(prefix)) return raw.slice(prefix.length);
  return raw;
}

function sleep(ms) {
  return new Promise((resolve) => setTimeout(resolve, ms));
}

async function fetchJsonWithRetry(url, options = {}) {
  const retries = Number(options.retries ?? 3);
  const timeoutMs = Number(options.timeoutMs ?? 20000);
  let lastError = null;
  for (let attempt = 1; attempt <= retries; attempt += 1) {
    const controller = new AbortController();
    const timeout = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const response = await fetch(url, {
        signal: controller.signal,
        headers: { Accept: "application/json" },
      });
      clearTimeout(timeout);
      if (!response.ok) {
        lastError = new Error(`HTTP ${response.status}`);
      } else {
        const payload = await response.json().catch(() => null);
        if (payload != null) return payload;
        lastError = new Error("Resposta JSON inválida");
      }
    } catch (error) {
      clearTimeout(timeout);
      lastError = error;
    }
    if (attempt < retries) await sleep(500 * attempt);
  }
  throw lastError || new Error("Falha no fetch JSON");
}

async function mapWithConcurrency(items, limit, mapper) {
  const safeLimit = Math.max(1, Number(limit) || 1);
  const results = new Array(items.length);
  let cursor = 0;
  async function worker() {
    while (cursor < items.length) {
      const index = cursor;
      cursor += 1;
      results[index] = await mapper(items[index], index);
    }
  }
  const workers = Array.from({ length: Math.min(safeLimit, items.length) }, () => worker());
  await Promise.all(workers);
  return results;
}

function normalizeGeoText(value) {
  const text = String(value || "").trim();
  return text || null;
}

function normalizeBoundaryLevel(value) {
  const text = String(value || "")
    .trim()
    .toLowerCase();
  if (text === "municipality" || text === "concelho") return "municipality";
  if (text === "parish" || text === "freguesia") return "parish";
  return null;
}

function extractBBoxFromCoordinates(coordinates, acc = { minLat: 90, maxLat: -90, minLon: 180, maxLon: -180 }) {
  if (!Array.isArray(coordinates)) return acc;
  if (typeof coordinates[0] === "number" && typeof coordinates[1] === "number") {
    const lon = Number(coordinates[0]);
    const lat = Number(coordinates[1]);
    if (Number.isFinite(lat) && Number.isFinite(lon)) {
      acc.minLat = Math.min(acc.minLat, lat);
      acc.maxLat = Math.max(acc.maxLat, lat);
      acc.minLon = Math.min(acc.minLon, lon);
      acc.maxLon = Math.max(acc.maxLon, lon);
    }
    return acc;
  }
  coordinates.forEach((item) => extractBBoxFromCoordinates(item, acc));
  return acc;
}

function normalizeGeoApiMunicipalityPayload(payload) {
  const feature =
    (payload?.geojson?.type === "Feature" ? payload.geojson : null) ||
    (payload?.geojsons?.municipio?.type === "Feature" ? payload.geojsons.municipio : null);
  if (!feature?.geometry) return null;
  return {
    boundaryName: normalizeGeoText(feature?.properties?.Concelho || payload?.nome || payload?.municipio),
    municipalityName: normalizeGeoText(feature?.properties?.Concelho || payload?.nome || payload?.municipio),
    geometry: feature.geometry,
  };
}

function normalizeGeoApiParishFeatures(payload, municipalityName) {
  const rows = [];
  const features = Array.isArray(payload?.geojsons?.freguesias) ? payload.geojsons.freguesias : [];
  features.forEach((feature) => {
    const geometry = feature?.geometry || null;
    if (!geometry) return;
    const boundaryName = normalizeGeoText(feature?.properties?.Freguesia || feature?.properties?.nome || feature?.name);
    if (!boundaryName) return;
    rows.push({
      boundaryName,
      municipalityName: normalizeGeoText(feature?.properties?.Concelho || municipalityName),
      geometry,
    });
  });
  return rows;
}

async function reverseGeocodeStop(lat, lon) {
  const url = new URL("https://nominatim.openstreetmap.org/reverse");
  url.searchParams.set("lat", String(lat));
  url.searchParams.set("lon", String(lon));
  url.searchParams.set("format", "jsonv2");
  url.searchParams.set("addressdetails", "1");
  url.searchParams.set("accept-language", "pt-PT,pt");
  url.searchParams.set("zoom", "18");
  const response = await fetch(url.toString(), {
    headers: {
      "User-Agent": "Bus-SAE-GTFS/1.0 (reverse-geocode-stops)",
    },
  });
  if (!response.ok) return { municipality: null, parish: null };
  const payload = await response.json().catch(() => ({}));
  const address = payload?.address || {};
  const municipality = normalizeGeoText(
    address.municipality || address.city || address.town || address.county || address.state_district
  );
  const parish = normalizeGeoText(
    address.city_district ||
      address.borough ||
      address.suburb ||
      address.village ||
      address.hamlet ||
      address.quarter ||
      address.neighbourhood
  );
  const normalizedParish = parish && municipality && parish.toLowerCase() === municipality.toLowerCase() ? null : parish;
  return { municipality, parish: normalizedParish };
}

function classifyServiceDayType(serviceCode, calendar) {
  const code = String(serviceCode || "").toUpperCase();
  if (code.includes("-DF") || code.endsWith("DF")) return "sunday_holiday";
  if (code.includes("-S") || code.endsWith("S")) return "saturday";
  if (code.includes("-U") || code.endsWith("U")) return "weekday";
  const sat = Number(calendar?.saturday || 0) === 1;
  const sun = Number(calendar?.sunday || 0) === 1;
  const weekday =
    Number(calendar?.monday || 0) === 1 ||
    Number(calendar?.tuesday || 0) === 1 ||
    Number(calendar?.wednesday || 0) === 1 ||
    Number(calendar?.thursday || 0) === 1 ||
    Number(calendar?.friday || 0) === 1;
  if (weekday && !sat && !sun) return "weekday";
  if (!weekday && sat && !sun) return "saturday";
  if (!weekday && !sat && sun) return "sunday_holiday";
  if (weekday) return "weekday";
  return "unknown";
}

function classifyServiceScope(serviceCode) {
  const code = String(serviceCode || "").toUpperCase();
  if (code.includes("XJA")) return "except_jul_aug";
  if (code.includes("JA")) return "jul_aug";
  return "all_year";
}

function buildOperationalCalendarCounts(startDate, endDate, holidayDates) {
  const holidaySet = new Set(holidayDates || []);
  const counts = {
    all_year: { weekday: 0, saturday: 0, sunday: 0, holidayNonSunday: 0 },
    jul_aug: { weekday: 0, saturday: 0, sunday: 0, holidayNonSunday: 0 },
    except_jul_aug: { weekday: 0, saturday: 0, sunday: 0, holidayNonSunday: 0 },
  };
  const start = toUtcDate(startDate);
  const end = toUtcDate(endDate);
  for (let d = new Date(start); d <= end; d = addDaysUtc(d, 1)) {
    const iso = isoFromDateUtc(d);
    const month = d.getUTCMonth() + 1;
    const isJulAug = month === 7 || month === 8;
    const dow = d.getUTCDay(); // 0 Sunday
    const isHoliday = holidaySet.has(iso);
    const scopes = [counts.all_year, isJulAug ? counts.jul_aug : counts.except_jul_aug];
    scopes.forEach((scope) => {
      if (dow === 6) scope.saturday += 1;
      if (dow === 0) scope.sunday += 1;
      if (dow >= 1 && dow <= 5 && !isHoliday) scope.weekday += 1;
      if (isHoliday && dow !== 0) scope.holidayNonSunday += 1;
    });
  }
  return counts;
}

function scopeMonths(scope) {
  if (scope === "jul_aug") return 2;
  if (scope === "except_jul_aug") return 10;
  return 12;
}

async function computeGtfsOperationalReference({ feedKey, routeId, startDate, endDate, holidayDates }) {
  const tripsRes = await db.query(
    `WITH selected_routes AS (
       SELECT
         r.feed_key,
         r.route_id,
         COALESCE(NULLIF(TRIM(r.route_short_name), ''), r.route_id) AS route_label,
         COALESCE(NULLIF(TRIM(r.route_long_name), ''), '-') AS route_long_name
       FROM gtfs_routes r
       JOIN gtfs_feeds f ON f.feed_key = r.feed_key
       WHERE ($1::text = '' OR r.feed_key = $1)
         AND ($2::text = '' OR r.route_id = $2)
         AND f.is_active = TRUE
     ),
     trip_shapes AS (
       SELECT
         t.feed_key,
         t.route_id,
         sr.route_label,
         sr.route_long_name,
         t.trip_id,
         t.service_id,
         COALESCE(
           SUM(
             CASE
               WHEN prev_lat IS NULL OR prev_lon IS NULL THEN 0
               ELSE
                 6371 * 2 * ASIN(
                   SQRT(
                     POWER(SIN(RADIANS((gs.shape_pt_lat - prev_lat) / 2)), 2) +
                     COS(RADIANS(prev_lat)) * COS(RADIANS(gs.shape_pt_lat)) *
                     POWER(SIN(RADIANS((gs.shape_pt_lon - prev_lon) / 2)), 2)
                   )
                 )
             END
           ),
           0
         )::numeric(12,3) AS trip_km
        FROM gtfs_trips t
        JOIN selected_routes sr ON sr.feed_key = t.feed_key AND sr.route_id = t.route_id
       LEFT JOIN (
         SELECT
           feed_key,
           shape_id,
           shape_pt_sequence,
           shape_pt_lat,
           shape_pt_lon,
           LAG(shape_pt_lat) OVER (PARTITION BY feed_key, shape_id ORDER BY shape_pt_sequence ASC) AS prev_lat,
           LAG(shape_pt_lon) OVER (PARTITION BY feed_key, shape_id ORDER BY shape_pt_sequence ASC) AS prev_lon
         FROM gtfs_shapes
       ) gs ON gs.feed_key = t.feed_key AND gs.shape_id = t.shape_id
       GROUP BY t.feed_key, t.route_id, sr.route_label, sr.route_long_name, t.trip_id, t.service_id
     )
     SELECT * FROM trip_shapes`,
    [feedKey, routeId || ""]
  );
  const calendarsRes = await db.query(
    `SELECT *
     FROM gtfs_calendars
     WHERE ($1::text = '' OR feed_key = $1)`,
    [feedKey]
  );
  const realizedRes = await db.query(
    `SELECT
       LOWER(TRIM(COALESCE(line_code, ''))) AS line_key,
       COALESCE(SUM(COALESCE(total_km, 0)), 0)::numeric(12,3) AS realized_km
     FROM services
     WHERE LOWER(TRIM(COALESCE(status::text, ''))) = 'completed'
     GROUP BY LOWER(TRIM(COALESCE(line_code, '')))`,
    []
  );
  const calByFeedAndService = new Map();
  calendarsRes.rows.forEach((row) => {
    const key = `${row.feed_key}::${stripFeedPrefix(row.service_id, row.feed_key)}`;
    if (!calByFeedAndService.has(key)) calByFeedAndService.set(key, row);
  });
  const counts = buildOperationalCalendarCounts(startDate, endDate, holidayDates);
  const profiles = new Map();
  tripsRes.rows.forEach((row) => {
    const serviceCode = stripFeedPrefix(row.service_id, row.feed_key);
    const profileKey = `${row.route_id}::${serviceCode}`;
    const cal = calByFeedAndService.get(`${row.feed_key}::${serviceCode}`) || null;
    const dayType = classifyServiceDayType(serviceCode, cal);
    const scope = classifyServiceScope(serviceCode);
    if (!profiles.has(profileKey)) {
      profiles.set(profileKey, {
        feed_key: row.feed_key,
        route_id: row.route_id,
        route_label: row.route_label,
        route_long_name: row.route_long_name,
        service_code: serviceCode,
        day_type: dayType,
        scope,
        trip_ids: new Set(),
        total_trip_km: 0,
      });
    }
    const p = profiles.get(profileKey);
    p.trip_ids.add(row.trip_id);
    p.total_trip_km += Number(row.trip_km || 0);
  });
  const linesMap = new Map();
  const profileRows = [];
  profiles.forEach((p) => {
    const tripsPerDay = p.trip_ids.size;
    if (tripsPerDay <= 0) return;
    const avgTripKm = p.total_trip_km / tripsPerDay;
    const scopeCounts = counts[p.scope] || counts.all_year;
    const weekdayDays = scopeCounts.weekday;
    const saturdayDays = scopeCounts.saturday;
    const sundayDays = scopeCounts.sunday;
    const holidayNonSundayDays = scopeCounts.holidayNonSunday;
    let daysForKm = 0;
    let weekdayOps = 0;
    let saturdayOps = 0;
    let sundayOps = 0;
    let holidayOps = 0;
    if (p.day_type === "weekday") {
      daysForKm = weekdayDays;
      weekdayOps = tripsPerDay * weekdayDays;
    } else if (p.day_type === "saturday") {
      daysForKm = saturdayDays;
      saturdayOps = tripsPerDay * saturdayDays;
    } else if (p.day_type === "sunday_holiday") {
      daysForKm = sundayDays + holidayNonSundayDays;
      sundayOps = tripsPerDay * sundayDays;
      holidayOps = tripsPerDay * holidayNonSundayDays;
    }
    const kmDay = tripsPerDay * avgTripKm;
    const kmTotal = kmDay * daysForKm;
    const months = scopeMonths(p.scope);
    const kmMonth = months > 0 ? kmTotal / months : 0;
    const row = {
      feed_key: p.feed_key,
      route_id: p.route_id,
      route_label: p.route_label,
      route_long_name: p.route_long_name,
      service_code: p.service_code,
      day_type: p.day_type,
      scope: p.scope,
      trips_per_day: tripsPerDay,
      avg_trip_km: Number(avgTripKm.toFixed(3)),
      km_day: Number(kmDay.toFixed(3)),
      km_month: Number(kmMonth.toFixed(3)),
      km_total_period: Number(kmTotal.toFixed(3)),
      weekday_ops: weekdayOps,
      saturday_ops: saturdayOps,
      sunday_ops: sundayOps,
      holiday_ops: holidayOps,
      total_ops: weekdayOps + saturdayOps + sundayOps + holidayOps,
      days_in_scope: daysForKm,
      months_in_scope: months,
    };
    profileRows.push(row);
    if (!linesMap.has(p.route_id)) {
      linesMap.set(p.route_id, {
        feed_key: p.feed_key,
        route_id: p.route_id,
        route_label: p.route_label,
        route_long_name: p.route_long_name,
        trips_defined: 0,
        trips_per_weekday: 0,
        trips_per_saturday: 0,
        trips_per_sunday: 0,
        trips_per_holiday: 0,
        avg_trip_km_acc: 0,
        avg_trip_km_count: 0,
        weekday_ops: 0,
        saturday_ops: 0,
        sunday_ops: 0,
        holiday_ops: 0,
        total_ops_days: 0,
        gtfs_year_km: 0,
      });
    }
    const line = linesMap.get(p.route_id);
    line.trips_defined += tripsPerDay;
    line.avg_trip_km_acc += avgTripKm;
    line.avg_trip_km_count += 1;
    if (p.day_type === "weekday") line.trips_per_weekday += tripsPerDay;
    if (p.day_type === "saturday") line.trips_per_saturday += tripsPerDay;
    if (p.day_type === "sunday_holiday") {
      line.trips_per_sunday += tripsPerDay;
      line.trips_per_holiday += tripsPerDay;
    }
    line.weekday_ops += weekdayOps;
    line.saturday_ops += saturdayOps;
    line.sunday_ops += sundayOps;
    line.holiday_ops += holidayOps;
    line.total_ops_days += weekdayOps + saturdayOps + sundayOps + holidayOps;
    line.gtfs_year_km += kmTotal;
  });
  const realizedByLine = new Map(
    realizedRes.rows.map((r) => [String(r.line_key || "").toLowerCase(), Number(r.realized_km || 0)])
  );
  const lines = Array.from(linesMap.values())
    .map((line) => {
      const realizedKm =
        realizedByLine.get(String(line.route_label || "").trim().toLowerCase()) ??
        realizedByLine.get(String(line.route_id || "").trim().toLowerCase()) ??
        0;
      const gtfsYearKm = Number(line.gtfs_year_km || 0);
      return {
        feed_key: line.feed_key,
        route_id: line.route_id,
        route_label: line.route_label,
        route_long_name: line.route_long_name,
        trips_defined: line.trips_defined,
        avg_trip_km: Number((line.avg_trip_km_count ? line.avg_trip_km_acc / line.avg_trip_km_count : 0).toFixed(3)),
        trips_per_weekday: Number(line.trips_per_weekday.toFixed(3)),
        trips_per_saturday: Number(line.trips_per_saturday.toFixed(3)),
        trips_per_sunday: Number(line.trips_per_sunday.toFixed(3)),
        trips_per_holiday: Number(line.trips_per_holiday.toFixed(3)),
        weekday_service_days: counts.all_year.weekday,
        saturday_service_days: counts.all_year.saturday,
        sunday_service_days: counts.all_year.sunday,
        holiday_service_days: counts.all_year.holidayNonSunday,
        weekday_ops: line.weekday_ops,
        saturday_ops: line.saturday_ops,
        sunday_ops: line.sunday_ops,
        holiday_ops: line.holiday_ops,
        total_ops_days: line.total_ops_days,
        gtfs_year_km: Number(gtfsYearKm.toFixed(3)),
        gtfs_month_avg_km: Number((gtfsYearKm / 12).toFixed(3)),
        gtfs_semester_avg_km: Number((gtfsYearKm / 2).toFixed(3)),
        realized_km: Number(realizedKm.toFixed(3)),
        km_gap_vs_realized: Number((gtfsYearKm - realizedKm).toFixed(3)),
        realized_vs_gtfs_pct: gtfsYearKm > 0 ? Number(((realizedKm / gtfsYearKm) * 100).toFixed(2)) : 0,
      };
    })
    .sort((a, b) => String(a.route_label || "").localeCompare(String(b.route_label || ""), "pt"));
  const globalTotals = {
    daily_km: Number(
      profileRows
        .reduce((acc, p) => acc + Number(p.km_day || 0), 0)
        .toFixed(3)
    ),
    monthly_km: Number(
      profileRows
        .reduce((acc, p) => acc + Number(p.km_month || 0), 0)
        .toFixed(3)
    ),
    annual_km: Number(
      lines
        .reduce((acc, l) => acc + Number(l.gtfs_year_km || 0), 0)
        .toFixed(3)
    ),
  };
  return { lines, profileRows, globalTotals, counts };
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

function xmlEscape(value) {
  return String(value == null ? "" : value)
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&apos;");
}

function buildGtfsLinesKml(feedKey, shapeRows) {
  const routeMap = new Map();
  shapeRows.forEach((row) => {
    const routeId = String(row.route_id || "").trim();
    if (!routeId) return;
    if (!routeMap.has(routeId)) {
      routeMap.set(routeId, {
        routeId,
        routeShortName: String(row.route_short_name || "").trim(),
        routeLongName: String(row.route_long_name || "").trim(),
        shapes: new Map(),
      });
    }
    const route = routeMap.get(routeId);
    const shapeId = String(row.shape_id || "").trim() || `${routeId}_shape`;
    if (!route.shapes.has(shapeId)) route.shapes.set(shapeId, []);
    const seq = Number(row.shape_pt_sequence);
    const lat = Number(row.shape_pt_lat);
    const lon = Number(row.shape_pt_lon);
    if (!Number.isFinite(seq) || !Number.isFinite(lat) || !Number.isFinite(lon)) return;
    route.shapes.get(shapeId).push({ seq, lat, lon });
  });

  const placemarks = [];
  routeMap.forEach((route) => {
    const routeLabel = route.routeShortName || route.routeLongName || route.routeId;
    route.shapes.forEach((points, shapeId) => {
      const sorted = points.sort((a, b) => a.seq - b.seq);
      if (sorted.length < 2) return;
      const coords = sorted.map((p) => `${p.lon},${p.lat},0`).join(" ");
      const desc = [
        `<p><strong>Feed:</strong> ${xmlEscape(feedKey)}</p>`,
        `<p><strong>Route ID:</strong> ${xmlEscape(route.routeId)}</p>`,
        `<p><strong>Shape ID:</strong> ${xmlEscape(shapeId)}</p>`,
      ].join("");
      placemarks.push(
        [
          "<Placemark>",
          `<name>${xmlEscape(routeLabel)}</name>`,
          `<description><![CDATA[${desc}]]></description>`,
          "<LineString>",
          "<tessellate>1</tessellate>",
          `<coordinates>${coords}</coordinates>`,
          "</LineString>",
          "</Placemark>",
        ].join("")
      );
    });
  });

  return [
    '<?xml version="1.0" encoding="UTF-8"?>',
    '<kml xmlns="http://www.opengis.net/kml/2.2">',
    "<Document>",
    `<name>${xmlEscape(`linhas_completas_${feedKey}`)}</name>`,
    ...placemarks,
    "</Document>",
    "</kml>",
  ].join("\n");
}

function parseTimeToSeconds(raw) {
  const text = String(raw || "").trim();
  const m = /^(\d{1,2}):(\d{2})(?::(\d{2}))?$/.exec(text);
  if (!m) return null;
  const h = Number(m[1]);
  const min = Number(m[2]);
  const s = Number(m[3] || 0);
  if (!Number.isFinite(h) || !Number.isFinite(min) || !Number.isFinite(s)) return null;
  return h * 3600 + min * 60 + s;
}

function formatSecondsToTime(totalSeconds) {
  const safe = Math.max(0, Math.round(Number(totalSeconds || 0)));
  const h = Math.floor(safe / 3600);
  const m = Math.floor((safe % 3600) / 60);
  const s = safe % 60;
  return `${String(h).padStart(2, "0")}:${String(m).padStart(2, "0")}:${String(s).padStart(2, "0")}`;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const toRad = (d) => (d * Math.PI) / 180;
  const dLat = toRad(lat2 - lat1);
  const dLon = toRad(lon2 - lon1);
  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(lat1)) * Math.cos(toRad(lat2)) * Math.sin(dLon / 2) ** 2;
  return 6371 * 2 * Math.asin(Math.sqrt(a));
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
  await db.query(`ALTER TABLE gtfs_stops ADD COLUMN IF NOT EXISTS stop_code VARCHAR(120)`);
  await db.query(`ALTER TABLE gtfs_stops ADD COLUMN IF NOT EXISTS municipality VARCHAR(120)`);
  await db.query(`ALTER TABLE gtfs_stops ADD COLUMN IF NOT EXISTS parish VARCHAR(120)`);
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
  await db.query(
    `CREATE TABLE IF NOT EXISTS gtfs_admin_boundaries (
      id BIGSERIAL PRIMARY KEY,
      level VARCHAR(20) NOT NULL,
      boundary_name VARCHAR(160) NOT NULL,
      municipality_name VARCHAR(160),
      geometry_type VARCHAR(20) NOT NULL,
      geometry_json JSONB NOT NULL,
      min_lat DOUBLE PRECISION,
      max_lat DOUBLE PRECISION,
      min_lon DOUBLE PRECISION,
      max_lon DOUBLE PRECISION,
      source_tag VARCHAR(160),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
}

async function ensureGtfsEditorIndexes() {
  await ensureGtfsMultiFeedInfra();
  await db.query(
    `CREATE TABLE IF NOT EXISTS gtfs_trip_deactivations (
      trip_id VARCHAR(120) PRIMARY KEY REFERENCES gtfs_trips(trip_id) ON DELETE CASCADE,
      deactivate_effective_from DATE NOT NULL,
      notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_trip_deactivations_effective
     ON gtfs_trip_deactivations(deactivate_effective_from, trip_id)`
  );
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
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_gtfs_admin_boundaries_level_bbox
     ON gtfs_admin_boundaries(level, min_lat, max_lat, min_lon, max_lon)`
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

    if (!routes.length || !trips.length) {
      return res.status(400).json({
        message: "GTFS incompleto. Necessario: routes.txt e trips.txt.",
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
      const stopCode = String(row.stop_code || "").trim() || null;
      const municipality = String(row.municipality || row.concelho || "").trim() || null;
      const parish = String(row.parish || row.freguesia || "").trim() || null;
      if (!stopId || lat == null || lon == null) continue;
      await db.query(
        `INSERT INTO gtfs_stops (stop_id, feed_key, stop_code, stop_name, stop_lat, stop_lon, municipality, parish)
         VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
         ON CONFLICT (stop_id) DO UPDATE
         SET feed_key = EXCLUDED.feed_key,
             stop_code = EXCLUDED.stop_code,
             stop_name = EXCLUDED.stop_name,
             stop_lat = EXCLUDED.stop_lat,
             stop_lon = EXCLUDED.stop_lon,
             municipality = EXCLUDED.municipality,
             parish = EXCLUDED.parish`,
        [stopId, feedKey, stopCode, row.stop_name || null, lat, lon, municipality, parish]
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

router.get("/feeds/:feedKey/export-lines.:format", async (req, res) => {
  const feedKey = normalizeFeedKey(req.params.feedKey);
  const format = String(req.params.format || "").trim().toLowerCase();
  if (!feedKey) return res.status(400).json({ message: "feedKey invalido." });
  if (format !== "kml" && format !== "kmz") {
    return res.status(400).json({ message: "Formato inválido. Use kml ou kmz." });
  }
  try {
    await ensureGtfsEditorIndexes();
    const shapesRes = await db.query(
      `SELECT DISTINCT
         replace(r.route_id, $1 || '::', '') AS route_id,
         COALESCE(r.route_short_name, '') AS route_short_name,
         COALESCE(r.route_long_name, '') AS route_long_name,
         replace(gs.shape_id, $1 || '::', '') AS shape_id,
         gs.shape_pt_lat,
         gs.shape_pt_lon,
         gs.shape_pt_sequence
       FROM gtfs_routes r
       JOIN gtfs_trips t ON t.route_id = r.route_id
       JOIN gtfs_shapes gs ON gs.feed_key = t.feed_key AND gs.shape_id = t.shape_id
       WHERE r.feed_key = $1
       ORDER BY route_id ASC, shape_id ASC, gs.shape_pt_sequence ASC`,
      [feedKey]
    );
    if (!shapesRes.rowCount) {
      return res.status(404).json({ message: "Sem geometria GTFS para exportar as linhas completas." });
    }

    const kmlText = buildGtfsLinesKml(feedKey, shapesRes.rows || []);
    if (format === "kml") {
      res.setHeader("Content-Type", "application/vnd.google-earth.kml+xml");
      res.setHeader("Content-Disposition", `attachment; filename=gtfs_${feedKey}_linhas_completas.kml`);
      return res.status(200).send(Buffer.from(kmlText, "utf8"));
    }

    const zip = new AdmZip();
    zip.addFile(`gtfs_${feedKey}_linhas_completas.kml`, Buffer.from(kmlText, "utf8"));
    const kmzBuffer = zip.toBuffer();
    res.setHeader("Content-Type", "application/vnd.google-earth.kmz");
    res.setHeader("Content-Disposition", `attachment; filename=gtfs_${feedKey}_linhas_completas.kmz`);
    return res.status(200).send(kmzBuffer);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar linhas completas em KML/KMZ." });
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
         COUNT(st.stop_id)::int AS stops_count,
         td.deactivate_effective_from,
         (td.trip_id IS NOT NULL) AS is_deactivated
       FROM gtfs_trips t
       JOIN gtfs_feeds gf ON gf.feed_key = t.feed_key
       LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
       LEFT JOIN gtfs_trip_deactivations td ON td.trip_id = t.trip_id
       WHERE t.route_id = $1
         AND gf.is_active = TRUE
       GROUP BY t.trip_id, t.feed_key, t.route_id, t.trip_headsign, t.direction_id, t.service_id, td.trip_id, td.deactivate_effective_from
       ORDER BY t.trip_id ASC`,
      [routeId]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar viagens GTFS." });
  }
});

router.patch("/editor/trips/:tripId/deactivation", async (req, res) => {
  const tripId = String(req.params.tripId || "").trim();
  const deactivateEffectiveFrom = parseIsoDateInput(req.body?.deactivateEffectiveFrom);
  const notes = String(req.body?.notes || "").trim() || null;
  const active = req.body?.active === true;
  if (!tripId) return res.status(400).json({ message: "Indique tripId." });
  if (!active && !deactivateEffectiveFrom) {
    return res.status(400).json({ message: "Indique deactivateEffectiveFrom para desativar a trip." });
  }
  try {
    await ensureGtfsEditorIndexes();
    const tripRes = await db.query(`SELECT trip_id FROM gtfs_trips WHERE trip_id = $1 LIMIT 1`, [tripId]);
    if (!tripRes.rowCount) return res.status(404).json({ message: "Trip GTFS não encontrada." });
    if (active) {
      await db.query(`DELETE FROM gtfs_trip_deactivations WHERE trip_id = $1`, [tripId]);
      return res.json({ message: "Trip reativada.", tripId, active: true });
    }
    const result = await db.query(
      `INSERT INTO gtfs_trip_deactivations (trip_id, deactivate_effective_from, notes, updated_at)
       VALUES ($1, $2, $3, NOW())
       ON CONFLICT (trip_id) DO UPDATE
       SET deactivate_effective_from = EXCLUDED.deactivate_effective_from,
           notes = EXCLUDED.notes,
           updated_at = NOW()
       RETURNING trip_id, deactivate_effective_from, notes`,
      [tripId, deactivateEffectiveFrom, notes]
    );
    return res.json({
      message: "Trip desativada com data de entrada em vigor.",
      trip: result.rows[0],
      active: false,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar desativação da trip." });
  }
});

router.get("/editor/stops", async (req, res) => {
  try {
    await ensureGtfsEditorIndexes();
    const feedKey = normalizeFeedKey(req.query.feedKey || "");
    const includeInactive = String(req.query.includeInactive || "").toLowerCase() === "true";
    const result = await db.query(
      `SELECT
         s.stop_id,
         COALESCE(s.stop_code, '') AS stop_code,
         s.feed_key,
         COALESCE(s.stop_name, '') AS stop_name,
         s.stop_lat,
         s.stop_lon,
         COALESCE(s.municipality, '') AS municipality,
         COALESCE(s.parish, '') AS parish
       FROM gtfs_stops s
       JOIN gtfs_feeds gf ON gf.feed_key = s.feed_key
       WHERE ($1::text = '' OR s.feed_key = $1)
         AND ($2::boolean = TRUE OR gf.is_active = TRUE)
       ORDER BY NULLIF(TRIM(s.stop_name), '') ASC NULLS LAST, s.stop_id ASC`,
      [feedKey, includeInactive]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar paragens GTFS." });
  }
});

router.patch("/editor/stops/:stopId", async (req, res) => {
  const stopId = String(req.params.stopId || "").trim();
  const stopName = String(req.body?.stopName || "").trim();
  if (!stopId || !stopName) {
    return res.status(400).json({ message: "Indique stopId e stopName válidos." });
  }
  try {
    await ensureGtfsEditorIndexes();
    const result = await db.query(
      `UPDATE gtfs_stops
       SET stop_name = $2
       WHERE stop_id = $1
       RETURNING stop_id, stop_name, feed_key`,
      [stopId, stopName]
    );
    if (!result.rowCount) return res.status(404).json({ message: "Paragem não encontrada." });
    return res.json({ message: "Nome da paragem atualizado.", stop: result.rows[0] });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar nome da paragem." });
  }
});

router.get("/editor/stops/by-municipality", async (req, res) => {
  try {
    await ensureGtfsEditorIndexes();
    const feedKey = normalizeFeedKey(req.query.feedKey || "");
    const includeInactive = String(req.query.includeInactive || "").toLowerCase() === "true";
    const result = await db.query(
      `SELECT
         s.stop_id,
         s.feed_key,
         COALESCE(s.stop_name, '') AS stop_name,
         s.stop_lat,
         s.stop_lon,
         COALESCE(NULLIF(TRIM(s.municipality), ''), 'Sem concelho') AS municipality,
         COALESCE(NULLIF(TRIM(s.parish), ''), 'Sem freguesia') AS parish
       FROM gtfs_stops s
       JOIN gtfs_feeds gf ON gf.feed_key = s.feed_key
       WHERE ($1::text = '' OR s.feed_key = $1)
         AND ($2::boolean = TRUE OR gf.is_active = TRUE)
       ORDER BY municipality ASC, parish ASC, NULLIF(TRIM(s.stop_name), '') ASC NULLS LAST, s.stop_id ASC`,
      [feedKey, includeInactive]
    );
    const byMunicipality = new Map();
    result.rows.forEach((row) => {
      const municipality = String(row.municipality || "Sem concelho");
      const parish = String(row.parish || "Sem freguesia");
      if (!byMunicipality.has(municipality)) byMunicipality.set(municipality, new Map());
      const byParish = byMunicipality.get(municipality);
      if (!byParish.has(parish)) byParish.set(parish, []);
      byParish.get(parish).push({
        stop_id: row.stop_id,
        stop_name: row.stop_name,
        stop_lat: row.stop_lat,
        stop_lon: row.stop_lon,
        feed_key: row.feed_key,
      });
    });
    const data = Array.from(byMunicipality.entries())
      .map(([municipality, parishMap]) => ({
        municipality,
        total_stops: Array.from(parishMap.values()).reduce((acc, list) => acc + list.length, 0),
        parishes: Array.from(parishMap.entries()).map(([parish, stops]) => ({
          parish,
          total_stops: stops.length,
          stops,
        })),
      }))
      .sort((a, b) => a.municipality.localeCompare(b.municipality, "pt-PT"));
    return res.json({ municipalities: data, totalStops: result.rows.length });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar paragens por concelho/freguesia." });
  }
});

router.post("/editor/stops/reverse-geocode", async (req, res) => {
  const feedKey = normalizeFeedKey(req.body?.feedKey || req.query?.feedKey || "");
  const maxStops = Math.max(1, Math.min(500, toInt(req.body?.maxStops || req.query?.maxStops) || 150));
  const forceRefresh = req.body?.forceRefresh === true || String(req.query?.forceRefresh || "").toLowerCase() === "true";
  try {
    await ensureGtfsEditorIndexes();
    const beforeCountRes = await db.query(
      `SELECT COUNT(*)::int AS total
       FROM gtfs_stops
       WHERE ($1::text = '' OR feed_key = $1)
         AND (
           $2::boolean = TRUE
           OR NULLIF(TRIM(COALESCE(municipality, '')), '') IS NULL
           OR NULLIF(TRIM(COALESCE(parish, '')), '') IS NULL
         )
         AND stop_lat IS NOT NULL
         AND stop_lon IS NOT NULL`,
      [feedKey, forceRefresh]
    );
    const remainingBefore = Number(beforeCountRes.rows?.[0]?.total || 0);
    const pendingRes = await db.query(
      `SELECT stop_id, stop_lat, stop_lon
       FROM gtfs_stops
       WHERE ($1::text = '' OR feed_key = $1)
         AND (
           $3::boolean = TRUE
           OR NULLIF(TRIM(COALESCE(municipality, '')), '') IS NULL
           OR NULLIF(TRIM(COALESCE(parish, '')), '') IS NULL
         )
         AND stop_lat IS NOT NULL
         AND stop_lon IS NOT NULL
       ORDER BY stop_id ASC
       LIMIT $2`,
      [feedKey, maxStops, forceRefresh]
    );
    const rows = pendingRes.rows || [];
    let updated = 0;
    let attempted = 0;
    for (const row of rows) {
      attempted += 1;
      const lat = Number(row.stop_lat);
      const lon = Number(row.stop_lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
      const geo = await reverseGeocodeStop(lat, lon).catch(() => ({ municipality: null, parish: null }));
      if (!geo.municipality && !geo.parish) {
        await sleep(1200);
        continue;
      }
      const upd = forceRefresh
        ? await db.query(
            `UPDATE gtfs_stops
             SET municipality = COALESCE($2, municipality),
                 parish = COALESCE($3, parish)
             WHERE stop_id = $1`,
            [row.stop_id, geo.municipality, geo.parish]
          )
        : await db.query(
            `UPDATE gtfs_stops
             SET municipality = COALESCE($2, municipality),
                 parish = COALESCE($3, parish)
             WHERE stop_id = $1
               AND (NULLIF(TRIM(COALESCE(municipality, '')), '') IS NULL
                 OR NULLIF(TRIM(COALESCE(parish, '')), '') IS NULL)`,
            [row.stop_id, geo.municipality, geo.parish]
          );
      if (upd.rowCount) updated += 1;
      await sleep(1200);
    }
    const afterCountRes = await db.query(
      `SELECT COUNT(*)::int AS total
       FROM gtfs_stops
       WHERE ($1::text = '' OR feed_key = $1)
         AND (
           $2::boolean = TRUE
           OR NULLIF(TRIM(COALESCE(municipality, '')), '') IS NULL
           OR NULLIF(TRIM(COALESCE(parish, '')), '') IS NULL
         )
         AND stop_lat IS NOT NULL
         AND stop_lon IS NOT NULL`,
      [feedKey, forceRefresh]
    );
    const remainingAfter = Number(afterCountRes.rows?.[0]?.total || 0);
    return res.json({
      message: "Geocodificação de paragens concluída.",
      feedKey: feedKey || "ativo",
      attempted,
      updated,
      forceRefresh,
      remainingBefore,
      remainingAfter,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao geocodificar paragens por coordenadas." });
  }
});

router.post("/editor/admin-boundaries/import-geojson", async (req, res) => {
  const level = normalizeBoundaryLevel(req.body?.level);
  const sourceTag = String(req.body?.sourceTag || "manual").trim() || "manual";
  const geojsonBase64 = String(req.body?.geojsonBase64 || "").trim();
  if (!level) return res.status(400).json({ message: "Nível inválido. Use municipality ou parish." });
  if (!geojsonBase64) return res.status(400).json({ message: "Envie geojsonBase64." });
  let parsed;
  try {
    const raw = Buffer.from(geojsonBase64, "base64").toString("utf8");
    parsed = JSON.parse(raw);
  } catch (_error) {
    return res.status(400).json({ message: "GeoJSON inválido." });
  }
  const features = Array.isArray(parsed?.features)
    ? parsed.features
    : parsed?.type === "Feature"
      ? [parsed]
      : [];
  if (!features.length) return res.status(400).json({ message: "GeoJSON sem features." });
  const client = await db.pool.connect();
  try {
    await ensureGtfsEditorIndexes();
    await client.query("BEGIN");
    await client.query(`DELETE FROM gtfs_admin_boundaries WHERE level = $1`, [level]);
    let inserted = 0;
    for (const feature of features) {
      const geometry = feature?.geometry || null;
      const props = feature?.properties || {};
      const geometryType = String(geometry?.type || "");
      if (!["Polygon", "MultiPolygon"].includes(geometryType)) continue;
      const boundaryName =
        normalizeGeoText(
          props.name ||
            props.NOME ||
            props.nome ||
            props.municipality ||
            props.concelho ||
            props.parish ||
            props.freguesia
        ) || null;
      if (!boundaryName) continue;
      const municipalityName =
        level === "parish"
          ? normalizeGeoText(props.municipality || props.concelho || props.MUNICIPIO || props.Municipio)
          : boundaryName;
      const bbox = extractBBoxFromCoordinates(geometry.coordinates);
      if (!Number.isFinite(bbox.minLat) || !Number.isFinite(bbox.minLon)) continue;
      await client.query(
        `INSERT INTO gtfs_admin_boundaries (
           level, boundary_name, municipality_name, geometry_type, geometry_json,
           min_lat, max_lat, min_lon, max_lon, source_tag, updated_at
         ) VALUES ($1,$2,$3,$4,$5::jsonb,$6,$7,$8,$9,$10,NOW())`,
        [
          level,
          boundaryName,
          municipalityName || null,
          geometryType,
          JSON.stringify(geometry),
          bbox.minLat,
          bbox.maxLat,
          bbox.minLon,
          bbox.maxLon,
          sourceTag,
        ]
      );
      inserted += 1;
    }
    await client.query("COMMIT");
    return res.json({ message: "Limites administrativos importados.", level, inserted });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao importar limites administrativos." });
  } finally {
    client.release();
  }
});

router.post("/editor/admin-boundaries/import-geoapi-pt", async (_req, res) => {
  const client = await db.pool.connect();
  try {
    await ensureGtfsEditorIndexes();
    const municipalities = await fetchJsonWithRetry("https://json.geoapi.pt/municipios", {
      retries: 4,
      timeoutMs: 25000,
    });
    if (!Array.isArray(municipalities) || !municipalities.length) {
      return res.status(502).json({ message: "Resposta inválida da lista de municípios no geoapi.pt." });
    }
    const municipalityRows = [];
    const parishRows = [];
    let failedMunicipalities = 0;
    const municipalityNames = municipalities
      .map((name) => String(name || "").trim())
      .filter(Boolean);
    const perMunicipality = await mapWithConcurrency(municipalityNames, 10, async (municipalityName) => {
      let parishesPayload = null;
      let detailFailed = false;
      try {
        parishesPayload = await fetchJsonWithRetry(
          `https://json.geoapi.pt/municipio/${encodeURIComponent(municipalityName)}/freguesias`,
          {
            retries: 3,
            timeoutMs: 20000,
          }
        );
      } catch (_error) {
        detailFailed = true;
      }
      return { municipalityName, parishesPayload, detailFailed };
    });

    for (const row of perMunicipality) {
      if (row?.detailFailed) failedMunicipalities += 1;
      if (row?.parishesPayload) {
        const municipalityFeature = normalizeGeoApiMunicipalityPayload(row.parishesPayload);
        if (municipalityFeature?.boundaryName && municipalityFeature?.geometry) municipalityRows.push(municipalityFeature);
        const normalizedParishes = normalizeGeoApiParishFeatures(row.parishesPayload, row.municipalityName);
        parishRows.push(...normalizedParishes);
      }
    }
    if (!municipalityRows.length) {
      return res.status(502).json({ message: "Não foi possível obter geometrias de municípios no geoapi.pt." });
    }
    await client.query("BEGIN");
    await client.query(`DELETE FROM gtfs_admin_boundaries WHERE level IN ('municipality','parish')`);
    let insertedMunicipalities = 0;
    let insertedParishes = 0;
    for (const row of municipalityRows) {
      const geometryType = String(row.geometry?.type || "");
      if (!["Polygon", "MultiPolygon"].includes(geometryType)) continue;
      const bbox = extractBBoxFromCoordinates(row.geometry.coordinates);
      if (!Number.isFinite(bbox.minLat) || !Number.isFinite(bbox.minLon)) continue;
      await client.query(
        `INSERT INTO gtfs_admin_boundaries (
           level, boundary_name, municipality_name, geometry_type, geometry_json,
           min_lat, max_lat, min_lon, max_lon, source_tag, updated_at
         ) VALUES ('municipality',$1,$2,$3,$4::jsonb,$5,$6,$7,$8,'geoapi.pt',NOW())`,
        [
          row.boundaryName,
          row.municipalityName || row.boundaryName,
          geometryType,
          JSON.stringify(row.geometry),
          bbox.minLat,
          bbox.maxLat,
          bbox.minLon,
          bbox.maxLon,
        ]
      );
      insertedMunicipalities += 1;
    }
    for (const row of parishRows) {
      const geometryType = String(row.geometry?.type || "");
      if (!["Polygon", "MultiPolygon"].includes(geometryType)) continue;
      const bbox = extractBBoxFromCoordinates(row.geometry.coordinates);
      if (!Number.isFinite(bbox.minLat) || !Number.isFinite(bbox.minLon)) continue;
      await client.query(
        `INSERT INTO gtfs_admin_boundaries (
           level, boundary_name, municipality_name, geometry_type, geometry_json,
           min_lat, max_lat, min_lon, max_lon, source_tag, updated_at
         ) VALUES ('parish',$1,$2,$3,$4::jsonb,$5,$6,$7,$8,'geoapi.pt',NOW())`,
        [
          row.boundaryName,
          row.municipalityName || null,
          geometryType,
          JSON.stringify(row.geometry),
          bbox.minLat,
          bbox.maxLat,
          bbox.minLon,
          bbox.maxLon,
        ]
      );
      insertedParishes += 1;
    }
    await client.query("COMMIT");
    return res.json({
      message: "Limites administrativos importados automaticamente do geoapi.pt.",
      municipalities: insertedMunicipalities,
      parishes: insertedParishes,
      failedMunicipalities,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    const errorCode = String(_error?.code || "");
    const errorMessage = String(_error?.message || "").slice(0, 220);
    return res.status(500).json({
      message: "Erro ao importar limites automáticos do geoapi.pt.",
      detail: errorMessage || "Erro desconhecido",
      code: errorCode || null,
    });
  } finally {
    client.release();
  }
});

router.post("/editor/stops/assign-admin-boundaries", async (req, res) => {
  const feedKey = normalizeFeedKey(req.body?.feedKey || req.query?.feedKey || "");
  const maxStops = Math.max(1, Math.min(20000, toInt(req.body?.maxStops || req.query?.maxStops) || 6000));
  const forceRefresh = req.body?.forceRefresh === true || String(req.query?.forceRefresh || "").toLowerCase() === "true";
  try {
    await ensureGtfsEditorIndexes();
    const [municipalityRes, parishRes] = await Promise.all([
      db.query(`SELECT * FROM gtfs_admin_boundaries WHERE level = 'municipality'`),
      db.query(`SELECT * FROM gtfs_admin_boundaries WHERE level = 'parish'`),
    ]);
    const municipalities = municipalityRes.rows || [];
    const parishes = parishRes.rows || [];
    if (!municipalities.length) {
      return res.status(400).json({ message: "Sem limites de concelho importados. Importe primeiro o GeoJSON de municípios." });
    }
    const stopsRes = await db.query(
      `SELECT stop_id, stop_lat, stop_lon, municipality, parish
       FROM gtfs_stops
       WHERE ($1::text = '' OR feed_key = $1)
         AND stop_lat IS NOT NULL
         AND stop_lon IS NOT NULL
         AND ($2::boolean = TRUE
           OR NULLIF(TRIM(COALESCE(municipality, '')), '') IS NULL
           OR NULLIF(TRIM(COALESCE(parish, '')), '') IS NULL)
       ORDER BY stop_id ASC
       LIMIT $3`,
      [feedKey, forceRefresh, maxStops]
    );
    let updated = 0;
    for (const stop of stopsRes.rows || []) {
      const lat = Number(stop.stop_lat);
      const lon = Number(stop.stop_lon);
      if (!Number.isFinite(lat) || !Number.isFinite(lon)) continue;
      const pt = point([lon, lat]);
      const municipalityHit = municipalities.find((row) => {
        if (
          lat < Number(row.min_lat) ||
          lat > Number(row.max_lat) ||
          lon < Number(row.min_lon) ||
          lon > Number(row.max_lon)
        ) {
          return false;
        }
        const geom = row.geometry_json || {};
        if (row.geometry_type === "Polygon") return booleanPointInPolygon(pt, polygon(geom.coordinates));
        if (row.geometry_type === "MultiPolygon") return booleanPointInPolygon(pt, multiPolygon(geom.coordinates));
        return false;
      });
      const parishHit = parishes.find((row) => {
        if (municipalityHit && row.municipality_name && municipalityHit.boundary_name) {
          if (String(row.municipality_name).toLowerCase() !== String(municipalityHit.boundary_name).toLowerCase()) return false;
        }
        if (
          lat < Number(row.min_lat) ||
          lat > Number(row.max_lat) ||
          lon < Number(row.min_lon) ||
          lon > Number(row.max_lon)
        ) {
          return false;
        }
        const geom = row.geometry_json || {};
        if (row.geometry_type === "Polygon") return booleanPointInPolygon(pt, polygon(geom.coordinates));
        if (row.geometry_type === "MultiPolygon") return booleanPointInPolygon(pt, multiPolygon(geom.coordinates));
        return false;
      });
      const municipality = municipalityHit?.boundary_name || null;
      const parish = parishHit?.boundary_name || null;
      if (!municipality && !parish) continue;
      const result = await db.query(
        `UPDATE gtfs_stops
         SET municipality = COALESCE($2, municipality),
             parish = COALESCE($3, parish)
         WHERE stop_id = $1`,
        [stop.stop_id, municipality, parish]
      );
      if (result.rowCount) updated += 1;
    }
    return res.json({
      message: "Atribuição administrativa concluída por polígonos.",
      feedKey: feedKey || "ativo",
      processed: (stopsRes.rows || []).length,
      updated,
      boundaries: { municipalities: municipalities.length, parishes: parishes.length },
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atribuir concelho/freguesia por polígonos." });
  }
});

router.post("/editor/line-builder", async (req, res) => {
  const feedKeyInput = normalizeFeedKey(req.body?.feedKey || "default");
  const routeShortName = String(req.body?.routeShortName || "").trim();
  const routeLongName = String(req.body?.routeLongName || "").trim();
  const tripHeadsign = String(req.body?.tripHeadsign || "").trim();
  const serviceIdRaw = String(req.body?.serviceId || "").trim();
  const startDate = parseIsoDateInput(req.body?.startDate);
  const endDate = parseIsoDateInput(req.body?.endDate);
  const directionId = toInt(req.body?.directionId);
  const days = {
    monday: toInt(req.body?.days?.monday) || 0,
    tuesday: toInt(req.body?.days?.tuesday) || 0,
    wednesday: toInt(req.body?.days?.wednesday) || 0,
    thursday: toInt(req.body?.days?.thursday) || 0,
    friday: toInt(req.body?.days?.friday) || 0,
    saturday: toInt(req.body?.days?.saturday) || 0,
    sunday: toInt(req.body?.days?.sunday) || 0,
  };
  const stopItems = Array.isArray(req.body?.stops) ? req.body.stops : [];
  if (!feedKeyInput || !routeShortName || !serviceIdRaw || !startDate || !endDate) {
    return res.status(400).json({ message: "Indique feed, linha, serviceId, startDate e endDate." });
  }
  if (endDate < startDate) {
    return res.status(400).json({ message: "A data final do calendário não pode ser anterior à data inicial." });
  }
  if (stopItems.length < 2) {
    return res.status(400).json({ message: "Indique pelo menos 2 paragens para a nova linha." });
  }
  if (Object.values(days).every((v) => Number(v) !== 1)) {
    return res.status(400).json({ message: "Selecione pelo menos um dia da semana para o calendário." });
  }
  const client = await db.pool.connect();
  try {
    await ensureGtfsEditorIndexes();
    await client.query("BEGIN");
    const feedRes = await client.query(`SELECT feed_key FROM gtfs_feeds WHERE feed_key = $1 LIMIT 1`, [feedKeyInput]);
    if (!feedRes.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Feed GTFS não encontrado para criar linha." });
    }
    const routeId = scopedId(feedKeyInput, `route_${routeShortName.toLowerCase().replace(/\s+/g, "_")}_${Date.now()}`);
    const tripId = scopedId(feedKeyInput, `trip_${routeShortName.toLowerCase().replace(/\s+/g, "_")}_${Date.now()}`);
    const shapeId = scopedId(feedKeyInput, `shape_${routeShortName.toLowerCase().replace(/\s+/g, "_")}_${Date.now()}`);
    const serviceId = scopedId(feedKeyInput, serviceIdRaw);
    await client.query(
      `INSERT INTO gtfs_routes (route_id, feed_key, route_short_name, route_long_name)
       VALUES ($1, $2, $3, $4)`,
      [routeId, feedKeyInput, routeShortName, routeLongName || null]
    );
    await client.query(
      `INSERT INTO gtfs_calendars (
         feed_key, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date, is_active, updated_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, TRUE, NOW())
       ON CONFLICT (feed_key, service_id) DO UPDATE SET
         monday = EXCLUDED.monday,
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
        feedKeyInput,
        serviceId,
        days.monday,
        days.tuesday,
        days.wednesday,
        days.thursday,
        days.friday,
        days.saturday,
        days.sunday,
        startDate,
        endDate,
      ]
    );
    await client.query(
      `INSERT INTO gtfs_trips (trip_id, feed_key, route_id, service_id, trip_headsign, direction_id, shape_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [tripId, feedKeyInput, routeId, serviceId, tripHeadsign || null, directionId ?? 0, shapeId]
    );
    let seq = 1;
    for (const stopItem of stopItems) {
      const existingStopIdRaw = String(stopItem?.stopId || "").trim();
      let stopId = existingStopIdRaw ? scopedId(feedKeyInput, stripFeedPrefix(existingStopIdRaw, feedKeyInput)) : null;
      if (stopId) {
        const stopExistsRes = await client.query(`SELECT stop_id FROM gtfs_stops WHERE stop_id = $1 LIMIT 1`, [stopId]);
        if (!stopExistsRes.rowCount) {
          await client.query("ROLLBACK");
          return res.status(400).json({ message: `Paragem não encontrada: ${existingStopIdRaw}` });
        }
      } else {
        const stopName = String(stopItem?.stopName || "").trim();
        const stopLat = toFloat(stopItem?.stopLat);
        const stopLon = toFloat(stopItem?.stopLon);
        const municipality = String(stopItem?.municipality || "").trim() || null;
        const parish = String(stopItem?.parish || "").trim() || null;
        if (!stopName || stopLat == null || stopLon == null) {
          await client.query("ROLLBACK");
          return res.status(400).json({ message: `Paragem ${seq}: indique stopName, stopLat e stopLon.` });
        }
        stopId = scopedId(feedKeyInput, `custom_${Date.now()}_${seq}`);
        await client.query(
          `INSERT INTO gtfs_stops (stop_id, feed_key, stop_name, stop_lat, stop_lon, municipality, parish)
           VALUES ($1, $2, $3, $4, $5, $6, $7)`,
          [stopId, feedKeyInput, stopName, stopLat, stopLon, municipality, parish]
        );
      }
      const arrivalTime = String(stopItem?.arrivalTime || "").trim() || null;
      const departureTime = String(stopItem?.departureTime || "").trim() || null;
      await client.query(
        `INSERT INTO gtfs_stop_times (feed_key, trip_id, arrival_time, departure_time, stop_id, stop_sequence)
         VALUES ($1, $2, $3, $4, $5, $6)`,
        [feedKeyInput, tripId, arrivalTime, departureTime, stopId, seq]
      );
      seq += 1;
    }
    await client.query("COMMIT");
    return res.json({
      message: "Nova linha GTFS criada com calendário e paragens.",
      route: { route_id: routeId, route_short_name: routeShortName, route_long_name: routeLongName || null },
      trip: { trip_id: tripId, service_id: serviceId },
      stopsCreatedOrLinked: stopItems.length,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao criar nova linha GTFS." });
  } finally {
    client.release();
  }
});

router.post("/editor/trips/create-on-route", async (req, res) => {
  const routeId = String(req.body?.routeId || "").trim();
  const feedKeyRaw = normalizeFeedKey(req.body?.feedKey || "");
  const tripHeadsign = String(req.body?.tripHeadsign || "").trim();
  const serviceIdRaw = String(req.body?.serviceId || "").trim();
  const startDate = parseIsoDateInput(req.body?.startDate);
  const endDate = parseIsoDateInput(req.body?.endDate);
  const directionId = toInt(req.body?.directionId);
  const sourceTripId = String(req.body?.sourceTripId || "").trim() || null;
  const timeShiftMinutes = toInt(req.body?.timeShiftMinutes);
  const days = {
    monday: toInt(req.body?.days?.monday) || 0,
    tuesday: toInt(req.body?.days?.tuesday) || 0,
    wednesday: toInt(req.body?.days?.wednesday) || 0,
    thursday: toInt(req.body?.days?.thursday) || 0,
    friday: toInt(req.body?.days?.friday) || 0,
    saturday: toInt(req.body?.days?.saturday) || 0,
    sunday: toInt(req.body?.days?.sunday) || 0,
  };
  if (!routeId || !serviceIdRaw || !startDate || !endDate) {
    return res.status(400).json({ message: "Indique routeId, serviceId, startDate e endDate." });
  }
  if (endDate < startDate) {
    return res.status(400).json({ message: "A data final do calendário não pode ser anterior à data inicial." });
  }
  if (Object.values(days).every((v) => Number(v) !== 1)) {
    return res.status(400).json({ message: "Selecione pelo menos um dia da semana para o calendário." });
  }

  const client = await db.pool.connect();
  try {
    await ensureGtfsEditorIndexes();
    await client.query("BEGIN");

    const routeRes = await client.query(
      `SELECT route_id, feed_key
       FROM gtfs_routes
       WHERE route_id = $1
       FOR UPDATE`,
      [routeId]
    );
    if (!routeRes.rowCount) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Linha GTFS não encontrada." });
    }
    const feedKey = String(feedKeyRaw || routeRes.rows[0].feed_key || "default");
    const serviceId = scopedId(feedKey, stripFeedPrefix(serviceIdRaw, feedKey));
    const nowTag = Date.now();
    const routeShortFallback = String(routeId).split("::").pop() || "route";
    const tripId = scopedId(feedKey, `trip_${routeShortFallback}_${nowTag}`);
    const shapeId = scopedId(feedKey, `shape_${routeShortFallback}_${nowTag}`);

    await client.query(
      `INSERT INTO gtfs_calendars (
         feed_key, service_id, monday, tuesday, wednesday, thursday, friday, saturday, sunday, start_date, end_date, is_active, updated_at
       ) VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, TRUE, NOW())
       ON CONFLICT (feed_key, service_id) DO UPDATE SET
         monday = EXCLUDED.monday,
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
        days.monday,
        days.tuesday,
        days.wednesday,
        days.thursday,
        days.friday,
        days.saturday,
        days.sunday,
        startDate,
        endDate,
      ]
    );

    await client.query(
      `INSERT INTO gtfs_trips (trip_id, feed_key, route_id, service_id, trip_headsign, direction_id, shape_id)
       VALUES ($1, $2, $3, $4, $5, $6, $7)`,
      [tripId, feedKey, routeId, serviceId, tripHeadsign || null, directionId ?? 0, shapeId]
    );

    let copiedStops = 0;
    if (sourceTripId) {
      const srcRes = await client.query(
        `SELECT feed_key, route_id
         FROM gtfs_trips
         WHERE trip_id = $1
         LIMIT 1`,
        [sourceTripId]
      );
      if (!srcRes.rowCount) {
        await client.query("ROLLBACK");
        return res.status(404).json({ message: "Trip base (sourceTripId) não encontrada." });
      }
      if (String(srcRes.rows[0].route_id || "") !== routeId) {
        await client.query("ROLLBACK");
        return res.status(400).json({ message: "A trip base deve pertencer à mesma linha." });
      }
      const srcStopsRes = await client.query(
        `SELECT stop_sequence, stop_id, arrival_time, departure_time
         FROM gtfs_stop_times
         WHERE trip_id = $1
         ORDER BY stop_sequence ASC`,
        [sourceTripId]
      );
      const shiftSec = (Number.isFinite(Number(timeShiftMinutes)) ? Number(timeShiftMinutes) : 0) * 60;
      for (const st of srcStopsRes.rows) {
        const arrSec = parseTimeToSeconds(st.arrival_time);
        const depSec = parseTimeToSeconds(st.departure_time);
        const arrival = arrSec == null ? st.arrival_time : formatSecondsToTime(arrSec + shiftSec);
        const departure = depSec == null ? st.departure_time : formatSecondsToTime(depSec + shiftSec);
        await client.query(
          `INSERT INTO gtfs_stop_times (feed_key, trip_id, arrival_time, departure_time, stop_id, stop_sequence)
           VALUES ($1, $2, $3, $4, $5, $6)`,
          [feedKey, tripId, arrival, departure, st.stop_id, st.stop_sequence]
        );
        copiedStops += 1;
      }
    }

    await client.query("COMMIT");
    return res.json({
      message: "Nova trip/horário criada na linha existente.",
      trip: { trip_id: tripId, route_id: routeId, service_id: serviceId, feed_key: feedKey },
      calendar: { startDate, endDate, days },
      copiedStops,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao criar trip/horário numa linha existente." });
  } finally {
    client.release();
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
         COALESCE(s.stop_code, '') AS stop_code,
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
      const stopTail = String(stopId || "").includes("::")
        ? String(stopId).split("::").slice(1).join("::")
        : String(stopId);
      const scopedStopId = scopedId(tripFeedKey, stripFeedPrefix(stopId, tripFeedKey));
      const scopedFromTail = scopedId(tripFeedKey, stopTail);
      const candidates = [...new Set([String(stopId).trim(), scopedStopId, scopedFromTail].filter(Boolean))];
      const stopRes = await client.query(
        `SELECT stop_id
         FROM gtfs_stops
         WHERE stop_id = ANY($1::text[])
         LIMIT 1`,
        [candidates]
      );
      if (!stopRes.rowCount) {
        await client.query("ROLLBACK");
        return res.status(404).json({ message: "stopId não existe em gtfs_stops." });
      }
      stopId = String(stopRes.rows[0].stop_id || "");
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

router.patch("/editor/trip-stops/reorder", async (req, res) => {
  const tripId = String(req.body?.tripId || "").trim();
  const stopSequence = toInt(req.body?.stopSequence);
  const direction = String(req.body?.direction || "").trim().toLowerCase();
  const applyScope = String(req.body?.applyScope || "trip").trim().toLowerCase();
  if (!tripId || !Number.isFinite(stopSequence) || stopSequence <= 0) {
    return res.status(400).json({ message: "Indique tripId e stopSequence válidos." });
  }
  if (!["up", "down"].includes(direction)) {
    return res.status(400).json({ message: "direction inválida (use up/down)." });
  }
  if (!["trip", "route"].includes(applyScope)) {
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
    if (!targetTripIds.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Não foram encontradas trips para atualizar." });
    }
    const avgSpeedKmh = 22;
    const dwellSeconds = 20;
    let affectedTrips = 0;
    for (const targetTripId of targetTripIds) {
      const stopsRes = await client.query(
        `SELECT
           st.stop_sequence,
           st.arrival_time,
           st.departure_time,
           st.stop_id,
           s.stop_lat,
           s.stop_lon
         FROM gtfs_stop_times st
         LEFT JOIN gtfs_stops s ON s.stop_id = st.stop_id
         WHERE st.trip_id = $1
         ORDER BY st.stop_sequence ASC`,
        [targetTripId]
      );
      const stops = stopsRes.rows;
      if (stops.length < 2) continue;
      const idx = stops.findIndex((s) => Number(s.stop_sequence) === stopSequence);
      if (idx < 0) continue;
      const swapIdx = direction === "up" ? idx - 1 : idx + 1;
      if (swapIdx < 0 || swapIdx >= stops.length) continue;
      const a = stops[idx];
      const b = stops[swapIdx];
      await client.query(
        `UPDATE gtfs_stop_times
         SET stop_sequence = CASE
           WHEN stop_sequence = $2 THEN $3
           WHEN stop_sequence = $3 THEN $2
           ELSE stop_sequence
         END
         WHERE trip_id = $1
           AND stop_sequence IN ($2, $3)`,
        [targetTripId, Number(a.stop_sequence), Number(b.stop_sequence)]
      );
      const renormRes = await client.query(
        `WITH ordered AS (
           SELECT ctid, ROW_NUMBER() OVER (ORDER BY stop_sequence ASC, ctid) AS new_seq
           FROM gtfs_stop_times
           WHERE trip_id = $1
         )
         UPDATE gtfs_stop_times st
         SET stop_sequence = o.new_seq
         FROM ordered o
         WHERE st.ctid = o.ctid
         RETURNING st.stop_sequence, st.stop_id, st.arrival_time, st.departure_time`,
        [targetTripId]
      );
      const normalized = renormRes.rows
        .map((r) => ({
          stop_sequence: Number(r.stop_sequence),
          stop_id: r.stop_id,
          arrival_time: r.arrival_time,
          departure_time: r.departure_time,
        }))
        .sort((x, y) => x.stop_sequence - y.stop_sequence);
      const locRes = await client.query(
        `SELECT stop_id, stop_lat, stop_lon FROM gtfs_stops WHERE stop_id = ANY($1::text[])`,
        [normalized.map((s) => s.stop_id)]
      );
      const locById = new Map(
        locRes.rows.map((r) => [String(r.stop_id || ""), { lat: Number(r.stop_lat), lon: Number(r.stop_lon) }])
      );
      let currentSeconds =
        parseTimeToSeconds(normalized[0]?.departure_time) ??
        parseTimeToSeconds(normalized[0]?.arrival_time) ??
        6 * 3600;
      for (let i = 0; i < normalized.length; i += 1) {
        if (i > 0) {
          const prev = normalized[i - 1];
          const curr = normalized[i];
          const prevLoc = locById.get(String(prev.stop_id || ""));
          const currLoc = locById.get(String(curr.stop_id || ""));
          let travelSeconds = 60;
          if (
            prevLoc &&
            currLoc &&
            Number.isFinite(prevLoc.lat) &&
            Number.isFinite(prevLoc.lon) &&
            Number.isFinite(currLoc.lat) &&
            Number.isFinite(currLoc.lon)
          ) {
            const km = haversineKm(prevLoc.lat, prevLoc.lon, currLoc.lat, currLoc.lon);
            travelSeconds = Math.max(45, Math.round((km / avgSpeedKmh) * 3600));
          }
          currentSeconds += travelSeconds;
        }
        const arr = formatSecondsToTime(currentSeconds);
        const dep = formatSecondsToTime(currentSeconds + dwellSeconds);
        await client.query(
          `UPDATE gtfs_stop_times
           SET arrival_time = $3,
               departure_time = $4
           WHERE trip_id = $1
             AND stop_sequence = $2`,
          [targetTripId, normalized[i].stop_sequence, arr, dep]
        );
        currentSeconds += dwellSeconds;
      }
      affectedTrips += 1;
    }
    await client.query("COMMIT");
    return res.json({
      message:
        applyScope === "route"
          ? `Paragens reordenadas e horários recalculados em ${affectedTrips} trips da carreira.`
          : "Paragem reordenada e horários recalculados na trip.",
      affectedTrips,
      applyScope,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao reordenar paragem da trip GTFS." });
  } finally {
    client.release();
  }
});

router.patch("/editor/trip-stops/reorder-position", async (req, res) => {
  const tripId = String(req.body?.tripId || "").trim();
  const fromStopSequence = toInt(req.body?.fromStopSequence);
  const toStopSequence = toInt(req.body?.toStopSequence);
  const applyScope = String(req.body?.applyScope || "trip").trim().toLowerCase();
  if (!tripId || !Number.isFinite(fromStopSequence) || !Number.isFinite(toStopSequence)) {
    return res.status(400).json({ message: "Indique tripId, fromStopSequence e toStopSequence válidos." });
  }
  if (!["trip", "route"].includes(applyScope)) {
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
    if (!targetTripIds.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Não foram encontradas trips para atualizar." });
    }

    const avgSpeedKmh = 22;
    const dwellSeconds = 20;
    let affectedTrips = 0;
    for (const targetTripId of targetTripIds) {
      const stopsRes = await client.query(
        `SELECT
           st.id AS row_id,
           st.stop_sequence,
           st.arrival_time,
           st.departure_time,
           st.stop_id,
           s.stop_lat,
           s.stop_lon
         FROM gtfs_stop_times st
         LEFT JOIN gtfs_stops s ON s.stop_id = st.stop_id
         WHERE st.trip_id = $1
         ORDER BY st.stop_sequence ASC`,
        [targetTripId]
      );
      const stops = stopsRes.rows;
      if (stops.length < 2) continue;
      const fromIdx = stops.findIndex((s) => Number(s.stop_sequence) === fromStopSequence);
      const toIdx = stops.findIndex((s) => Number(s.stop_sequence) === toStopSequence);
      if (fromIdx < 0 || toIdx < 0 || fromIdx === toIdx) continue;
      const moved = stops.splice(fromIdx, 1)[0];
      stops.splice(toIdx, 0, moved);

      // Two-phase sequence write avoids transient unique-sequence collisions.
      for (let i = 0; i < stops.length; i += 1) {
        await client.query(
          `UPDATE gtfs_stop_times
           SET stop_sequence = $3
           WHERE trip_id = $1
             AND id = $2`,
          [targetTripId, Number(stops[i].row_id), i + 10001]
        );
      }
      for (let i = 0; i < stops.length; i += 1) {
        await client.query(
          `UPDATE gtfs_stop_times
           SET stop_sequence = $3
           WHERE trip_id = $1
             AND id = $2`,
          [targetTripId, Number(stops[i].row_id), i + 1]
        );
      }

      const normalizedRes = await client.query(
        `SELECT st.stop_sequence, st.stop_id, st.arrival_time, st.departure_time
         FROM gtfs_stop_times st
         WHERE st.trip_id = $1
         ORDER BY st.stop_sequence ASC`,
        [targetTripId]
      );
      const normalized = normalizedRes.rows.map((r) => ({
        stop_sequence: Number(r.stop_sequence),
        stop_id: r.stop_id,
        arrival_time: r.arrival_time,
        departure_time: r.departure_time,
      }));
      const locRes = await client.query(
        `SELECT stop_id, stop_lat, stop_lon FROM gtfs_stops WHERE stop_id = ANY($1::text[])`,
        [normalized.map((s) => s.stop_id)]
      );
      const locById = new Map(
        locRes.rows.map((r) => [String(r.stop_id || ""), { lat: Number(r.stop_lat), lon: Number(r.stop_lon) }])
      );
      let currentSeconds =
        parseTimeToSeconds(normalized[0]?.departure_time) ??
        parseTimeToSeconds(normalized[0]?.arrival_time) ??
        6 * 3600;
      for (let i = 0; i < normalized.length; i += 1) {
        if (i > 0) {
          const prev = normalized[i - 1];
          const curr = normalized[i];
          const prevLoc = locById.get(String(prev.stop_id || ""));
          const currLoc = locById.get(String(curr.stop_id || ""));
          let travelSeconds = 60;
          if (
            prevLoc &&
            currLoc &&
            Number.isFinite(prevLoc.lat) &&
            Number.isFinite(prevLoc.lon) &&
            Number.isFinite(currLoc.lat) &&
            Number.isFinite(currLoc.lon)
          ) {
            const km = haversineKm(prevLoc.lat, prevLoc.lon, currLoc.lat, currLoc.lon);
            travelSeconds = Math.max(45, Math.round((km / avgSpeedKmh) * 3600));
          }
          currentSeconds += travelSeconds;
        }
        const arr = formatSecondsToTime(currentSeconds);
        const dep = formatSecondsToTime(currentSeconds + dwellSeconds);
        await client.query(
          `UPDATE gtfs_stop_times
           SET arrival_time = $3,
               departure_time = $4
           WHERE trip_id = $1
             AND stop_sequence = $2`,
          [targetTripId, normalized[i].stop_sequence, arr, dep]
        );
        currentSeconds += dwellSeconds;
      }
      affectedTrips += 1;
    }

    await client.query("COMMIT");
    return res.json({
      message:
        applyScope === "route"
          ? `Paragens reposicionadas e horários recalculados em ${affectedTrips} trips da carreira.`
          : "Paragem reposicionada e horários recalculados na trip.",
      affectedTrips,
      applyScope,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao reposicionar paragem da trip GTFS." });
  } finally {
    client.release();
  }
});

router.patch("/editor/trip-stops/spine/normalize", async (req, res) => {
  const tripId = String(req.body?.tripId || "").trim();
  const applyScope = String(req.body?.applyScope || "trip")
    .trim()
    .toLowerCase();
  if (!tripId) {
    return res.status(400).json({ message: "Indique tripId." });
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
    let affectedTrips = 0;
    for (const targetTripId of targetTripIds) {
      const updateRes = await client.query(
        `WITH ordered AS (
           SELECT ctid, ROW_NUMBER() OVER (ORDER BY stop_sequence ASC, stop_id ASC) AS new_seq
           FROM gtfs_stop_times
           WHERE trip_id = $1
         )
         UPDATE gtfs_stop_times st
         SET stop_sequence = o.new_seq
         FROM ordered o
         WHERE st.ctid = o.ctid`,
        [targetTripId]
      );
      if (updateRes.rowCount > 0) affectedTrips += 1;
    }
    await client.query("COMMIT");
    return res.json({
      message:
        applyScope === "route"
          ? `Espinha normalizada em ${affectedTrips} trips da carreira.`
          : "Espinha da trip normalizada.",
      tripId,
      applyScope,
      affectedTrips,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao normalizar espinha da trip/carreira." });
  } finally {
    client.release();
  }
});

router.patch("/editor/trip-stops/time", async (req, res) => {
  const tripId = String(req.body?.tripId || "").trim();
  const stopSequence = toInt(req.body?.stopSequence);
  const arrivalTimeRaw = String(req.body?.arrivalTime || "").trim();
  const departureTimeRaw = String(req.body?.departureTime || "").trim();
  const applyScope = String(req.body?.applyScope || "trip").trim().toLowerCase();
  if (!tripId || !Number.isFinite(stopSequence) || stopSequence <= 0) {
    return res.status(400).json({ message: "Indique tripId e stopSequence válidos." });
  }
  if (!["trip", "route"].includes(applyScope)) {
    return res.status(400).json({ message: "applyScope inválido (use trip ou route)." });
  }
  const arrivalSeconds = arrivalTimeRaw ? parseTimeToSeconds(arrivalTimeRaw) : null;
  const departureSeconds = departureTimeRaw ? parseTimeToSeconds(departureTimeRaw) : null;
  const arrivalTime = Number.isFinite(arrivalSeconds) ? formatSecondsToTime(arrivalSeconds) : null;
  const departureTime = Number.isFinite(departureSeconds) ? formatSecondsToTime(departureSeconds) : null;
  if (!arrivalTime && !departureTime) {
    return res.status(400).json({ message: "Indique arrivalTime e/ou departureTime válidos (HH:MM:SS)." });
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
    if (!targetTripIds.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Não foram encontradas trips para atualizar." });
    }
    let affectedTrips = 0;
    for (const targetTripId of targetTripIds) {
      const updRes = await client.query(
        `UPDATE gtfs_stop_times
         SET arrival_time = COALESCE($3, arrival_time),
             departure_time = COALESCE($4, departure_time)
         WHERE trip_id = $1
           AND stop_sequence = $2`,
        [targetTripId, stopSequence, arrivalTime, departureTime]
      );
      if (updRes.rowCount) affectedTrips += 1;
    }
    await client.query("COMMIT");
    return res.json({
      message:
        applyScope === "route"
          ? `Horários atualizados na sequência ${stopSequence} em ${affectedTrips} trips da carreira.`
          : "Horário da paragem atualizado na trip.",
      affectedTrips,
      applyScope,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao atualizar horário da paragem na trip GTFS." });
  } finally {
    client.release();
  }
});

router.patch("/editor/trip-stops/time/auto-adjust", async (req, res) => {
  const tripId = String(req.body?.tripId || "").trim();
  const applyScope = String(req.body?.applyScope || "trip").trim().toLowerCase();
  if (!tripId) return res.status(400).json({ message: "Indique tripId." });
  if (!["trip", "route"].includes(applyScope)) {
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
    if (!targetTripIds.length) {
      await client.query("ROLLBACK");
      return res.status(404).json({ message: "Não foram encontradas trips para atualizar." });
    }
    const avgSpeedKmh = 22;
    const dwellSeconds = 20;
    let affectedTrips = 0;
    for (const targetTripId of targetTripIds) {
      const stopsRes = await client.query(
        `SELECT
           st.stop_sequence,
           st.stop_id,
           st.arrival_time,
           st.departure_time,
           s.stop_lat,
           s.stop_lon
         FROM gtfs_stop_times st
         LEFT JOIN gtfs_stops s ON s.stop_id = st.stop_id
         WHERE st.trip_id = $1
         ORDER BY st.stop_sequence ASC`,
        [targetTripId]
      );
      const stops = stopsRes.rows.map((r) => ({
        seq: Number(r.stop_sequence),
        stopId: String(r.stop_id || ""),
        arr: r.arrival_time,
        dep: r.departure_time,
        lat: Number(r.stop_lat),
        lon: Number(r.stop_lon),
      }));
      if (!stops.length) continue;
      let currentSeconds =
        parseTimeToSeconds(stops[0]?.dep) ??
        parseTimeToSeconds(stops[0]?.arr) ??
        6 * 3600;
      for (let i = 0; i < stops.length; i += 1) {
        if (i > 0) {
          const prev = stops[i - 1];
          const curr = stops[i];
          let travelSeconds = 60;
          if (
            Number.isFinite(prev.lat) &&
            Number.isFinite(prev.lon) &&
            Number.isFinite(curr.lat) &&
            Number.isFinite(curr.lon)
          ) {
            const km = haversineKm(prev.lat, prev.lon, curr.lat, curr.lon);
            travelSeconds = Math.max(45, Math.round((km / avgSpeedKmh) * 3600));
          }
          currentSeconds += travelSeconds;
        }
        const arr = formatSecondsToTime(currentSeconds);
        const dep = formatSecondsToTime(currentSeconds + dwellSeconds);
        await client.query(
          `UPDATE gtfs_stop_times
           SET arrival_time = $3,
               departure_time = $4
           WHERE trip_id = $1
             AND stop_sequence = $2`,
          [targetTripId, stops[i].seq, arr, dep]
        );
        currentSeconds += dwellSeconds;
      }
      affectedTrips += 1;
    }
    await client.query("COMMIT");
    return res.json({
      message:
        applyScope === "route"
          ? `Tempos ajustados automaticamente em ${affectedTrips} trips da carreira.`
          : "Tempos ajustados automaticamente na trip.",
      affectedTrips,
      applyScope,
    });
  } catch (_error) {
    await client.query("ROLLBACK").catch(() => {});
    return res.status(500).json({ message: "Erro ao ajustar tempos automaticamente." });
  } finally {
    client.release();
  }
});

router.get("/analytics/overview", async (req, res) => {
  try {
    await ensureGtfsEditorIndexes();
    const feedKey = normalizeFeedKey(req.query.feedKey || "");
    const periodMode = String(req.query.periodMode || "").trim().toLowerCase();
    const startDateInput = parseIsoDateInput(req.query.startDate);
    const endDateInput = parseIsoDateInput(req.query.endDate);
    const municipalHoliday = parseMunicipalHolidayInput(req.query.municipalHoliday);
    if (startDateInput && endDateInput && startDateInput > endDateInput) {
      return res.status(400).json({ message: "Intervalo inválido: startDate maior que endDate." });
    }
    const { startDate, endDate, effectiveStartDate, gtfsEffectiveFrom, calendarEffectiveFrom } =
      await resolveAnalyticsPeriod(feedKey, startDateInput, endDateInput, periodMode);
    const holidayDates = buildHolidayDateList(startDate, endDate, municipalHoliday);
    const result = await db.query(
      `WITH selected_routes AS (
         SELECT r.feed_key, r.route_id, r.route_short_name, r.route_long_name
         FROM gtfs_routes r
         JOIN gtfs_feeds f ON f.feed_key = r.feed_key
         WHERE ($1::text = '' OR r.feed_key = $1)
           AND f.is_active = TRUE
       ),
       period AS (
         SELECT
           COALESCE($2::date, CURRENT_DATE)::date AS start_date,
           COALESCE(
             $3::date,
             (
               COALESCE($2::date, CURRENT_DATE::date)
               + INTERVAL '1 year' - INTERVAL '1 day'
             )::date
           ) AS end_date
       ),
       trip_shapes AS (
         SELECT
           t.feed_key,
           t.route_id,
           t.trip_id,
           t.service_id,
           t.shape_id,
           COALESCE(
             SUM(
               CASE
                 WHEN prev_lat IS NULL OR prev_lon IS NULL THEN 0
                 ELSE
                   6371 * 2 * ASIN(
                     SQRT(
                       POWER(SIN(RADIANS((gs.shape_pt_lat - prev_lat) / 2)), 2) +
                       COS(RADIANS(prev_lat)) * COS(RADIANS(gs.shape_pt_lat)) *
                       POWER(SIN(RADIANS((gs.shape_pt_lon - prev_lon) / 2)), 2)
                     )
                   )
               END
             ),
             0
           )::numeric(12,3) AS trip_km
        FROM gtfs_trips t
        JOIN selected_routes sr ON sr.feed_key = t.feed_key AND sr.route_id = t.route_id
         LEFT JOIN (
           SELECT
             feed_key,
             shape_id,
             shape_pt_sequence,
             shape_pt_lat,
             shape_pt_lon,
             LAG(shape_pt_lat) OVER (PARTITION BY feed_key, shape_id ORDER BY shape_pt_sequence ASC) AS prev_lat,
             LAG(shape_pt_lon) OVER (PARTITION BY feed_key, shape_id ORDER BY shape_pt_sequence ASC) AS prev_lon
           FROM gtfs_shapes
         ) gs ON gs.feed_key = t.feed_key AND gs.shape_id = t.shape_id
         GROUP BY t.feed_key, t.route_id, t.trip_id, t.service_id, t.shape_id
       ),
       trip_calendar AS (
         SELECT
           ts.feed_key,
           ts.route_id,
           ts.trip_id,
           ts.trip_km,
           ts.service_id,
           c.service_id AS calendar_service_id,
           c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday,
           c.start_date,
           c.end_date
         FROM trip_shapes ts
         LEFT JOIN gtfs_calendars c
           ON c.feed_key = ts.feed_key
          AND (
            c.service_id = ts.service_id
            OR c.service_id = (ts.feed_key || '::' || ts.service_id)
            OR ts.service_id = (ts.feed_key || '::' || c.service_id)
          )
       ),
       trip_active_dates AS (
         SELECT
           tc.feed_key,
           tc.route_id,
           tc.trip_id,
           tc.trip_km,
           gs.d::date AS op_date
         FROM trip_calendar tc
         CROSS JOIN period p
         JOIN LATERAL generate_series(
           GREATEST(COALESCE(tc.start_date, p.start_date), p.start_date),
           LEAST(COALESCE(tc.end_date, p.end_date), p.end_date),
           INTERVAL '1 day'
         ) gs(d) ON TRUE
        LEFT JOIN gtfs_calendar_dates cd_remove
          ON cd_remove.feed_key = tc.feed_key
          AND (
            cd_remove.service_id = tc.service_id
            OR cd_remove.service_id = COALESCE(tc.calendar_service_id, '')
            OR cd_remove.service_id = (tc.feed_key || '::' || tc.service_id)
          )
          AND cd_remove.calendar_date = gs.d::date
          AND cd_remove.exception_type = 2
         WHERE (
             (EXTRACT(ISODOW FROM gs.d) = 1 AND COALESCE(tc.monday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 2 AND COALESCE(tc.tuesday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 3 AND COALESCE(tc.wednesday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 4 AND COALESCE(tc.thursday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 5 AND COALESCE(tc.friday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 6 AND COALESCE(tc.saturday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 7 AND COALESCE(tc.sunday, 0) = 1)
           )
          AND cd_remove.id IS NULL
         UNION ALL
         SELECT
           tc.feed_key,
           tc.route_id,
           tc.trip_id,
           tc.trip_km,
           cd_add.calendar_date::date AS op_date
         FROM trip_calendar tc
         JOIN period p ON TRUE
         JOIN gtfs_calendar_dates cd_add
           ON cd_add.feed_key = tc.feed_key
          AND (
            cd_add.service_id = tc.service_id
            OR cd_add.service_id = COALESCE(tc.calendar_service_id, '')
            OR cd_add.service_id = (tc.feed_key || '::' || tc.service_id)
          )
          AND cd_add.exception_type = 1
          AND cd_add.calendar_date BETWEEN p.start_date AND p.end_date
        UNION ALL
        SELECT
          tc.feed_key,
          tc.route_id,
          tc.trip_id,
          tc.trip_km,
          gs.d::date AS op_date
        FROM trip_calendar tc
        CROSS JOIN period p
        JOIN LATERAL generate_series(p.start_date, p.end_date, INTERVAL '1 day') gs(d) ON TRUE
        WHERE tc.calendar_service_id IS NULL
       ),
       trip_day_counts AS (
         SELECT
           tad.feed_key,
           tad.route_id,
           tad.trip_id,
           MAX(tad.trip_km)::numeric(12,3) AS trip_km,
           COUNT(*) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) BETWEEN 1 AND 5 AND NOT (tad.op_date = ANY($4::date[])))::int AS weekday_days,
           COUNT(*) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) = 6)::int AS saturday_days,
           COUNT(*) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) = 7)::int AS sunday_days,
           COUNT(*) FILTER (WHERE tad.op_date = ANY($4::date[]))::int AS holiday_days,
           COUNT(*)::int AS total_active_days
         FROM (
           SELECT DISTINCT feed_key, route_id, trip_id, trip_km, op_date
           FROM trip_active_dates
         ) tad
         GROUP BY tad.feed_key, tad.route_id, tad.trip_id
       ),
       route_service_days AS (
         SELECT
           tad.feed_key,
           tad.route_id,
           COUNT(DISTINCT tad.op_date) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) BETWEEN 1 AND 5 AND NOT (tad.op_date = ANY($4::date[])))::int AS weekday_service_days,
           COUNT(DISTINCT tad.op_date) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) = 6)::int AS saturday_service_days,
           COUNT(DISTINCT tad.op_date) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) = 7)::int AS sunday_service_days,
           COUNT(DISTINCT tad.op_date) FILTER (WHERE tad.op_date = ANY($4::date[]))::int AS holiday_service_days
         FROM (
           SELECT DISTINCT feed_key, route_id, op_date
           FROM trip_active_dates
         ) tad
         GROUP BY tad.feed_key, tad.route_id
       ),
       route_agg AS (
         SELECT
           sr.feed_key,
           sr.route_id,
           COALESCE(NULLIF(TRIM(sr.route_short_name), ''), sr.route_id) AS route_label,
           COALESCE(NULLIF(TRIM(sr.route_long_name), ''), '-') AS route_long_name,
           COUNT(DISTINCT ts.trip_id)::int AS trips_defined,
           AVG(ts.trip_km)::numeric(12,3) AS avg_trip_km,
           COALESCE(rsd.weekday_service_days, 0)::int AS weekday_service_days,
           COALESCE(rsd.saturday_service_days, 0)::int AS saturday_service_days,
           COALESCE(rsd.sunday_service_days, 0)::int AS sunday_service_days,
           COALESCE(rsd.holiday_service_days, 0)::int AS holiday_service_days,
           COUNT(DISTINCT CASE WHEN COALESCE(tdc.weekday_days, 0) > 0 THEN ts.trip_id END)::int AS weekday_trips_defined,
           COUNT(DISTINCT CASE WHEN COALESCE(tdc.saturday_days, 0) > 0 THEN ts.trip_id END)::int AS saturday_trips_defined,
           COUNT(DISTINCT CASE WHEN COALESCE(tdc.sunday_days, 0) > 0 THEN ts.trip_id END)::int AS sunday_trips_defined,
           COUNT(DISTINCT CASE WHEN COALESCE(tdc.holiday_days, 0) > 0 THEN ts.trip_id END)::int AS holiday_trips_defined,
           COALESCE(SUM(tdc.weekday_days), 0)::int AS weekday_ops,
           COALESCE(SUM(tdc.saturday_days), 0)::int AS saturday_ops,
           COALESCE(SUM(tdc.sunday_days), 0)::int AS sunday_ops,
           COALESCE(SUM(tdc.holiday_days), 0)::int AS holiday_ops,
           COALESCE(SUM(tdc.total_active_days), 0)::int AS total_ops_days,
           COALESCE(SUM(tdc.trip_km * tdc.total_active_days), 0)::numeric(12,3) AS gtfs_year_km
        FROM selected_routes sr
        LEFT JOIN trip_shapes ts ON ts.feed_key = sr.feed_key AND ts.route_id = sr.route_id
        LEFT JOIN trip_day_counts tdc ON tdc.feed_key = ts.feed_key AND tdc.route_id = ts.route_id AND tdc.trip_id = ts.trip_id
         LEFT JOIN route_service_days rsd ON rsd.feed_key = sr.feed_key AND rsd.route_id = sr.route_id
         GROUP BY sr.feed_key, sr.route_id, route_label, route_long_name
           , rsd.weekday_service_days, rsd.saturday_service_days, rsd.sunday_service_days, rsd.holiday_service_days
       ),
      route_trip_profiles AS (
        SELECT
          ts.feed_key,
          ts.route_id,
          COUNT(DISTINCT CASE
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-DF'
              THEN NULL
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-S'
              THEN NULL
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-U'
              THEN ts.trip_id
            ELSE NULL
          END)::int AS weekday_trips_defined,
          COUNT(DISTINCT CASE
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-S'
              THEN ts.trip_id
            ELSE NULL
          END)::int AS saturday_trips_defined,
          COUNT(DISTINCT CASE
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-DF'
              THEN ts.trip_id
            ELSE NULL
          END)::int AS sunday_holiday_trips_defined
        FROM trip_shapes ts
        GROUP BY ts.feed_key, ts.route_id
      ),
       realized_by_line AS (
         SELECT
           LOWER(TRIM(COALESCE(s.line_code, ''))) AS line_key,
           COALESCE(SUM(COALESCE(s.total_km, 0)), 0)::numeric(12,3) AS realized_km
         FROM services s
         WHERE LOWER(TRIM(COALESCE(s.status::text, ''))) = 'completed'
         GROUP BY LOWER(TRIM(COALESCE(s.line_code, '')))
       )
       SELECT
         ra.feed_key,
         ra.route_id,
         ra.route_label,
         ra.route_long_name,
         ra.trips_defined,
         COALESCE(ra.avg_trip_km, 0)::numeric(12,3) AS avg_trip_km,
        COALESCE(rtp.weekday_trips_defined, 0)::numeric(12,3) AS trips_per_weekday,
        COALESCE(rtp.saturday_trips_defined, 0)::numeric(12,3) AS trips_per_saturday,
        COALESCE(rtp.sunday_holiday_trips_defined, 0)::numeric(12,3) AS trips_per_sunday,
        COALESCE(rtp.sunday_holiday_trips_defined, 0)::numeric(12,3) AS trips_per_holiday,
         ra.weekday_service_days,
         ra.saturday_service_days,
         ra.sunday_service_days,
         ra.holiday_service_days,
         ra.weekday_ops,
         ra.saturday_ops,
         ra.sunday_ops,
         ra.holiday_ops,
         ra.total_ops_days,
         COALESCE(ra.gtfs_year_km, 0)::numeric(12,3) AS gtfs_year_km,
         (COALESCE(ra.gtfs_year_km, 0) / 12.0)::numeric(12,3) AS gtfs_month_avg_km,
         (COALESCE(ra.gtfs_year_km, 0) / 2.0)::numeric(12,3) AS gtfs_semester_avg_km,
         COALESCE(rbl.realized_km, 0)::numeric(12,3) AS realized_km,
         (COALESCE(ra.gtfs_year_km, 0) - COALESCE(rbl.realized_km, 0))::numeric(12,3) AS km_gap_vs_realized,
         CASE
           WHEN COALESCE(ra.gtfs_year_km, 0) <= 0 THEN 0::numeric(6,2)
           ELSE ((COALESCE(rbl.realized_km, 0) / ra.gtfs_year_km) * 100)::numeric(6,2)
         END AS realized_vs_gtfs_pct
       FROM route_agg ra
      LEFT JOIN route_trip_profiles rtp
        ON rtp.feed_key = ra.feed_key
       AND rtp.route_id = ra.route_id
       LEFT JOIN realized_by_line rbl
         ON rbl.line_key = LOWER(TRIM(COALESCE(ra.route_label, '')))
         OR rbl.line_key = LOWER(TRIM(COALESCE(ra.route_id, '')))
       ORDER BY route_label ASC, route_id ASC`,
      [feedKey, startDate, endDate, holidayDates]
    );
    const exceptionsRes = await db.query(
      `WITH selected_routes AS (
         SELECT r.feed_key, r.route_id
         FROM gtfs_routes r
         JOIN gtfs_feeds f ON f.feed_key = r.feed_key
         WHERE ($1::text = '' OR r.feed_key = $1)
           AND f.is_active = TRUE
       ),
       selected_services AS (
         SELECT DISTINCT t.feed_key, t.service_id
         FROM gtfs_trips t
         JOIN selected_routes sr ON sr.feed_key = t.feed_key AND sr.route_id = t.route_id
       ),
       period AS (
         SELECT
           COALESCE($2::date, CURRENT_DATE)::date AS start_date,
           COALESCE(
             $3::date,
             (
               COALESCE($2::date, CURRENT_DATE::date)
               + INTERVAL '1 year' - INTERVAL '1 day'
             )::date
           ) AS end_date
       ),
       matched_exceptions AS (
         SELECT cd.id, cd.calendar_date, cd.exception_type
         FROM gtfs_calendar_dates cd
         JOIN period p ON TRUE
         WHERE cd.calendar_date BETWEEN p.start_date AND p.end_date
           AND EXISTS (
             SELECT 1
             FROM selected_services ss
             WHERE ss.feed_key = cd.feed_key
               AND (
                 cd.service_id = ss.service_id
                 OR cd.service_id = (ss.feed_key || '::' || ss.service_id)
                 OR ss.service_id = (ss.feed_key || '::' || cd.service_id)
               )
           )
       )
       SELECT
         COUNT(*) FILTER (WHERE exception_type = 1)::int AS added_entries,
         COUNT(*) FILTER (WHERE exception_type = 2)::int AS removed_entries,
         COUNT(DISTINCT calendar_date) FILTER (WHERE exception_type = 1)::int AS added_days,
         COUNT(DISTINCT calendar_date) FILTER (WHERE exception_type = 2)::int AS removed_days
       FROM matched_exceptions`,
      [feedKey, startDate, endDate]
    );
    const exceptions = exceptionsRes.rows[0] || {};
    return res.json({
      assumptions: {
        period: "1 ano operacional baseado no calendario GTFS",
        startDate,
        endDate,
        effectiveStartDate,
        gtfsEffectiveFrom,
        calendarEffectiveFrom,
        municipalHoliday: municipalHoliday?.label || null,
        timezone: "Europe/Lisbon",
        dstAuto: true,
      },
      calendarExceptions: {
        addedEntries: Number(exceptions.added_entries || 0),
        removedEntries: Number(exceptions.removed_entries || 0),
        addedDays: Number(exceptions.added_days || 0),
        removedDays: Number(exceptions.removed_days || 0),
      },
      lines: result.rows,
    });
  } catch (error) {
    console.error("[gtfs analytics overview] erro ao gerar analise", error);
    return res.status(500).json({ message: "Erro ao gerar analise GTFS por linha.", detail: error?.message || null });
  }
});

router.get("/analytics/line-detail", async (req, res) => {
  const routeId = String(req.query.routeId || "").trim();
  if (!routeId) return res.status(400).json({ message: "Indique routeId." });
  try {
    await ensureGtfsEditorIndexes();
    const lineRes = await db.query(
      `SELECT route_id, feed_key, route_short_name, route_long_name
       FROM gtfs_routes
       WHERE route_id = $1
       LIMIT 1`,
      [routeId]
    );
    if (!lineRes.rowCount) return res.status(404).json({ message: "Linha GTFS nao encontrada." });
    const tripsRes = await db.query(
      `SELECT
         t.trip_id,
         t.trip_headsign,
         t.direction_id,
         t.service_id,
         t.shape_id,
         COUNT(st.stop_id)::int AS stops_count
       FROM gtfs_trips t
       LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
       WHERE t.route_id = $1
       GROUP BY t.trip_id, t.trip_headsign, t.direction_id, t.service_id, t.shape_id
       ORDER BY t.trip_id ASC`,
      [routeId]
    );
    const tripIds = tripsRes.rows.map((r) => String(r.trip_id || "").trim()).filter(Boolean);
    let stopsRows = [];
    if (tripIds.length) {
      const stopsRes = await db.query(
        `SELECT
           st.trip_id,
           st.stop_sequence,
           st.arrival_time,
           st.departure_time,
           st.stop_id,
           s.stop_name,
           s.stop_lat,
           s.stop_lon
         FROM gtfs_stop_times st
         LEFT JOIN gtfs_stops s ON s.stop_id = st.stop_id
         WHERE st.trip_id = ANY($1::text[])
         ORDER BY st.trip_id ASC, st.stop_sequence ASC`,
        [tripIds]
      );
      stopsRows = stopsRes.rows;
    }
    const shapeIds = Array.from(
      new Set(tripsRes.rows.map((r) => String(r.shape_id || "").trim()).filter(Boolean))
    );
    let shapeRows = [];
    if (shapeIds.length) {
      const shapeRes = await db.query(
        `SELECT shape_id, shape_pt_lat, shape_pt_lon, shape_pt_sequence
         FROM gtfs_shapes
         WHERE shape_id = ANY($1::text[])
         ORDER BY shape_id ASC, shape_pt_sequence ASC`,
        [shapeIds]
      );
      shapeRows = shapeRes.rows;
    }
    const pointsByShape = new Map();
    shapeRows.forEach((row) => {
      const shapeId = String(row.shape_id || "");
      if (!pointsByShape.has(shapeId)) pointsByShape.set(shapeId, []);
      pointsByShape.get(shapeId).push({
        lat: Number(row.shape_pt_lat),
        lng: Number(row.shape_pt_lon),
        sequence: Number(row.shape_pt_sequence),
      });
    });
    const stopsByTrip = new Map();
    stopsRows.forEach((row) => {
      const tripId = String(row.trip_id || "");
      if (!stopsByTrip.has(tripId)) stopsByTrip.set(tripId, []);
      stopsByTrip.get(tripId).push({
        stop_sequence: Number(row.stop_sequence),
        stop_id: row.stop_id || "",
        stop_name: row.stop_name || "",
        arrival_time: row.arrival_time || "",
        departure_time: row.departure_time || "",
        lat: row.stop_lat == null ? null : Number(row.stop_lat),
        lng: row.stop_lon == null ? null : Number(row.stop_lon),
      });
    });
    const trips = tripsRes.rows.map((trip) => ({
      trip_id: trip.trip_id,
      trip_headsign: trip.trip_headsign || "",
      direction_id: trip.direction_id,
      service_id: trip.service_id || "",
      shape_id: trip.shape_id || "",
      stops_count: Number(trip.stops_count || 0),
      shape_points: pointsByShape.get(String(trip.shape_id || "")) || [],
      stops: stopsByTrip.get(String(trip.trip_id || "")) || [],
    }));
    const tripCount = trips.length;
    const avgStops = tripCount ? Number((trips.reduce((acc, t) => acc + Number(t.stops_count || 0), 0) / tripCount).toFixed(2)) : 0;
    return res.json({
      line: lineRes.rows[0],
      summary: {
        trip_count: tripCount,
        avg_stops_count: avgStops,
      },
      trips,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao carregar detalhe da linha GTFS." });
  }
});

router.get("/analytics/export.xlsx", async (req, res) => {
  try {
    await ensureGtfsEditorIndexes();
    const feedKey = normalizeFeedKey(req.query.feedKey || "");
    const routeId = String(req.query.routeId || "").trim();
    const periodMode = String(req.query.periodMode || "").trim().toLowerCase();
    const startDateInput = parseIsoDateInput(req.query.startDate);
    const endDateInput = parseIsoDateInput(req.query.endDate);
    const municipalHoliday = parseMunicipalHolidayInput(req.query.municipalHoliday);
    if (startDateInput && endDateInput && startDateInput > endDateInput) {
      return res.status(400).json({ message: "Intervalo inválido: startDate maior que endDate." });
    }
    const { startDate, endDate, effectiveStartDate, gtfsEffectiveFrom, calendarEffectiveFrom } =
      await resolveAnalyticsPeriod(feedKey, startDateInput, endDateInput, periodMode);
    const holidayDates = buildHolidayDateList(startDate, endDate, municipalHoliday);
    const overviewRes = await db.query(
      `WITH selected_routes AS (
         SELECT r.feed_key, r.route_id, r.route_short_name, r.route_long_name
         FROM gtfs_routes r
         JOIN gtfs_feeds f ON f.feed_key = r.feed_key
         WHERE ($1::text = '' OR r.feed_key = $1)
           AND f.is_active = TRUE
       ),
       period AS (
         SELECT
           COALESCE($2::date, CURRENT_DATE)::date AS start_date,
           COALESCE(
             $3::date,
             (
               COALESCE($2::date, CURRENT_DATE::date)
               + INTERVAL '1 year' - INTERVAL '1 day'
             )::date
           ) AS end_date
       ),
       trip_shapes AS (
         SELECT
           t.feed_key,
           t.route_id,
           t.trip_id,
           t.service_id,
           t.shape_id,
           COALESCE(
             SUM(
               CASE
                 WHEN prev_lat IS NULL OR prev_lon IS NULL THEN 0
                 ELSE
                   6371 * 2 * ASIN(
                     SQRT(
                       POWER(SIN(RADIANS((gs.shape_pt_lat - prev_lat) / 2)), 2) +
                       COS(RADIANS(prev_lat)) * COS(RADIANS(gs.shape_pt_lat)) *
                       POWER(SIN(RADIANS((gs.shape_pt_lon - prev_lon) / 2)), 2)
                     )
                   )
               END
             ),
             0
           )::numeric(12,3) AS trip_km
         FROM gtfs_trips t
         JOIN selected_routes sr ON sr.route_id = t.route_id
         LEFT JOIN (
           SELECT
             feed_key,
             shape_id,
             shape_pt_sequence,
             shape_pt_lat,
             shape_pt_lon,
             LAG(shape_pt_lat) OVER (PARTITION BY feed_key, shape_id ORDER BY shape_pt_sequence ASC) AS prev_lat,
             LAG(shape_pt_lon) OVER (PARTITION BY feed_key, shape_id ORDER BY shape_pt_sequence ASC) AS prev_lon
           FROM gtfs_shapes
         ) gs ON gs.feed_key = t.feed_key AND gs.shape_id = t.shape_id
         GROUP BY t.feed_key, t.route_id, t.trip_id, t.service_id, t.shape_id
       ),
       trip_calendar AS (
         SELECT
           ts.feed_key,
           ts.route_id,
           ts.trip_id,
           ts.trip_km,
           ts.service_id,
           c.service_id AS calendar_service_id,
           c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday,
           c.start_date,
           c.end_date
         FROM trip_shapes ts
         LEFT JOIN gtfs_calendars c
           ON c.feed_key = ts.feed_key
          AND (
            c.service_id = ts.service_id
            OR c.service_id = (ts.feed_key || '::' || ts.service_id)
            OR ts.service_id = (ts.feed_key || '::' || c.service_id)
          )
       ),
       trip_active_dates AS (
         SELECT
           tc.feed_key,
           tc.route_id,
           tc.trip_id,
           tc.trip_km,
           gs.d::date AS op_date
         FROM trip_calendar tc
         CROSS JOIN period p
         JOIN LATERAL generate_series(
           GREATEST(COALESCE(tc.start_date, p.start_date), p.start_date),
           LEAST(COALESCE(tc.end_date, p.end_date), p.end_date),
           INTERVAL '1 day'
         ) gs(d) ON TRUE
        LEFT JOIN gtfs_calendar_dates cd_remove
          ON cd_remove.feed_key = tc.feed_key
          AND (
            cd_remove.service_id = tc.service_id
            OR cd_remove.service_id = COALESCE(tc.calendar_service_id, '')
            OR cd_remove.service_id = (tc.feed_key || '::' || tc.service_id)
          )
          AND cd_remove.calendar_date = gs.d::date
          AND cd_remove.exception_type = 2
         WHERE (
             (EXTRACT(ISODOW FROM gs.d) = 1 AND COALESCE(tc.monday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 2 AND COALESCE(tc.tuesday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 3 AND COALESCE(tc.wednesday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 4 AND COALESCE(tc.thursday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 5 AND COALESCE(tc.friday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 6 AND COALESCE(tc.saturday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 7 AND COALESCE(tc.sunday, 0) = 1)
           )
          AND cd_remove.id IS NULL
         UNION ALL
         SELECT
           tc.feed_key,
           tc.route_id,
           tc.trip_id,
           tc.trip_km,
           cd_add.calendar_date::date AS op_date
         FROM trip_calendar tc
         JOIN period p ON TRUE
         JOIN gtfs_calendar_dates cd_add
           ON cd_add.feed_key = tc.feed_key
          AND (
            cd_add.service_id = tc.service_id
            OR cd_add.service_id = COALESCE(tc.calendar_service_id, '')
            OR cd_add.service_id = (tc.feed_key || '::' || tc.service_id)
          )
          AND cd_add.exception_type = 1
          AND cd_add.calendar_date BETWEEN p.start_date AND p.end_date
        UNION ALL
        SELECT
          tc.feed_key,
          tc.route_id,
          tc.trip_id,
          tc.trip_km,
          gs.d::date AS op_date
        FROM trip_calendar tc
        CROSS JOIN period p
        JOIN LATERAL generate_series(p.start_date, p.end_date, INTERVAL '1 day') gs(d) ON TRUE
        WHERE tc.calendar_service_id IS NULL
       ),
       trip_day_counts AS (
         SELECT
           tad.feed_key,
           tad.route_id,
           tad.trip_id,
           MAX(tad.trip_km)::numeric(12,3) AS trip_km,
           COUNT(*) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) BETWEEN 1 AND 5 AND NOT (tad.op_date = ANY($4::date[])))::int AS weekday_days,
           COUNT(*) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) = 6)::int AS saturday_days,
           COUNT(*) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) = 7)::int AS sunday_days,
           COUNT(*) FILTER (WHERE tad.op_date = ANY($4::date[]))::int AS holiday_days,
           COUNT(*)::int AS total_active_days
         FROM (
           SELECT DISTINCT feed_key, route_id, trip_id, trip_km, op_date
           FROM trip_active_dates
         ) tad
         GROUP BY tad.feed_key, tad.route_id, tad.trip_id
       ),
       route_service_days AS (
         SELECT
           tad.feed_key,
           tad.route_id,
           COUNT(DISTINCT tad.op_date) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) BETWEEN 1 AND 5 AND NOT (tad.op_date = ANY($4::date[])))::int AS weekday_service_days,
           COUNT(DISTINCT tad.op_date) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) = 6)::int AS saturday_service_days,
           COUNT(DISTINCT tad.op_date) FILTER (WHERE EXTRACT(ISODOW FROM tad.op_date) = 7)::int AS sunday_service_days,
           COUNT(DISTINCT tad.op_date) FILTER (WHERE tad.op_date = ANY($4::date[]))::int AS holiday_service_days
         FROM (
           SELECT DISTINCT feed_key, route_id, op_date
           FROM trip_active_dates
         ) tad
         GROUP BY tad.feed_key, tad.route_id
       ),
       route_agg AS (
         SELECT
           sr.feed_key,
           sr.route_id,
           COALESCE(NULLIF(TRIM(sr.route_short_name), ''), sr.route_id) AS route_label,
           COALESCE(NULLIF(TRIM(sr.route_long_name), ''), '-') AS route_long_name,
           COUNT(DISTINCT ts.trip_id)::int AS trips_defined,
           AVG(ts.trip_km)::numeric(12,3) AS avg_trip_km,
           COALESCE(rsd.weekday_service_days, 0)::int AS weekday_service_days,
           COALESCE(rsd.saturday_service_days, 0)::int AS saturday_service_days,
           COALESCE(rsd.sunday_service_days, 0)::int AS sunday_service_days,
           COALESCE(rsd.holiday_service_days, 0)::int AS holiday_service_days,
           COUNT(DISTINCT CASE WHEN COALESCE(tdc.weekday_days, 0) > 0 THEN ts.trip_id END)::int AS weekday_trips_defined,
           COUNT(DISTINCT CASE WHEN COALESCE(tdc.saturday_days, 0) > 0 THEN ts.trip_id END)::int AS saturday_trips_defined,
           COUNT(DISTINCT CASE WHEN COALESCE(tdc.sunday_days, 0) > 0 THEN ts.trip_id END)::int AS sunday_trips_defined,
           COUNT(DISTINCT CASE WHEN COALESCE(tdc.holiday_days, 0) > 0 THEN ts.trip_id END)::int AS holiday_trips_defined,
           COALESCE(SUM(tdc.weekday_days), 0)::int AS weekday_ops,
           COALESCE(SUM(tdc.saturday_days), 0)::int AS saturday_ops,
           COALESCE(SUM(tdc.sunday_days), 0)::int AS sunday_ops,
           COALESCE(SUM(tdc.holiday_days), 0)::int AS holiday_ops,
           COALESCE(SUM(tdc.total_active_days), 0)::int AS total_ops_days,
           COALESCE(SUM(tdc.trip_km * tdc.total_active_days), 0)::numeric(12,3) AS gtfs_year_km
        FROM selected_routes sr
        LEFT JOIN trip_shapes ts ON ts.feed_key = sr.feed_key AND ts.route_id = sr.route_id
        LEFT JOIN trip_day_counts tdc ON tdc.feed_key = ts.feed_key AND tdc.route_id = ts.route_id AND tdc.trip_id = ts.trip_id
         LEFT JOIN route_service_days rsd ON rsd.feed_key = sr.feed_key AND rsd.route_id = sr.route_id
         GROUP BY sr.feed_key, sr.route_id, route_label, route_long_name
           , rsd.weekday_service_days, rsd.saturday_service_days, rsd.sunday_service_days, rsd.holiday_service_days
       ),
      route_trip_profiles AS (
        SELECT
          ts.feed_key,
          ts.route_id,
          COUNT(DISTINCT CASE
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-DF'
              THEN NULL
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-S'
              THEN NULL
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-U'
              THEN ts.trip_id
            ELSE NULL
          END)::int AS weekday_trips_defined,
          COUNT(DISTINCT CASE
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-S'
              THEN ts.trip_id
            ELSE NULL
          END)::int AS saturday_trips_defined,
          COUNT(DISTINCT CASE
            WHEN UPPER(REGEXP_REPLACE(COALESCE(ts.service_id, ''), ('^' || ts.feed_key || '::'), '')) LIKE '%-DF'
              THEN ts.trip_id
            ELSE NULL
          END)::int AS sunday_holiday_trips_defined
        FROM trip_shapes ts
        GROUP BY ts.feed_key, ts.route_id
      ),
       realized_by_line AS (
         SELECT
           LOWER(TRIM(COALESCE(s.line_code, ''))) AS line_key,
           COALESCE(SUM(COALESCE(s.total_km, 0)), 0)::numeric(12,3) AS realized_km
         FROM services s
         WHERE LOWER(TRIM(COALESCE(s.status::text, ''))) = 'completed'
         GROUP BY LOWER(TRIM(COALESCE(s.line_code, '')))
       )
       SELECT
         ra.feed_key,
         ra.route_id,
         ra.route_label,
         ra.route_long_name,
         ra.trips_defined,
         COALESCE(ra.avg_trip_km, 0)::numeric(12,3) AS avg_trip_km,
        COALESCE(rtp.weekday_trips_defined, 0)::numeric(12,3) AS trips_per_weekday,
        COALESCE(rtp.saturday_trips_defined, 0)::numeric(12,3) AS trips_per_saturday,
        COALESCE(rtp.sunday_holiday_trips_defined, 0)::numeric(12,3) AS trips_per_sunday,
        COALESCE(rtp.sunday_holiday_trips_defined, 0)::numeric(12,3) AS trips_per_holiday,
         ra.weekday_service_days,
         ra.saturday_service_days,
         ra.sunday_service_days,
         ra.holiday_service_days,
         ra.weekday_ops,
         ra.saturday_ops,
         ra.sunday_ops,
         ra.holiday_ops,
         ra.total_ops_days,
         COALESCE(ra.gtfs_year_km, 0)::numeric(12,3) AS gtfs_year_km,
         (COALESCE(ra.gtfs_year_km, 0) / 12.0)::numeric(12,3) AS gtfs_month_avg_km,
         (COALESCE(ra.gtfs_year_km, 0) / 2.0)::numeric(12,3) AS gtfs_semester_avg_km,
         COALESCE(rbl.realized_km, 0)::numeric(12,3) AS realized_km,
         (COALESCE(ra.gtfs_year_km, 0) - COALESCE(rbl.realized_km, 0))::numeric(12,3) AS km_gap_vs_realized,
         CASE
           WHEN COALESCE(ra.gtfs_year_km, 0) <= 0 THEN 0::numeric(6,2)
           ELSE ((COALESCE(rbl.realized_km, 0) / ra.gtfs_year_km) * 100)::numeric(6,2)
         END AS realized_vs_gtfs_pct
       FROM route_agg ra
      LEFT JOIN route_trip_profiles rtp
        ON rtp.feed_key = ra.feed_key
       AND rtp.route_id = ra.route_id
       LEFT JOIN realized_by_line rbl
         ON rbl.line_key = LOWER(TRIM(COALESCE(ra.route_label, '')))
         OR rbl.line_key = LOWER(TRIM(COALESCE(ra.route_id, '')))
       ORDER BY route_label ASC, route_id ASC`,
      [feedKey, startDate, endDate, holidayDates]
    );

    const detailsRes = await db.query(
      `SELECT
         t.feed_key,
         t.route_id,
         COALESCE(NULLIF(TRIM(r.route_short_name), ''), t.route_id) AS route_label,
         COALESCE(NULLIF(TRIM(r.route_long_name), ''), '-') AS route_long_name,
         t.trip_id,
         COALESCE(t.trip_headsign, '') AS trip_headsign,
         t.direction_id,
         t.service_id,
         t.shape_id,
         COUNT(st.stop_id)::int AS stops_count
       FROM gtfs_trips t
       JOIN gtfs_routes r ON r.route_id = t.route_id
       JOIN gtfs_feeds f ON f.feed_key = t.feed_key
       LEFT JOIN gtfs_stop_times st ON st.trip_id = t.trip_id
       WHERE ($1::text = '' OR t.feed_key = $1)
         AND ($2::text = '' OR t.route_id = $2)
         AND f.is_active = TRUE
       GROUP BY
         t.feed_key, t.route_id, route_label, route_long_name,
         t.trip_id, t.trip_headsign, t.direction_id, t.service_id, t.shape_id
       ORDER BY route_label ASC, t.trip_id ASC`,
      [feedKey, routeId]
    );

    const calendarsRes = await db.query(
      `SELECT
         c.feed_key,
         c.service_id,
         c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday,
         c.start_date, c.end_date, c.is_active
       FROM gtfs_calendars c
       WHERE ($1::text = '' OR c.feed_key = $1)
       ORDER BY c.service_id ASC`,
      [feedKey]
    );
    const calendarDatesRes = await db.query(
      `SELECT
         cd.feed_key,
         cd.service_id,
         cd.calendar_date,
         cd.exception_type
       FROM gtfs_calendar_dates cd
       WHERE ($1::text = '' OR cd.feed_key = $1)
       ORDER BY cd.service_id ASC, cd.calendar_date ASC`,
      [feedKey]
    );

    const overviewRows = overviewRes.rows.map((row) => ({
      feed_key: row.feed_key || "",
      route_id: row.route_id || "",
      route_label: row.route_label || "",
      route_long_name: row.route_long_name || "",
      trips_defined: Number(row.trips_defined || 0),
      trips_per_weekday: Number(row.trips_per_weekday || 0),
      trips_per_saturday: Number(row.trips_per_saturday || 0),
      trips_per_sunday: Number(row.trips_per_sunday || 0),
      trips_per_holiday: Number(row.trips_per_holiday || 0),
      weekday_service_days: Number(row.weekday_service_days || 0),
      saturday_service_days: Number(row.saturday_service_days || 0),
      sunday_service_days: Number(row.sunday_service_days || 0),
      holiday_service_days: Number(row.holiday_service_days || 0),
      avg_trip_km: Number(row.avg_trip_km || 0),
      weekday_ops: Number(row.weekday_ops || 0),
      saturday_ops: Number(row.saturday_ops || 0),
      sunday_ops: Number(row.sunday_ops || 0),
      holiday_ops: Number(row.holiday_ops || 0),
      total_ops_days: Number(row.total_ops_days || 0),
      gtfs_year_km: Number(row.gtfs_year_km || 0),
      gtfs_month_avg_km: Number(row.gtfs_month_avg_km || 0),
      gtfs_semester_avg_km: Number(row.gtfs_semester_avg_km || 0),
      realized_km: Number(row.realized_km || 0),
      km_gap_vs_realized: Number(row.km_gap_vs_realized || 0),
      realized_vs_gtfs_pct: Number(row.realized_vs_gtfs_pct || 0),
    }));

    const detailRows = detailsRes.rows.map((row) => ({
      feed_key: row.feed_key || "",
      route_id: row.route_id || "",
      route_label: row.route_label || "",
      route_long_name: row.route_long_name || "",
      trip_id: row.trip_id || "",
      trip_headsign: row.trip_headsign || "",
      direction_id: row.direction_id == null ? "" : Number(row.direction_id),
      service_id: row.service_id || "",
      shape_id: row.shape_id || "",
      stops_count: Number(row.stops_count || 0),
    }));

    const assumptionsRows = [
      { key: "period", value: "1 ano operacional baseado no calendario GTFS" },
      { key: "feedKey", value: feedKey || "(active feeds)" },
      { key: "routeId", value: routeId || "(all routes)" },
      { key: "startDate", value: startDate || "" },
      { key: "endDate", value: endDate || "" },
      { key: "effectiveStartDate", value: effectiveStartDate || "" },
      { key: "gtfsEffectiveFrom", value: gtfsEffectiveFrom || "" },
      { key: "calendarEffectiveFrom", value: calendarEffectiveFrom || "" },
      { key: "municipalHoliday", value: municipalHoliday?.label || "" },
      { key: "timezone", value: "Europe/Lisbon" },
      { key: "dstAuto", value: "true" },
    ];

    const planRowsRes = await db.query(
      `WITH selected_routes AS (
         SELECT r.feed_key, r.route_id, COALESCE(NULLIF(TRIM(r.route_short_name), ''), r.route_id) AS route_label
         FROM gtfs_routes r
         JOIN gtfs_feeds f ON f.feed_key = r.feed_key
         WHERE ($1::text = '' OR r.feed_key = $1)
           AND ($2::text = '' OR r.route_id = $2)
           AND f.is_active = TRUE
       ),
       period AS (
         SELECT
           COALESCE($3::date, COALESCE((SELECT gf.gtfs_effective_from FROM gtfs_feeds gf WHERE gf.feed_key = $1), CURRENT_DATE)::date) AS start_date,
           COALESCE(
             $4::date,
             (
               COALESCE($3::date, COALESCE((SELECT gf.gtfs_effective_from FROM gtfs_feeds gf WHERE gf.feed_key = $1), CURRENT_DATE)::date)
               + INTERVAL '1 year' - INTERVAL '1 day'
             )::date
           ) AS end_date
       ),
       trip_shapes AS (
         SELECT
           t.feed_key, t.route_id, t.trip_id, t.service_id,
           COALESCE(
             SUM(
               CASE
                 WHEN prev_lat IS NULL OR prev_lon IS NULL THEN 0
                 ELSE
                   6371 * 2 * ASIN(
                     SQRT(
                       POWER(SIN(RADIANS((gs.shape_pt_lat - prev_lat) / 2)), 2) +
                       COS(RADIANS(prev_lat)) * COS(RADIANS(gs.shape_pt_lat)) *
                       POWER(SIN(RADIANS((gs.shape_pt_lon - prev_lon) / 2)), 2)
                     )
                   )
               END
             ),
             0
           )::numeric(12,3) AS trip_km
         FROM gtfs_trips t
         JOIN selected_routes sr ON sr.route_id = t.route_id
         LEFT JOIN (
           SELECT
             feed_key, shape_id, shape_pt_sequence, shape_pt_lat, shape_pt_lon,
             LAG(shape_pt_lat) OVER (PARTITION BY feed_key, shape_id ORDER BY shape_pt_sequence ASC) AS prev_lat,
             LAG(shape_pt_lon) OVER (PARTITION BY feed_key, shape_id ORDER BY shape_pt_sequence ASC) AS prev_lon
           FROM gtfs_shapes
         ) gs ON gs.feed_key = t.feed_key AND gs.shape_id = t.shape_id
         GROUP BY t.feed_key, t.route_id, t.trip_id, t.service_id
       ),
       trip_calendar AS (
         SELECT
           ts.feed_key, ts.route_id, ts.trip_id, ts.trip_km, ts.service_id,
           c.service_id AS calendar_service_id,
           c.monday, c.tuesday, c.wednesday, c.thursday, c.friday, c.saturday, c.sunday,
           c.start_date, c.end_date
         FROM trip_shapes ts
         LEFT JOIN gtfs_calendars c
           ON c.feed_key = ts.feed_key
          AND (
            c.service_id = ts.service_id
            OR c.service_id = (ts.feed_key || '::' || ts.service_id)
            OR ts.service_id = (ts.feed_key || '::' || c.service_id)
          )
       ),
       base_days AS (
         SELECT
           tc.feed_key, tc.route_id, tc.trip_id, tc.trip_km, gs.d::date AS op_date
         FROM trip_calendar tc
         CROSS JOIN period p
         JOIN LATERAL generate_series(
           GREATEST(COALESCE(tc.start_date, p.start_date), p.start_date),
           LEAST(COALESCE(tc.end_date, p.end_date), p.end_date),
           INTERVAL '1 day'
         ) gs(d) ON TRUE
         LEFT JOIN gtfs_calendar_dates cd_remove
           ON cd_remove.feed_key = tc.feed_key
          AND (
            cd_remove.service_id = tc.service_id
            OR cd_remove.service_id = COALESCE(tc.calendar_service_id, '')
            OR cd_remove.service_id = (tc.feed_key || '::' || tc.service_id)
          )
          AND cd_remove.calendar_date = gs.d::date
          AND cd_remove.exception_type = 2
         WHERE (
             (EXTRACT(ISODOW FROM gs.d) = 1 AND COALESCE(tc.monday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 2 AND COALESCE(tc.tuesday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 3 AND COALESCE(tc.wednesday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 4 AND COALESCE(tc.thursday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 5 AND COALESCE(tc.friday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 6 AND COALESCE(tc.saturday, 0) = 1) OR
             (EXTRACT(ISODOW FROM gs.d) = 7 AND COALESCE(tc.sunday, 0) = 1)
           )
           AND cd_remove.id IS NULL
       ),
       added_days AS (
         SELECT
           tc.feed_key, tc.route_id, tc.trip_id, tc.trip_km, cd_add.calendar_date::date AS op_date
         FROM trip_calendar tc
         JOIN period p ON TRUE
         JOIN gtfs_calendar_dates cd_add
           ON cd_add.feed_key = tc.feed_key
          AND (
            cd_add.service_id = tc.service_id
            OR cd_add.service_id = COALESCE(tc.calendar_service_id, '')
            OR cd_add.service_id = (tc.feed_key || '::' || tc.service_id)
          )
          AND cd_add.exception_type = 1
          AND cd_add.calendar_date BETWEEN p.start_date AND p.end_date
       ),
      assumed_days AS (
        SELECT
          tc.feed_key, tc.route_id, tc.trip_id, tc.trip_km, gs.d::date AS op_date
        FROM trip_calendar tc
        CROSS JOIN period p
        JOIN LATERAL generate_series(p.start_date, p.end_date, INTERVAL '1 day') gs(d) ON TRUE
        WHERE tc.calendar_service_id IS NULL
      ),
       all_days AS (
         SELECT DISTINCT feed_key, route_id, trip_id, trip_km, op_date FROM base_days
         UNION
         SELECT DISTINCT feed_key, route_id, trip_id, trip_km, op_date FROM added_days
        UNION
        SELECT DISTINCT feed_key, route_id, trip_id, trip_km, op_date FROM assumed_days
       )
       SELECT
         ad.op_date,
         sr.route_label,
         ad.route_id,
         COUNT(DISTINCT ad.trip_id)::int AS trips_count,
         COALESCE(SUM(ad.trip_km), 0)::numeric(12,3) AS km_day
       FROM all_days ad
       JOIN selected_routes sr ON sr.route_id = ad.route_id
       GROUP BY ad.op_date, sr.route_label, ad.route_id
       ORDER BY ad.op_date ASC, sr.route_label ASC, ad.route_id ASC`,
      [feedKey, routeId, startDate, endDate]
    );

    const wb = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(overviewRows), "linhas");
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(detailRows), "trips_detalhe");
    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(
        calendarsRes.rows.map((r) => ({
          feed_key: r.feed_key || "",
          service_id: r.service_id || "",
          monday: Number(r.monday || 0),
          tuesday: Number(r.tuesday || 0),
          wednesday: Number(r.wednesday || 0),
          thursday: Number(r.thursday || 0),
          friday: Number(r.friday || 0),
          saturday: Number(r.saturday || 0),
          sunday: Number(r.sunday || 0),
          start_date: r.start_date || "",
          end_date: r.end_date || "",
          is_active: r.is_active === true,
        }))
      ),
      "calendar"
    );
    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(
        calendarDatesRes.rows.map((r) => ({
          feed_key: r.feed_key || "",
          service_id: r.service_id || "",
          calendar_date: r.calendar_date || "",
          exception_type: Number(r.exception_type || 0),
        }))
      ),
      "calendar_dates"
    );
    XLSX.utils.book_append_sheet(
      wb,
      XLSX.utils.json_to_sheet(
        planRowsRes.rows.map((r) => ({
          op_date: r.op_date,
          route_label: r.route_label || "",
          route_id: r.route_id || "",
          trips_count: Number(r.trips_count || 0),
          km_day: Number(r.km_day || 0),
        }))
      ),
      "plano_operacao_dia"
    );
    XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(assumptionsRows), "parametros");
    const buffer = XLSX.write(wb, { type: "buffer", bookType: "xlsx" });
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    res.setHeader("Content-Disposition", "attachment; filename=gtfs_analise_detalhada.xlsx");
    return res.status(200).send(buffer);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar análise GTFS em Excel." });
  }
});

module.exports = router;
