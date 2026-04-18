const express = require("express");
const db = require("../db");
const authMiddleware = require("../middleware/auth");
const { requireRoles, normalizeRole } = require("../middleware/roles");
const XLSX = require("xlsx");
const { calculatePathDistance } = require("../utils/distance");
const { archiveClosedRosterDays } = require("../utils/rosterArchive");
const { OVERVIEW_TODAY_SQL } = require("../utils/overviewToday");
const { serviceActivityLisbonDayFilter } = require("../utils/serviceListFilters");
const { findBestTripForLine, getShapePointsByTripId, getStopsByTripId } = require("../utils/gtfsTripResolve");
const { matchGpsPointsToGtfsStops } = require("../utils/stopPassageMatch");
const { resolveTotalKmWithPlannedFallback } = require("../utils/plannedKmFallback");

/** Mudar ao alterar mensagens/lógica do PATCH /roster/:id/reassign (diagnóstico de deploy). */
const ROSTER_REASSIGN_API_REVISION = "20260412g";

const router = express.Router();

function rosterReassignJson(res, httpStatus, payload) {
  res.setHeader("X-Escala-Reassign-Api", ROSTER_REASSIGN_API_REVISION);
  return res.status(httpStatus).json({ ...payload, apiRevision: ROSTER_REASSIGN_API_REVISION });
}
router.use(authMiddleware);
router.use(requireRoles("supervisor", "admin"));
router.use(async (_req, _res, next) => {
  try {
    await archiveClosedRosterDays();
    return next();
  } catch (_error) {
    return next();
  }
});

async function ensurePlannedServiceLocationColumns() {
  await db.query(
    `ALTER TABLE planned_services
       ADD COLUMN IF NOT EXISTS start_location VARCHAR(120)`
  );
  await db.query(
    `ALTER TABLE planned_services
       ADD COLUMN IF NOT EXISTS end_location VARCHAR(120)`
  );
  await db.query(
    `ALTER TABLE planned_services
       ADD COLUMN IF NOT EXISTS kms_carga NUMERIC(12,3)`
  );
}

function buildFallbackEmail(username, mechanicNumber) {
  const safeUser = String(username || "user").replace(/[^a-zA-Z0-9._-]/g, "").toLowerCase() || "user";
  const safeMec = String(mechanicNumber || "sem-mec").replace(/[^a-zA-Z0-9._-]/g, "");
  return `${safeUser}.${safeMec}.${Date.now()}@no-email.local`;
}

function csvEscape(value) {
  const raw = value == null ? "" : String(value);
  const escaped = raw.replace(/"/g, '""');
  return `"${escaped}"`;
}

function parseBooleanLike(value, fallback = true) {
  if (value == null || value === "") return fallback;
  const v = String(value).trim().toLowerCase();
  return ["1", "true", "sim", "yes", "ativo", "activa", "active"].includes(v);
}

function parseCsvText(csvText) {
  const lines = String(csvText || "")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);
  if (lines.length < 2) return [];

  const delimiter = (lines[0].match(/;/g) || []).length > (lines[0].match(/,/g) || []).length ? ";" : ",";
  const headers = lines[0].split(delimiter).map((h) => h.trim().toLowerCase());
  const rows = [];
  for (let i = 1; i < lines.length; i += 1) {
    const cols = lines[i].split(delimiter).map((c) => c.trim());
    const row = {};
    headers.forEach((h, idx) => {
      row[h] = cols[idx] ?? "";
    });
    rows.push(row);
  }
  return rows;
}

function normalizeImportRows(rows) {
  return rows.map((row) => {
    const normalized = {};
    Object.keys(row || {}).forEach((key) => {
      normalized[String(key).trim().toLowerCase()] = row[key];
    });
    return normalized;
  });
}

/** Cabecalhos Excel/CSV: remove acentos e normaliza para chave tipo snake_case. */
function stripAccentsHeader(str) {
  return String(str || "")
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
}

function rowToHeaderLookup(row) {
  const lookup = {};
  for (const [rawKey, rawVal] of Object.entries(row || {})) {
    const nk = stripAccentsHeader(String(rawKey).trim().toLowerCase())
      .replace(/\s+/g, "_")
      .replace(/_+/g, "_")
      .replace(/[^a-z0-9_]/g, "");
    const v = rawVal == null ? "" : String(rawVal).trim();
    if (v) lookup[nk] = v;
  }
  return lookup;
}

function pickStartLocationFromRow(row) {
  const loc = rowToHeaderLookup(row);
  const keys = [
    "inicio",
    "origem",
    "partida",
    "saida",
    "local_inicio",
    "local_de_inicio",
    "localinicio",
    "start_location",
    "origem_servico",
    "origem_do_servico",
    "local_partida",
    "local_de_partida",
    "ponto_de_partida",
    "local_saida",
    "local_de_saida",
    "local_de_origem",
    "local_origem",
  ];
  for (const k of keys) {
    if (loc[k]) return loc[k];
  }
  return "";
}

/** Texto do campo Observações (opcional; usado p.ex. na deteção de linhas não operacionais). */
function pickObservacoesFromImportRow(row) {
  const loc = rowToHeaderLookup(row);
  const keys = [
    "observacoes",
    "observations",
    "observation",
    "obs",
    "notas",
    "comentarios",
    "anotacoes",
    "informacao_adicional",
    "informacao_complementar",
  ];
  for (const k of keys) {
    if (loc[k]) return String(loc[k]).trim();
  }
  const r = row || {};
  return String(
    r.observacoes ||
      r.observations ||
      r.obs ||
      r.notas ||
      r.comentarios ||
      ""
  ).trim();
}

/** Chave normalizada de coluna (igual a rowToHeaderLookup, sem exigir valor). */
function normalizeImportColumnKey(rawKey) {
  return stripAccentsHeader(String(rawKey).trim().toLowerCase())
    .replace(/\s+/g, "_")
    .replace(/_+/g, "_")
    .replace(/[^a-z0-9_]/g, "");
}

function parseKmImportValue(raw) {
  if (raw == null) return null;
  const s = String(raw).trim().replace(/\s/g, "");
  if (!s) return null;
  const n = Number(s.replace(",", "."));
  return Number.isFinite(n) ? n : null;
}

/** Mapa coluna normalizada -> valor bruto (inclui células vazias). */
function importRowByNormalizedKeys(row) {
  const by = {};
  for (const [rawKey, rawVal] of Object.entries(row || {})) {
    by[normalizeImportColumnKey(rawKey)] = rawVal;
  }
  return by;
}

const KMS_CARGA_KEYS = ["kmscarga", "kms_carga", "km_carga"];
const KMS_VAZIO_KEYS = ["kmsvazio", "kms_vazio", "km_vazio"];
const KMS_VAZIO_TEC_KEYS = ["kmsvaziotechnico", "kms_vazio_tecnico", "kmsvazio_tecnico"];

function pickKmFromNormalizedRow(byNorm, keyCandidates) {
  for (const k of keyCandidates) {
    if (!Object.prototype.hasOwnProperty.call(byNorm, k)) continue;
    const raw = byNorm[k];
    if (raw == null || String(raw).trim() === "") continue;
    return parseKmImportValue(raw);
  }
  return null;
}

/**
 * Se o ficheiro tiver colunas de km de carga/vazio, aplica regras de escala:
 * - linhas com KmsVazio ou KmsVazioTecnico > 0 são deslocações em vazio → ignoradas;
 * - se existir coluna de KmsCarga, só entram linhas com KmsCarga > 0.
 */
function buildKmEscalaRuleFromImportRows(rows) {
  if (!rows.length) return { active: false, hasCargaCol: false };
  const headers = Object.keys(rows[0] || {}).map(normalizeImportColumnKey);
  const uniq = [...new Set(headers)];
  const hasCargaCol = uniq.some(
    (h) =>
      KMS_CARGA_KEYS.includes(h) ||
      ((h.includes("carga") || h.endsWith("carga")) && (h.includes("km") || h.includes("kms")))
  );
  const hasVazioCol = uniq.some(
    (h) =>
      KMS_VAZIO_KEYS.includes(h) ||
      (h.includes("vazio") && !h.includes("tecnico"))
  );
  const hasVazioTecCol = uniq.some(
    (h) => KMS_VAZIO_TEC_KEYS.includes(h) || (h.includes("tecnico") && h.includes("vazio"))
  );
  const active = hasCargaCol || hasVazioCol || hasVazioTecCol;
  return { active, hasCargaCol };
}

function kmEscalaRowShouldSkip(row, rule) {
  if (!rule.active) return { skip: false, message: "" };
  const by = importRowByNormalizedKeys(row);
  const carga = pickKmFromNormalizedRow(by, KMS_CARGA_KEYS);
  const vazio = pickKmFromNormalizedRow(by, KMS_VAZIO_KEYS);
  const vazioTec = pickKmFromNormalizedRow(by, KMS_VAZIO_TEC_KEYS);
  const vazioPos = (vazio != null && vazio > 0) || (vazioTec != null && vazioTec > 0);
  if (vazioPos) {
    return {
      skip: true,
      message:
        "Deslocacao em vazio (KmsVazio/KmsVazioTecnico) — nao incluida nos servicos escalados.",
    };
  }
  if (rule.hasCargaCol) {
    const cargaOk = carga != null && carga > 0;
    if (!cargaOk) {
      return {
        skip: true,
        message: "Sem KmsCarga (km de servico com carga) — nao incluido nos servicos escalados.",
      };
    }
  }
  return { skip: false, message: "" };
}

function pickEndLocationFromRow(row) {
  const loc = rowToHeaderLookup(row);
  const keys = [
    "fim",
    "termino",
    "terminus",
    "destino",
    "chegada",
    "local_fim",
    "local_de_fim",
    "localtermino",
    "end_location",
    "destino_servico",
    "destino_do_servico",
    "local_chegada",
    "local_de_chegada",
    "ponto_de_chegada",
    "local_destino",
    "local_de_destino",
    "termino_servico",
  ];
  for (const k of keys) {
    if (loc[k]) return loc[k];
  }
  return "";
}

function parseDateOnly(dateText) {
  if (!dateText) return null;
  const raw = String(dateText).trim();
  const isoMatch = raw.match(/^(\d{4})-(\d{2})-(\d{2})$/);
  if (isoMatch) return raw;

  const ptMatch = raw.match(/^(\d{1,2})\/(\d{1,2})\/(\d{4})$/);
  if (ptMatch) {
    const dd = String(Number(ptMatch[1])).padStart(2, "0");
    const mm = String(Number(ptMatch[2])).padStart(2, "0");
    const yyyy = ptMatch[3];
    return `${yyyy}-${mm}-${dd}`;
  }

  const date = new Date(`${raw}T00:00:00`);
  if (Number.isNaN(date.getTime())) return null;
  return date.toISOString().slice(0, 10);
}

function normalizeCode(value) {
  return String(value || "")
    .toUpperCase()
    .replace(/[^A-Z0-9]/g, "");
}

function normalizeName(value) {
  const raw = String(value || "").replace(/\([^)]*\)/g, " ");
  return raw
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "")
    .toLowerCase()
    .replace(/[^a-z0-9]/g, "");
}

/** Pausa / abastecimento / etc.: só palavras inteiras (evita "controlo de outros veículos" por causa de "outros"). */
const NON_OPERATIONAL_IMPORT_TOKENS = new Set([
  "pausa",
  "abastecimento",
  "trabalhos",
  "outros",
  "apoioadministrativo",
  "acompanharservico",
]);

function importRowIsNonOperational(startLocation, endLocation, obsText) {
  const text = [startLocation, endLocation, obsText].filter(Boolean).join(" ");
  const lowered = String(text || "")
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  const tokens = lowered.split(/[^a-z0-9]+/).filter(Boolean);
  for (const t of tokens) {
    if (NON_OPERATIONAL_IMPORT_TOKENS.has(t)) return true;
  }
  for (let i = 0; i < tokens.length - 1; i += 1) {
    if (tokens[i] === "acompanhar" && tokens[i + 1] === "servico") return true;
  }
  return false;
}

function normalizeMechanicNumber(value) {
  const digits = String(value || "").replace(/\D/g, "");
  if (!digits) return "";
  return String(Number(digits));
}

/** Estado em daily_roster: chave minúscula sem acentos; vazio → pending. */
function normalizeDailyRosterStatusKey(status) {
  const raw = String(status ?? "")
    .replace(/\u00A0/g, " ")
    .replace(/[\u200B-\u200D\uFEFF]/g, "")
    .trim()
    .toLowerCase()
    .normalize("NFD")
    .replace(/[\u0300-\u036f]/g, "");
  return raw || "pending";
}

/** Só bloqueia reatribuição estados «finais» na escala; pending/assigned/valores desconhecidos permitem (há validação na app). */
function dailyRosterStatusBlocksReassign(status) {
  const k = normalizeDailyRosterStatusKey(status);
  return k === "in_progress" || k === "completed";
}

/** Texto para mensagens ao utilizador (sem termos técnicos em inglês como «assigned»). */
function dailyRosterStatusMessagePt(status) {
  const k = normalizeDailyRosterStatusKey(status);
  const map = {
    pending: "pendente",
    assigned: "escalado por iniciar",
    pendente: "pendente",
    atribuido: "atribuído",
    in_progress: "viagem em curso na escala",
    completed: "concluído na escala",
    cancelled: "cancelado",
  };
  const raw = String(status ?? "").trim();
  return map[k] || raw || "—";
}

function findDriverByApproxName(driversRows, rawName) {
  const normalized = normalizeName(rawName);
  if (!normalized) return null;

  const exact = driversRows.find((d) => normalizeName(d.name) === normalized);
  if (exact) return exact;

  // Fallback: compare meaningful tokens (min 4 chars).
  const tokens = normalized.match(/[a-z0-9]{4,}/g) || [];
  if (!tokens.length) return null;
  return (
    driversRows.find((d) => {
      const dn = normalizeName(d.name);
      return tokens.every((t) => dn.includes(t) || t.includes(dn));
    }) || null
  );
}

function toHourMinute(value) {
  if (!value) return null;
  const m = String(value).match(/\b(\d{1,2}):(\d{2})\b/);
  if (!m) return null;
  const hh = String(Math.min(Number(m[1]), 23)).padStart(2, "0");
  const mm = String(Math.min(Number(m[2]), 59)).padStart(2, "0");
  return `${hh}:${mm}`;
}

function formatLisbonExecutionDay(startedAt, endedAt) {
  const base = startedAt || endedAt;
  if (!base) return "";
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Lisbon" }).format(new Date(base));
}

function formatLisbonTimeOnly(value) {
  if (!value) return "";
  return new Intl.DateTimeFormat("pt-PT", {
    timeZone: "Europe/Lisbon",
    hour: "2-digit",
    minute: "2-digit",
    second: "2-digit",
    hour12: false,
  }).format(new Date(value));
}

async function computePlannedKmByTripId(tripId, cache) {
  if (!tripId) return null;
  const key = String(tripId);
  if (cache.has(key)) return cache.get(key);

  const shapeResult = await db.query(
    `SELECT s.shape_pt_lat, s.shape_pt_lon
     FROM gtfs_shapes s
     JOIN gtfs_trips t ON t.shape_id = s.shape_id
     WHERE t.trip_id = $1
     ORDER BY s.shape_pt_sequence ASC`,
    [tripId]
  );
  const points = shapeResult.rows.map((p) => ({
    lat: Number(p.shape_pt_lat),
    lng: Number(p.shape_pt_lon),
  }));
  const km = points.length >= 2 ? calculatePathDistance(points) : null;
  cache.set(key, km);
  return km;
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

async function closeActiveSegment(serviceId, finalStatus = "completed") {
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
     SET status = $2,
         ended_at = COALESCE(ended_at, NOW()),
         km_segment = $3
     WHERE id = $1`,
    [activeSegment.id, finalStatus, kmSegment]
  );

  return { ...activeSegment, kmSegment };
}

async function enrichServiceRowsForExport(rows) {
  const serviceIds = rows.map((r) => Number(r.id)).filter((id) => Number.isFinite(id) && id > 0);
  const handoverMetricsByServiceId = new Map();
  if (serviceIds.length) {
    const segRes = await db.query(
      `SELECT
         seg.service_id,
         seg.started_at,
         seg.km_segment,
         u.name AS driver_name
       FROM service_segments seg
       JOIN users u ON u.id = seg.driver_id
       WHERE seg.service_id = ANY($1::int[])
       ORDER BY seg.service_id ASC, seg.started_at ASC`,
      [serviceIds]
    );
    const byService = new Map();
    segRes.rows.forEach((row) => {
      const sid = Number(row.service_id);
      if (!byService.has(sid)) byService.set(sid, []);
      byService.get(sid).push(row);
    });
    byService.forEach((segments, sid) => {
      const first = segments[0] || null;
      const rest = segments.slice(1);
      const kmInitial = Number(first?.km_segment || 0);
      const kmContinuation = rest.reduce((sum, seg) => sum + Number(seg.km_segment || 0), 0);
      handoverMetricsByServiceId.set(sid, {
        had_handover: segments.length > 1,
        initial_driver_name: first?.driver_name || null,
        continuation_driver_name: rest.length ? rest[rest.length - 1].driver_name : null,
        km_initial_driver: kmInitial,
        km_continuation_driver: kmContinuation,
        km_handover_sum: kmInitial + kmContinuation,
      });
    });
  }

  const tripKmCache = new Map();
  const enriched = [];
  for (const r of rows) {
    const plannedKm = await computePlannedKmByTripId(r.gtfs_trip_id, tripKmCache);
    const realizedKm = r.total_km == null ? null : Number(r.total_km);
    const handoverMetrics = handoverMetricsByServiceId.get(Number(r.id)) || {
      had_handover: false,
      initial_driver_name: null,
      continuation_driver_name: null,
      km_initial_driver: 0,
      km_continuation_driver: 0,
      km_handover_sum: 0,
    };
    enriched.push({
      ...r,
      execution_day: formatLisbonExecutionDay(r.started_at, r.ended_at),
      planned_km_line: plannedKm,
      realized_km_service: realizedKm,
      ...handoverMetrics,
    });
  }
  return enriched;
}

function extractRosterAssignmentsFromOperationalPdf(pdfText, context) {
  const lines = String(pdfText || "")
    .split(/\r?\n/)
    .map((l) => l.replace(/\s+/g, " ").trim())
    .filter(Boolean);

  const reports = [];
  const mechanicsSet = context?.mechanicsSet || new Set();
  const driversByNormalizedName = context?.driversByNormalizedName || [];
  let currentDriver = null;

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const normalizedLine = normalizeName(line);

    const mechanicsRaw = line.match(/\b\d{3,8}\b/g) || [];
    const mechanicsInLine = mechanicsRaw.filter((m) => mechanicsSet.has(m));
    const anyMechanicInLine = mechanicsRaw[0] || null;
    const byName = driversByNormalizedName.find((d) => normalizedLine.includes(d.normalizedName));
    const nameBeforeParen = line.match(/^([A-Za-zÀ-ÿ'\-\s]+)\s*\(/);
    const headerLikeName = nameBeforeParen ? nameBeforeParen[1].trim() : null;
    if (mechanicsInLine.length || anyMechanicInLine || byName || headerLikeName) {
      currentDriver = {
        mechanicNumber: mechanicsInLine[0] || anyMechanicInLine || byName?.mechanicNumber || null,
        driverName: byName?.name || headerLikeName || null,
      };
    }
    if (!currentDriver) continue;

    // Service lines usually have multiple times and a "plate-like" first token.
    const timeMatches = line.match(/\b\d{1,2}:\d{2}\b/g) || [];
    if (timeMatches.length < 2) continue;

    const cols = line.split(/\s{2,}/).filter(Boolean);
    const tokens = cols.length > 1 ? cols : line.split(" ").filter(Boolean);
    if (tokens.length < 6) continue;

    const plateMatch = tokens[0].match(/^[0-9]{2,3}[.\-][0-9]{1,3}$/);
    if (!plateMatch) continue;

    const plateNumber = tokens[0];
    const fleetToken = tokens[2] || "";
    const fleetNumberMatch = String(fleetToken).match(/\d{2,6}/);
    const fleetNumber = fleetNumberMatch ? Number(fleetNumberMatch[0]) : null;

    const startTime = toHourMinute(tokens[3]) || toHourMinute(timeMatches[0]);
    const endTime = toHourMinute(tokens[8]) || toHourMinute(timeMatches[timeMatches.length - 1]);
    if (!startTime || !endTime) continue;

    const departure = tokens[4] || "";
    const arrival = tokens[5] || "";
    const serviceType = tokens[6] || "";
    const authority = tokens[7] || "";

    reports.push({
      line: i + 1,
      mechanicNumber: currentDriver.mechanicNumber,
      driverName: currentDriver.driverName,
      plateNumber,
      fleetNumber,
      startTime,
      endTime,
      departure,
      arrival,
      serviceType,
      authority,
      raw: line,
    });
  }

  const seen = new Set();
  return reports.filter((r) => {
    const key = `${r.mechanicNumber}|${r.plateNumber}|${r.fleetNumber || "-"}|${r.startTime}|${r.endTime}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

function extractRosterAssignmentsFromPdfText(pdfText, context) {
  const lines = String(pdfText || "")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);

  const reports = [];
  const serviceCodesSet = context?.serviceCodesSet || new Set();
  const mechanicsSet = context?.mechanicsSet || new Set();
  const driversByNormalizedName = context?.driversByNormalizedName || [];

  for (let i = 0; i < lines.length; i += 1) {
    const line = lines[i];
    const mechanicsInLine = Array.from(new Set((line.match(/\b\d{3,8}\b/g) || []).filter((m) => mechanicsSet.has(m))));
    let chosenMechanic = mechanicsInLine[0] || null;

    // Fallback by driver name if mechanic number is missing in this line.
    if (!chosenMechanic) {
      const normalizedLine = normalizeName(line);
      const byName = driversByNormalizedName.find((d) => normalizedLine.includes(d.normalizedName));
      if (byName) chosenMechanic = byName.mechanicNumber;
    }

    const rawTokens = line.match(/[A-Za-z0-9\-\/]{2,16}/g) || [];
    const validServiceCodes = rawTokens
      .map((t) => normalizeCode(t))
      .filter((code) => code.length >= 2 && serviceCodesSet.has(code));

    let chosenServiceCode = validServiceCodes[0] || null;

    // Fallback: service code may be in next line due to PDF line breaks.
    if (!chosenServiceCode && i + 1 < lines.length) {
      const nextTokens = lines[i + 1].match(/[A-Za-z0-9\-\/]{2,16}/g) || [];
      const nextValidCodes = nextTokens
        .map((t) => normalizeCode(t))
        .filter((code) => code.length >= 2 && serviceCodesSet.has(code));
      chosenServiceCode = nextValidCodes[0] || null;
    }

    if (!chosenMechanic || !chosenServiceCode) continue;

    reports.push({
      line: i + 1,
      raw: line,
      mechanicNumber: chosenMechanic,
      serviceCode: chosenServiceCode,
    });
  }
  // Deduplicate repeated rows produced by wrapped PDFs.
  const seen = new Set();
  return reports.filter((r) => {
    const key = `${r.mechanicNumber}|${r.serviceCode}|${r.line}`;
    if (seen.has(key)) return false;
    seen.add(key);
    return true;
  });
}

router.get("/overview", async (req, res) => {
  try {
    const result = await db.query(OVERVIEW_TODAY_SQL);
    return res.json(result.rows[0]);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao obter resumo do dashboard." });
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

async function ensureSupervisorConflictAlertsTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS supervisor_conflict_alerts (
      id BIGSERIAL PRIMARY KEY,
      driver_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      roster_id INT,
      planned_service_id INT,
      service_schedule VARCHAR(80),
      line_code VARCHAR(40),
      conflict_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
      notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_supervisor_conflict_alerts_created
     ON supervisor_conflict_alerts(created_at DESC)`
  );
}

router.get("/conflict-alerts", async (_req, res) => {
  try {
    await ensureSupervisorConflictAlertsTable();
    const result = await db.query(
      `SELECT
         a.id,
         a.driver_id,
         u.name AS driver_name,
         u.mechanic_number AS driver_mechanic_number,
         a.roster_id,
         a.planned_service_id,
         ps.service_code,
         COALESCE(a.service_schedule, ps.service_schedule) AS service_schedule,
         COALESCE(a.line_code, ps.line_code) AS line_code,
         ps.start_location,
         ps.end_location,
         a.conflict_planned_service_ids,
         a.notes,
         a.created_at
       FROM supervisor_conflict_alerts a
       JOIN users u ON u.id = a.driver_id
       LEFT JOIN planned_services ps ON ps.id = a.planned_service_id
       ORDER BY a.created_at DESC
       LIMIT 200`
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar alertas de conflito." });
  }
});

router.get("/roster/today", async (req, res) => {
  try {
    await ensurePlannedServiceLocationColumns();
    await ensureDriverNotificationsTable();
    const dateRaw = String(req.query.date || "").trim();
    const serviceDate =
      dateRaw && /^\d{4}-\d{2}-\d{2}$/.test(dateRaw) ? dateRaw : null;

    // Corrige linhas «pending» na escala quando já existe execução na app (ex.: reimportação
    // antiga repunha o estado para pending sem alinhar com services).
    await db.query(
      `UPDATE daily_roster dr
       SET status = (
         SELECT CASE
           WHEN EXISTS (
             SELECT 1 FROM services s
             WHERE s.planned_service_id = dr.planned_service_id
               AND s.driver_id = dr.driver_id
               AND LOWER(TRIM(s.status::text)) IN ('in_progress', 'awaiting_handover')
               AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date
           ) THEN 'in_progress'
           WHEN EXISTS (
             SELECT 1 FROM services s
             WHERE s.planned_service_id = dr.planned_service_id
               AND s.driver_id = dr.driver_id
               AND LOWER(TRIM(s.status::text)) = 'completed'
               AND (
                 (s.ended_at IS NOT NULL AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date)
                 OR (
                   s.ended_at IS NULL
                   AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date
                 )
               )
           ) THEN 'completed'
           ELSE dr.status
         END
       )
       WHERE dr.service_date = COALESCE($1::date, CURRENT_DATE)
         AND COALESCE(NULLIF(LOWER(TRIM(dr.status::text)), ''), 'pending') IN ('pending', 'pendente')
         AND (
           EXISTS (
             SELECT 1 FROM services s
             WHERE s.planned_service_id = dr.planned_service_id
               AND s.driver_id = dr.driver_id
               AND LOWER(TRIM(s.status::text)) IN ('in_progress', 'awaiting_handover')
               AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date
           )
           OR EXISTS (
             SELECT 1 FROM services s
             WHERE s.planned_service_id = dr.planned_service_id
               AND s.driver_id = dr.driver_id
               AND LOWER(TRIM(s.status::text)) = 'completed'
               AND (
                 (s.ended_at IS NOT NULL AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date)
                 OR (
                   s.ended_at IS NULL
                   AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date
                 )
               )
           )
         )`,
      [serviceDate]
    );

    const result = await db.query(
      `SELECT
         dr.id AS roster_id,
         dr.service_date,
         dr.status AS roster_status,
         dr.driver_id,
         u.name AS driver_name,
         u.username AS driver_username,
         u.mechanic_number AS driver_mechanic_number,
         ps.id AS planned_service_id,
         ps.service_code,
         ps.line_code,
         ps.fleet_number,
         ps.plate_number,
         ps.service_schedule,
         ps.start_location,
         ps.end_location,
         ps.kms_carga,
         (COALESCE(flags.drs_allowed, false) AND NOT COALESCE(flags.has_block, false)) AS can_reassign,
         CASE
           WHEN NOT COALESCE(flags.drs_allowed, false) THEN 'estado_escala'
           WHEN COALESCE(flags.has_block, false) THEN 'execucao_app'
           ELSE NULL
         END AS reassign_blocked_reason
       FROM daily_roster dr
       JOIN planned_services ps ON ps.id = dr.planned_service_id
       JOIN users u ON u.id = dr.driver_id
       JOIN LATERAL (
         SELECT
           COALESCE(
             COALESCE(NULLIF(LOWER(TRIM(dr.status::text)), ''), 'pending') IN (
               'assigned',
               'pending',
               'pendente',
               'atribuido',
               'atribuído'
             ),
             false
           ) AS drs_allowed,
           EXISTS (
             SELECT 1
             FROM services s
             WHERE s.planned_service_id = dr.planned_service_id
               AND s.driver_id = dr.driver_id
               AND (
                 (
                   LOWER(TRIM(s.status::text)) IN ('in_progress', 'awaiting_handover')
                   AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date
                 )
                 OR (
                   LOWER(TRIM(s.status::text)) = 'completed'
                   AND s.ended_at IS NOT NULL
                   AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date
                 )
               )
           ) AS has_block
       ) AS flags ON true
       WHERE dr.service_date = COALESCE($1::date, CURRENT_DATE)
         AND COALESCE(ps.kms_carga, 0) > 0
       ORDER BY ps.service_schedule ASC NULLS LAST, u.name ASC`,
      [serviceDate]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar escala do dia." });
  }
});

router.patch("/roster/:rosterId/reassign", async (req, res) => {
  const rosterId = Number(req.params.rosterId);
  const { newDriverId, reason } = req.body || {};
  const reasonText = String(reason || "").trim();

  if (!Number.isFinite(rosterId) || rosterId <= 0) {
    return rosterReassignJson(res, 400, { message: "Identificador de escala invalido." });
  }
  const newId = Number(newDriverId);
  if (!Number.isFinite(newId) || newId <= 0) {
    return rosterReassignJson(res, 400, { message: "Selecione o motorista de destino." });
  }
  if (reasonText.length < 5) {
    return rosterReassignJson(res, 400, { message: "Indique um motivo com pelo menos 5 caracteres." });
  }

  const client = await db.pool.connect();
  try {
    await ensurePlannedServiceLocationColumns();
    await ensureDriverNotificationsTable();
    await client.query("BEGIN");

    // Alinhar estado da linha com a tabela services (igual ideia ao GET /roster/today), antes de validar.
    await client.query(
      `UPDATE daily_roster dr
       SET status = (
         SELECT CASE
           WHEN EXISTS (
             SELECT 1 FROM services s
             WHERE s.planned_service_id = dr.planned_service_id
               AND s.driver_id = dr.driver_id
               AND LOWER(TRIM(s.status::text)) IN ('in_progress', 'awaiting_handover')
               AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date
           ) THEN 'in_progress'::varchar
           WHEN EXISTS (
             SELECT 1 FROM services s
             WHERE s.planned_service_id = dr.planned_service_id
               AND s.driver_id = dr.driver_id
               AND LOWER(TRIM(s.status::text)) = 'completed'
               AND s.ended_at IS NOT NULL
               AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = dr.service_date
           ) THEN 'completed'::varchar
           ELSE dr.status
         END
       )
       WHERE dr.id = $1`,
      [rosterId]
    );

    const rosterRes = await client.query(
      `SELECT dr.id, dr.driver_id, dr.planned_service_id, dr.service_date, dr.status,
              ps.service_code, ps.line_code, ps.fleet_number, ps.plate_number, ps.service_schedule,
              u_old.name AS old_driver_name
       FROM daily_roster dr
       JOIN planned_services ps ON ps.id = dr.planned_service_id
       JOIN users u_old ON u_old.id = dr.driver_id
       WHERE dr.id = $1
       FOR UPDATE`,
      [rosterId]
    );
    if (!rosterRes.rowCount) {
      await client.query("ROLLBACK");
      return rosterReassignJson(res, 404, { message: "Linha de escala nao encontrada." });
    }
    const dr = rosterRes.rows[0];
    if (dailyRosterStatusBlocksReassign(dr.status)) {
      await client.query("ROLLBACK");
      const estadoPt = dailyRosterStatusMessagePt(dr.status);
      return rosterReassignJson(res, 409, {
        message: `Não é possível reatribuir: a escala já consta como «${estadoPt}» (viagem em curso ou concluída neste dia).`,
      });
    }

    const blockRes = await client.query(
      `SELECT 1
       FROM services s
       WHERE s.planned_service_id = $1
         AND s.driver_id = $3
         AND (
           (
             LOWER(TRIM(s.status::text)) IN ('in_progress', 'awaiting_handover')
             AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = $2::date
           )
           OR (
             LOWER(TRIM(s.status::text)) = 'completed'
             AND s.ended_at IS NOT NULL
             AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = $2::date
           )
         )
       LIMIT 1`,
      [dr.planned_service_id, dr.service_date, dr.driver_id]
    );
    if (blockRes.rowCount > 0) {
      await client.query("ROLLBACK");
      return rosterReassignJson(res, 409, {
        message:
          "Existe na aplicacao, para este motorista e planificacao neste dia, um servico ja iniciado ou concluido (registo de viagem). Nao e possivel alterar o escalamento ate esse registo ser coerente com a escala.",
      });
    }

    if (newId === dr.driver_id) {
      await client.query("ROLLBACK");
      return rosterReassignJson(res, 400, { message: "Escolha um motorista diferente do atual." });
    }

    const newDriverRes = await client.query(
      `SELECT id, name, username, mechanic_number, role, is_active
       FROM users
       WHERE id = $1
       FOR UPDATE`,
      [newId]
    );
    if (!newDriverRes.rowCount) {
      await client.query("ROLLBACK");
      return rosterReassignJson(res, 404, { message: "Motorista de destino nao encontrado." });
    }
    const nd = newDriverRes.rows[0];
    if (normalizeRole(nd.role) !== "driver" || !nd.is_active) {
      await client.query("ROLLBACK");
      return rosterReassignJson(res, 400, { message: "O destino tem de ser um motorista ativo." });
    }

    const dupRes = await client.query(
      `SELECT id FROM daily_roster
       WHERE driver_id = $1
         AND planned_service_id = $2
         AND service_date = $3::date
         AND id <> $4
       LIMIT 1`,
      [newId, dr.planned_service_id, dr.service_date, rosterId]
    );
    if (dupRes.rowCount > 0) {
      await client.query("ROLLBACK");
      return rosterReassignJson(res, 409, {
        message: "O motorista selecionado ja tem este servico na mesma data.",
      });
    }

    const updRes = await client.query(
      `UPDATE daily_roster SET driver_id = $1 WHERE id = $2 RETURNING id, driver_id`,
      [newId, rosterId]
    );
    if (!updRes.rowCount) {
      await client.query("ROLLBACK");
      return rosterReassignJson(res, 404, {
        message:
          "Nao foi possivel actualizar a escala (linha inexistente ou ja arquivada). Actualize a pagina e confirme a data.",
      });
    }

    const dateLabel = dr.service_date
      ? new Date(`${dr.service_date}T12:00:00`).toLocaleDateString("pt-PT")
      : String(dr.service_date);
    const svcLabel = `${dr.service_code} | Linha ${dr.line_code} | ${dr.service_schedule} | Frota ${dr.fleet_number}`;

    await client.query(
      `INSERT INTO driver_notifications (driver_id, title, message, notification_type, roster_id)
       VALUES ($1, $2, $3, 'roster_assigned', $4)`,
      [
        newId,
        "Novo servico na sua escala",
        `Foi atribuido o servico ${svcLabel} na data ${dateLabel}. Motivo (supervisor): ${reasonText}`,
        rosterId,
      ]
    );

    await client.query(
      `INSERT INTO driver_notifications (driver_id, title, message, notification_type, roster_id)
       VALUES ($1, $2, $3, 'roster_removed', $4)`,
      [
        dr.driver_id,
        "Servico removido da sua escala",
        `O servico ${svcLabel} na data ${dateLabel} foi atribuido a ${nd.name}. Motivo: ${reasonText}`,
        rosterId,
      ]
    );

    await client.query("COMMIT");
    return rosterReassignJson(res, 200, {
      roster_id: rosterId,
      new_driver_id: newId,
      new_driver_name: nd.name,
      message: "Escalamento atualizado e notificacoes registadas.",
    });
  } catch (error) {
    try {
      await client.query("ROLLBACK");
    } catch (_rollbackErr) {
      // ignore
    }
    console.error("roster reassign", error);
    return rosterReassignJson(res, 500, { message: "Erro ao alterar escalamento." });
  } finally {
    client.release();
  }
});

router.get("/drivers", async (req, res) => {
  const { company } = req.query;
  try {
    const where = [];
    const values = [];
    if (company) {
      where.push("company_name = $1");
      values.push(company);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const result = await db.query(
      `SELECT id, name, username, email, mechanic_number, company_name, is_active, role, created_at
       FROM users
       ${whereSql}
       ORDER BY created_at DESC`,
      values
    );
    return res.json(result.rows);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao listar motoristas." });
  }
});

router.post("/drivers", async (req, res) => {
  const { name, username, email, mechanicNumber, password, companyName, isActive } = req.body;
  if (!name || !username || !mechanicNumber || !password) {
    return res.status(400).json({ message: "Preencha nome, username, numero mecanografico e password." });
  }
  try {
    const bcrypt = require("bcryptjs");
    const hash = await bcrypt.hash(password, 10);
    const normalizedEmail = String(email || "").trim() || null;
    const result = await db.query(
      `INSERT INTO users (name, username, email, mechanic_number, company_name, is_active, role, password_hash)
       VALUES ($1, $2, $3, $4, $5, $6, 'driver', $7)
       RETURNING id, name, username, email, mechanic_number, company_name, is_active, role`,
      [name, username, normalizedEmail, mechanicNumber, companyName || null, isActive !== false, hash]
    );
    return res.status(201).json(result.rows[0]);
  } catch (error) {
    if (error.code === "23505") {
      return res.status(409).json({ message: "Username, email ou numero mecanografico ja existe." });
    }
    return res.status(500).json({ message: "Erro ao criar motorista." });
  }
});

router.post("/users", async (req, res) => {
  const { name, username, email, password, role, isActive } = req.body || {};
  const allowedRoles = ["viewer", "supervisor", "admin"];
  const normalizedRole = normalizeRole(role);
  if (!name || !username || !password || !role) {
    return res.status(400).json({ message: "Preencha nome, username, password e perfil." });
  }
  if (!allowedRoles.includes(normalizedRole)) {
    return res.status(400).json({ message: "Perfil invalido para utilizador de acesso." });
  }

  try {
    const bcrypt = require("bcryptjs");
    const hash = await bcrypt.hash(password, 10);
    const normalizedEmail = String(email || "").trim() || null;
    const fallbackEmail = normalizedEmail || buildFallbackEmail(username, normalizedRole);
    const result = await db.query(
      `INSERT INTO users (name, username, email, role, is_active, password_hash)
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, name, username, email, role, is_active`,
      [name, username, fallbackEmail, normalizedRole, isActive !== false, hash]
    );
    return res.status(201).json(result.rows[0]);
  } catch (error) {
    if (error.code === "23505") {
      return res.status(409).json({ message: "Username ou email ja existe." });
    }
    return res.status(500).json({ message: "Erro ao criar utilizador de acesso." });
  }
});

router.patch("/drivers/:driverId", async (req, res) => {
  const { driverId } = req.params;
  const { name, username, email, mechanicNumber, companyName, isActive } = req.body;
  try {
    const result = await db.query(
      `UPDATE users
       SET name = COALESCE($2, name),
           username = COALESCE($3, username),
           email = COALESCE($4, email),
           mechanic_number = COALESCE($5, mechanic_number),
           company_name = COALESCE($6, company_name),
           is_active = COALESCE($7, is_active)
       WHERE id = $1 AND role = 'driver'
       RETURNING id, name, username, email, mechanic_number, company_name, is_active, role`,
      [driverId, name || null, username || null, email || null, mechanicNumber || null, companyName || null, isActive]
    );
    if (!result.rowCount) {
      return res.status(404).json({ message: "Motorista nao encontrado." });
    }
    return res.json(result.rows[0]);
  } catch (error) {
    if (error.code === "23505") {
      return res.status(409).json({ message: "Username, email ou numero mecanografico duplicado." });
    }
    return res.status(500).json({ message: "Erro ao atualizar motorista." });
  }
});

router.patch("/users/password-reset", async (req, res) => {
  const { username, newPassword, allowedRoles, activateUser } = req.body || {};
  const normalizedUsername = String(username || "").trim();
  const passwordText = String(newPassword || "");
  const rolesList = Array.isArray(allowedRoles)
    ? allowedRoles.map((r) => normalizeRole(r)).filter(Boolean)
    : [];

  if (!normalizedUsername || !passwordText) {
    return res.status(400).json({ message: "Indique username e nova password." });
  }
  if (passwordText.length < 4) {
    return res.status(400).json({ message: "A nova password deve ter pelo menos 4 caracteres." });
  }

  try {
    const userResult = await db.query(
      `SELECT id, username, role, is_active
       FROM users
       WHERE LOWER(username) = LOWER($1)
       LIMIT 1`,
      [normalizedUsername]
    );
    if (!userResult.rowCount) {
      return res.status(404).json({ message: "Utilizador nao encontrado." });
    }

    const user = userResult.rows[0];
    const userRole = normalizeRole(user.role);
    if (rolesList.length && !rolesList.includes(userRole)) {
      return res.status(400).json({
        message: `Este utilizador nao pertence ao perfil esperado (${rolesList.join(", ")}).`,
      });
    }

    const bcrypt = require("bcryptjs");
    const hash = await bcrypt.hash(passwordText, 10);
    const updated = await db.query(
      `UPDATE users
       SET password_hash = $2,
           is_active = CASE WHEN $3::boolean IS TRUE THEN TRUE ELSE is_active END
       WHERE id = $1
       RETURNING id, username, role, is_active`,
      [user.id, hash, activateUser === true]
    );
    return res.json({
      message: "Password atualizada com sucesso.",
      user: updated.rows[0],
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar password do utilizador." });
  }
});

const ACCESS_USER_ROLES = new Set(["viewer", "viewr", "supervisor", "admin"]);

router.patch("/users/:userId", async (req, res) => {
  const userId = Number(req.params.userId);
  const { isActive } = req.body || {};
  const actorId = Number(req.user?.id);

  if (!Number.isFinite(userId) || userId <= 0) {
    return res.status(400).json({ message: "Identificador de utilizador invalido." });
  }
  if (typeof isActive !== "boolean") {
    return res.status(400).json({ message: "Indique isActive (true ou false)." });
  }
  if (!isActive && Number.isFinite(actorId) && actorId === userId) {
    return res.status(400).json({ message: "Nao pode desativar a sua propria conta nesta sessao." });
  }

  try {
    const existing = await db.query(
      `SELECT id, role FROM users WHERE id = $1 LIMIT 1`,
      [userId]
    );
    if (!existing.rowCount) {
      return res.status(404).json({ message: "Utilizador nao encontrado." });
    }
    const userRole = normalizeRole(existing.rows[0].role);
    if (!ACCESS_USER_ROLES.has(userRole)) {
      return res.status(400).json({
        message: "Só e possivel ativar/desativar utilizadores de acesso (visualizacao, supervisor ou administrador).",
      });
    }

    const result = await db.query(
      `UPDATE users SET is_active = $2 WHERE id = $1
       RETURNING id, name, username, email, role, is_active`,
      [userId, isActive]
    );
    return res.json(result.rows[0]);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar estado do utilizador." });
  }
});

router.post("/drivers/import", async (req, res) => {
  const { csvText, fileBase64, fileType, defaultCompany } = req.body;
  if (!csvText && !fileBase64) {
    return res.status(400).json({ message: "Forneca csvText ou ficheiro (base64)." });
  }

  let rows = [];
  if (fileBase64) {
    try {
      const buffer = Buffer.from(fileBase64, "base64");
      const normalizedFileType = String(fileType || "").trim().toLowerCase();
      if (normalizedFileType === "csv") {
        rows = parseCsvText(buffer.toString("utf8"));
      } else {
        const workbook = XLSX.read(buffer, { type: "buffer" });
        const firstSheetName = workbook.SheetNames[0];
        const firstSheet = workbook.Sheets[firstSheetName];
        rows = normalizeImportRows(XLSX.utils.sheet_to_json(firstSheet, { defval: "" }));
      }
    } catch (_error) {
      return res.status(400).json({ message: "Ficheiro invalido para importacao (use CSV ou Excel)." });
    }
  } else {
    rows = parseCsvText(csvText);
  }

  if (!rows.length) {
    return res.status(400).json({ message: "Ficheiro sem dados validos." });
  }

  const bcrypt = require("bcryptjs");
  let inserted = 0;
  let updated = 0;
  const errors = [];
  const rowReports = [];
  for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    const name = row.name || row.nome;
    const username = row.username || row.utilizador;
    const email = String(row.email || "").trim() || null;
    const mechanicNumber = row.mechanic_number || row.mecanografico || row.numero_mecanografico;
    const password = row.password || row.senha || "123456";
    const companyName = row.company_name || row.empresa || defaultCompany || null;
    const isActive = parseBooleanLike(row.is_active ?? row.ativo, true);
    const line = rowIndex + 2;
    const rowKey = username || email || mechanicNumber || `linha_${line}`;

    if (!name || !username || !mechanicNumber) {
      const message = "Campos obrigatorios em falta (name, username, mechanic_number).";
      errors.push(`Linha ${line}: ${message}`);
      rowReports.push({
        line,
        key: rowKey,
        status: "error",
        action: "ignored",
        message,
      });
      continue;
    }

    try {
      const existing = await db.query(
        `SELECT id FROM users
         WHERE username = $1 OR mechanic_number = $2
            OR ($3 IS NOT NULL AND email = $3)
         LIMIT 1`,
        [username, mechanicNumber, email]
      );
      if (existing.rowCount) {
        await db.query(
          `UPDATE users
           SET name = $2,
               username = $3,
               email = COALESCE($4, email),
               mechanic_number = $5,
               company_name = $6,
               is_active = $7,
               role = 'driver'
           WHERE id = $1`,
          [existing.rows[0].id, name, username, email, mechanicNumber, companyName, isActive]
        );
        updated += 1;
        rowReports.push({
          line,
          key: rowKey,
          status: "ok",
          action: "updated",
          message: "Motorista atualizado.",
        });
      } else {
        const hash = await bcrypt.hash(password, 10);
        await db.query(
          `INSERT INTO users (name, username, email, mechanic_number, company_name, is_active, role, password_hash)
           VALUES ($1, $2, $3, $4, $5, $6, 'driver', $7)`,
          [name, username, email, mechanicNumber, companyName, isActive, hash]
        );
        inserted += 1;
        rowReports.push({
          line,
          key: rowKey,
          status: "ok",
          action: "inserted",
          message: "Motorista inserido.",
        });
      }
    } catch (error) {
      const message =
        error?.code === "23505"
          ? "Conflito de username/email/mecanografico."
          : "Erro inesperado ao processar a linha.";
      errors.push(`Linha ${line}: ${message}`);
      rowReports.push({
        line,
        key: rowKey,
        status: "error",
        action: "failed",
        message,
      });
    }
  }

  return res.json({
    inserted,
    updated,
    failed: rowReports.filter((r) => r.status === "error").length,
    errors,
    rowReports,
  });
});


router.post("/roster/import", async (req, res) => {
  const { csvText, fileBase64, fileType, serviceDate, dryRun } = req.body || {};
  if (!csvText && !fileBase64) {
    return res.status(400).json({ message: "Fornece csvText ou ficheiro (base64)." });
  }

  const parsedDate = parseDateOnly(serviceDate) || new Date().toISOString().slice(0, 10);
  try {
    await ensurePlannedServiceLocationColumns();

    let rows = [];
    if (fileBase64) {
      const buffer = Buffer.from(fileBase64, "base64");
      if (String(fileType || "").toLowerCase() === "xlsx") {
        const workbook = XLSX.read(buffer, { type: "buffer" });
        const firstSheet = workbook.Sheets[workbook.SheetNames[0]];
        rows = normalizeImportRows(XLSX.utils.sheet_to_json(firstSheet, { defval: "" }));
      } else {
        rows = parseCsvText(buffer.toString("utf8"));
      }
    } else {
      rows = parseCsvText(csvText);
    }

    if (!rows.length) {
      return res.status(400).json({ message: "Ficheiro sem linhas validas para importacao." });
    }

    const driversResult = await db.query(
      `SELECT id, name, mechanic_number
       FROM users
       WHERE role = 'driver'`
    );
    const plannedResult = await db.query(
      `SELECT id, service_code, line_code, start_location, end_location, kms_carga, fleet_number, plate_number, service_schedule
       FROM planned_services
       WHERE service_code IS NOT NULL`
    );

    const driversByMechanic = new Map();
    const driversByName = new Map();
    driversResult.rows.forEach((d) => {
      const mec = String(d.mechanic_number || "").trim();
      const normalizedMec = normalizeMechanicNumber(mec);
      if (mec) driversByMechanic.set(mec, d);
      if (normalizedMec) driversByMechanic.set(normalizedMec, d);
      driversByName.set(normalizeName(d.name), d);
    });
    const plannedByCode = new Map();
    const plannedByComposite = new Map();
    plannedResult.rows.forEach((p) => {
      plannedByCode.set(normalizeCode(p.service_code), p);
      const composite = [
        String(p.plate_number || "").trim(),
        String(p.fleet_number || "").trim(),
        String(p.service_schedule || "").trim(),
      ].join("|");
      plannedByComposite.set(composite, p);
    });

    const kmEscalaRule = buildKmEscalaRuleFromImportRows(rows);

    let inserted = 0;
    let updated = 0;
    let failed = 0;
    const rowReports = [];

    for (let idx = 0; idx < rows.length; idx += 1) {
      const row = rows[idx];
      const line = idx + 2;
      const obsText = pickObservacoesFromImportRow(row) || "";
      const kmByNorm = importRowByNormalizedKeys(row);
      const kmsCarga = pickKmFromNormalizedRow(kmByNorm, KMS_CARGA_KEYS);
      const kmSkip = kmEscalaRowShouldSkip(row, kmEscalaRule);
      if (kmSkip.skip) {
        rowReports.push({
          line,
          status: "ignored",
          action: "skipped",
          message: kmSkip.message,
          mechanicNumber: "-",
          driverName: "-",
          serviceCode: "-",
          plateNumber: "-",
          fleetNumber: "-",
          serviceSchedule: "-",
          kmsCarga: kmsCarga ?? "-",
        });
        continue;
      }
      const carreiraEscalaRaw = String(row.numero_carreiraescala || row.numerocarreiraescala || row.numero_carreira || row.numerocarreira || "").trim();
      const mechanicNumber = String(
        row.mechanic_number ||
          row.mecanografico ||
          row.numero_mecanografico ||
          row.mechanicnumber ||
          row.numeromecanografico ||
          ""
      ).trim();
      const driverNameRaw = String(row.driver_name || row.nome || row.name || row.nomeescala || "").trim();
      const serviceCodeRaw = String(
        row.service_code ||
          row.servico ||
          row.codigo_servico ||
          row.cod_servico ||
          row.servicecode ||
          carreiraEscalaRaw ||
          ""
      ).trim();
      const dateRaw = String(
        row.service_date || row.data || row.date || row.data_inicio || row.datainicio || parsedDate
      ).trim();
      const rowServiceDate = parseDateOnly(dateRaw) || parsedDate;
      const startTime = toHourMinute(row.hora_inicio || row.horainicio || row.start_time || row.hora_inicio_servico || "");
      const endTime = toHourMinute(row.hora_fim || row.horafim || row.end_time || row.hora_fim_servico || "");
      const serviceSchedule =
        String(row.service_schedule || row.horario_servico || "").trim() ||
        (startTime && endTime ? `${startTime}-${endTime}` : "");
      const plateNumber = String(row.numero_chapa || row.numerochapa || row.chapa || "").trim();
      const fleetNumberRaw = String(row.numero_frota || row.numerofrota || row.frota || "").trim();
      const fleetNumber = fleetNumberRaw ? Number(fleetNumberRaw) : null;
      const startLocation = pickStartLocationFromRow(row);
      const endLocation = pickEndLocationFromRow(row);
      const lineCodeRaw = String(
        carreiraEscalaRaw ||
          row.line_code ||
          row.linha ||
          [startLocation, endLocation].filter(Boolean).join(" -> ")
      ).trim();
      const hasOperationalFields = Boolean(plateNumber && fleetNumberRaw && serviceSchedule && lineCodeRaw);
      const isNonOperationalRow = importRowIsNonOperational(startLocation, endLocation, obsText);

      if (isNonOperationalRow) {
        rowReports.push({
          line,
          status: "ignored",
          action: "skipped",
          message: "Linha nao operacional (pausa/abastecimento/outros).",
          mechanicNumber: mechanicNumber || "-",
          driverName: driverNameRaw || "-",
          serviceCode: serviceCodeRaw || "-",
          plateNumber: plateNumber || "-",
          fleetNumber: fleetNumberRaw || "-",
          serviceSchedule: serviceSchedule || "-",
        });
        continue;
      }

      const normalizedMechanic = normalizeMechanicNumber(mechanicNumber);
      let driver = mechanicNumber ? driversByMechanic.get(mechanicNumber) : null;
      if (!driver && normalizedMechanic) driver = driversByMechanic.get(normalizedMechanic);
      if (!driver && driverNameRaw) {
        driver = driversByName.get(normalizeName(driverNameRaw)) || findDriverByApproxName(driversResult.rows, driverNameRaw);
      }
      let planned =
        serviceCodeRaw
          ? plannedByCode.get(normalizeCode(serviceCodeRaw))
          : null;
      if (!planned && plateNumber && serviceSchedule) {
        const composite = [plateNumber, String(fleetNumber || ""), serviceSchedule].join("|");
        planned = plannedByComposite.get(composite) || null;
      }

      if (!planned && (serviceCodeRaw || plateNumber || serviceSchedule)) {
        if (!hasOperationalFields) {
          failed += 1;
          const missingSchedule = !String(serviceSchedule || "").trim();
          const msg = missingSchedule
            ? "Linha sem horario reconhecido (horario_servico ou hora_inicio e hora_fim com formato HH:MM). Verifique tambem chapa, frota e carreira/linha."
            : "Linha sem dados operacionais suficientes (chapa/frota/horario/carreira).";
          rowReports.push({
            line,
            status: "error",
            action: "ignored",
            message: msg,
            mechanicNumber: mechanicNumber || "-",
            driverName: driverNameRaw || "-",
            serviceCode: serviceCodeRaw || "-",
            plateNumber: plateNumber || "-",
            fleetNumber: fleetNumberRaw || "-",
            serviceSchedule: serviceSchedule || "-",
          });
          continue;
        }
        let newCode = normalizeCode(
          [
            carreiraEscalaRaw || serviceCodeRaw || "ESC",
            plateNumber || "SEMCHAPA",
            Number.isFinite(fleetNumber) ? String(fleetNumber) : "SEMFROTA",
            serviceSchedule || "SEMHORARIO",
          ].join("_")
        );
        if (!newCode) {
          newCode = normalizeCode(`ESC${rowServiceDate.replace(/-/g, "")}${mechanicNumber}${plateNumber}${serviceSchedule}`).slice(
            0,
            50
          );
        }
        let candidate = newCode || `ESC${Date.now()}`;
        let suffix = 1;
        while (plannedByCode.has(candidate)) {
          suffix += 1;
          candidate = `${(newCode || "ESC").slice(0, 46)}${String(suffix).padStart(2, "0")}`;
        }

        const insertedPlanned = await db.query(
          `INSERT INTO planned_services (service_code, line_code, start_location, end_location, kms_carga, fleet_number, plate_number, service_schedule)
           VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
           RETURNING id, service_code, line_code, start_location, end_location, kms_carga, fleet_number, plate_number, service_schedule`,
          [
            candidate,
            lineCodeRaw,
            startLocation || null,
            endLocation || null,
            kmsCarga,
            Number.isFinite(fleetNumber) ? String(fleetNumber) : fleetNumberRaw,
            plateNumber,
            serviceSchedule,
          ]
        );
        planned = insertedPlanned.rows[0];
        plannedByCode.set(normalizeCode(planned.service_code), planned);
        plannedByComposite.set(
          [String(planned.plate_number || "").trim(), String(planned.fleet_number || "").trim(), String(planned.service_schedule || "").trim()].join(
            "|"
          ),
          planned
        );
      }

      if (!driver || !planned) {
        failed += 1;
        rowReports.push({
          line,
          status: "error",
          action: "ignored",
          message: !driver && !planned ? "Motorista e servico nao encontrados." : !driver ? "Motorista nao encontrado." : "Servico nao encontrado.",
          mechanicNumber: mechanicNumber || "-",
          driverName: driverNameRaw || "-",
          serviceCode: serviceCodeRaw || "-",
          plateNumber: plateNumber || "-",
          fleetNumber: fleetNumberRaw || "-",
          serviceSchedule: serviceSchedule || "-",
        });
        continue;
      }

      if (!dryRun && (startLocation || endLocation || kmsCarga != null)) {
        await db.query(
          `UPDATE planned_services
           SET start_location = CASE
                 WHEN $1::text IS NOT NULL AND LENGTH(TRIM($1::text)) > 0 THEN LEFT(TRIM($1::text), 120)
                 ELSE start_location
               END,
               end_location = CASE
                 WHEN $2::text IS NOT NULL AND LENGTH(TRIM($2::text)) > 0 THEN LEFT(TRIM($2::text), 120)
                 ELSE end_location
               END,
               kms_carga = COALESCE($3::numeric, kms_carga)
           WHERE id = $4`,
          [startLocation || null, endLocation || null, kmsCarga, planned.id]
        );
        if (startLocation) planned.start_location = startLocation;
        if (endLocation) planned.end_location = endLocation;
        if (kmsCarga != null) planned.kms_carga = kmsCarga;
      }

      if (dryRun) {
        rowReports.push({
          line,
          status: "ok",
          action: "preview",
          message: "Valido para importacao.",
          mechanicNumber: driver.mechanic_number || mechanicNumber || "-",
          driverName: driver.name,
          serviceCode: planned.service_code,
          plateNumber: plateNumber || planned.plate_number || "-",
          fleetNumber: String(Number.isFinite(fleetNumber) ? fleetNumber : planned.fleet_number || "-"),
          serviceSchedule: serviceSchedule || planned.service_schedule || "-",
          start_location: startLocation || planned.start_location || "-",
          end_location: endLocation || planned.end_location || "-",
          kmsCarga: kmsCarga ?? planned.kms_carga ?? "-",
        });
        continue;
      }

      const saveResult = await db.query(
        `INSERT INTO daily_roster (driver_id, planned_service_id, service_date, status)
         VALUES ($1, $2, $3, 'pending')
         ON CONFLICT (driver_id, planned_service_id, service_date)
         DO UPDATE SET status = CASE
           WHEN daily_roster.status IN ('in_progress', 'completed') THEN daily_roster.status
           ELSE EXCLUDED.status
         END
         RETURNING (xmax = 0) AS inserted`,
        [driver.id, planned.id, rowServiceDate]
      );
      if (saveResult.rows[0]?.inserted) inserted += 1;
      else updated += 1;

      rowReports.push({
        line,
        status: "ok",
        action: saveResult.rows[0]?.inserted ? "inserted" : "updated",
        message: "Escala importada.",
        mechanicNumber: driver.mechanic_number || mechanicNumber || "-",
        driverName: driver.name,
        serviceCode: planned.service_code,
        plateNumber: plateNumber || planned.plate_number || "-",
        fleetNumber: String(Number.isFinite(fleetNumber) ? fleetNumber : planned.fleet_number || "-"),
        serviceSchedule: serviceSchedule || planned.service_schedule || "-",
        start_location: planned.start_location || startLocation || "-",
        end_location: planned.end_location || endLocation || "-",
        kmsCarga: kmsCarga ?? planned.kms_carga ?? "-",
      });
    }

    return res.json({
      serviceDate: parsedDate,
      parsedLines: rows.length,
      inserted,
      updated,
      failed,
      ignored: rowReports.filter((r) => r.status === "ignored").length,
      dryRun: Boolean(dryRun),
      rowReports,
    });
  } catch (error) {
    return res.status(400).json({
      message: `Erro ao processar importacao da escala: ${error?.message || "erro desconhecido"}`,
    });
  }
});

router.get("/drivers/import-template.csv", async (_req, res) => {
  const header = ["name", "username", "email", "mechanic_number", "password", "company_name", "is_active"];
  const sampleRows = [
    ["Joao Silva", "joao.silva", "joao@empresa.pt", "10001", "123456", "Rodotejo", "true"],
    ["Ana Costa", "ana.costa", "ana@empresa.pt", "10002", "123456", "Rodotejo", "false"],
  ];
  const csv = [header.map(csvEscape).join(","), ...sampleRows.map((r) => r.map(csvEscape).join(","))].join("\n");
  res.setHeader("Content-Type", "text/csv; charset=utf-8");
  res.setHeader("Content-Disposition", "attachment; filename=template_importacao_motoristas.csv");
  return res.status(200).send(csv);
});

router.get("/drivers/import-template.xlsx", async (_req, res) => {
  const rows = [
    {
      name: "Joao Silva",
      username: "joao.silva",
      email: "joao@empresa.pt",
      mechanic_number: "10001",
      password: "123456",
      company_name: "Rodotejo",
      is_active: "true",
    },
    {
      name: "Ana Costa",
      username: "ana.costa",
      email: "ana@empresa.pt",
      mechanic_number: "10002",
      password: "123456",
      company_name: "Rodotejo",
      is_active: "false",
    },
  ];
  const sheet = XLSX.utils.json_to_sheet(rows);
  const book = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(book, sheet, "motoristas");
  const buffer = XLSX.write(book, { type: "buffer", bookType: "xlsx" });
  res.setHeader(
    "Content-Type",
    "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
  );
  res.setHeader("Content-Disposition", "attachment; filename=template_importacao_motoristas.xlsx");
  return res.status(200).send(buffer);
});

router.get("/drivers/export.csv", async (req, res) => {
  const { company } = req.query;
  try {
    const where = [];
    const values = [];
    if (company) {
      where.push(`company_name = $1`);
      values.push(company);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const result = await db.query(
      `SELECT id, name, username, email, mechanic_number, company_name, is_active, role, created_at
       FROM users
       ${whereSql}
       ORDER BY created_at DESC`,
      values
    );

    const header = [
      "id",
      "name",
      "username",
      "email",
      "mechanic_number",
      "company_name",
      "is_active",
      "role",
      "created_at",
    ];
    const rows = result.rows.map((r) =>
      [r.id, r.name, r.username, r.email, r.mechanic_number, r.company_name, r.is_active, r.role, r.created_at]
        .map(csvEscape)
        .join(",")
    );
    const csv = [header.map(csvEscape).join(","), ...rows].join("\n");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=motoristas.csv");
    return res.status(200).send(csv);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao exportar motoristas." });
  }
});

router.get("/drivers/export.xlsx", async (req, res) => {
  const { company } = req.query;
  try {
    const where = [];
    const values = [];
    if (company) {
      where.push(`company_name = $1`);
      values.push(company);
    }
    const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";
    const result = await db.query(
      `SELECT id, name, username, email, mechanic_number, company_name, is_active, role, created_at
       FROM users
       ${whereSql}
       ORDER BY created_at DESC`,
      values
    );
    const rows = result.rows.map((r) => ({
      id: r.id,
      name: r.name,
      username: r.username,
      email: r.email,
      mechanic_number: r.mechanic_number,
      company_name: r.company_name,
      is_active: r.is_active,
      role: r.role,
      created_at: r.created_at,
    }));
    const sheet = XLSX.utils.json_to_sheet(rows);
    const book = XLSX.utils.book_new();
    XLSX.utils.book_append_sheet(book, sheet, "motoristas");
    const buffer = XLSX.write(book, { type: "buffer", bookType: "xlsx" });
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader("Content-Disposition", "attachment; filename=motoristas.xlsx");
    return res.status(200).send(buffer);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao exportar motoristas em Excel." });
  }
});

router.get("/services", async (req, res) => {
  const { driverId, lineCode, status, fromDate, toDate } = req.query;
  const where = [];
  const values = [];
  let i = 1;

  if (driverId) {
    where.push(`s.driver_id = $${i}`);
    values.push(driverId);
    i += 1;
  }
  if (lineCode) {
    where.push(`s.line_code = $${i}`);
    values.push(lineCode);
    i += 1;
  }
  if (status) {
    where.push(`s.status = $${i}`);
    values.push(status);
    i += 1;
  }

  const dayFilter = serviceActivityLisbonDayFilter(fromDate, toDate, i);
  if (dayFilter) {
    where.push(dayFilter.sql);
    dayFilter.values.forEach((v) => values.push(v));
    i = dayFilter.nextIndex;
  }

  where.push(`(s.planned_service_id IS NULL OR COALESCE(ps.kms_carga, 0) > 0)`);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  try {
    const result = await db.query(
      `SELECT
         s.id,
         s.driver_id,
         u.name AS driver_name,
         s.plate_number,
         s.service_schedule,
         s.line_code,
         s.fleet_number,
         s.status,
         s.gtfs_trip_id,
         s.started_at,
         s.ended_at,
         s.total_km,
         s.route_deviation_m,
         s.is_off_route
       FROM services s
       JOIN users u ON u.id = s.driver_id
       LEFT JOIN planned_services ps ON ps.id = s.planned_service_id
       ${whereSql}
       ORDER BY s.started_at DESC`,
      values
    );
    const rowsWithMetrics = await enrichServiceRowsForExport(result.rows);
    return res.json(rowsWithMetrics);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao listar servicos do dashboard." });
  }
});

router.get("/services/live", async (_req, res) => {
  try {
    const routeColorColumnRes = await db.query(
      `SELECT 1
       FROM information_schema.columns
       WHERE table_name = 'gtfs_routes'
         AND column_name = 'route_color'
       LIMIT 1`
    );
    const hasRouteColor = routeColorColumnRes.rowCount > 0;
    const routeColorSelect = hasRouteColor ? "gr.route_color" : "NULL::text AS route_color";
    const routeColorJoins = hasRouteColor
      ? `LEFT JOIN gtfs_trips gt ON gt.trip_id = s.gtfs_trip_id
         LEFT JOIN gtfs_routes gr ON gr.route_id = gt.route_id`
      : "";

    const result = await db.query(
      `SELECT
         s.id,
         s.line_code,
         s.fleet_number,
         s.service_schedule,
         s.status,
         u.name AS driver_name,
         lp.lat,
         lp.lng,
         lp.captured_at,
         ${routeColorSelect}
       FROM services s
       JOIN users u ON u.id = s.driver_id
       LEFT JOIN LATERAL (
         SELECT sp.lat, sp.lng, sp.captured_at
         FROM service_points sp
         WHERE sp.service_id = s.id
         ORDER BY sp.captured_at DESC
         LIMIT 1
       ) lp ON true
       ${routeColorJoins}
       WHERE s.status = 'in_progress'
       ORDER BY s.started_at DESC`
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar serviços em execução." });
  }
});

router.patch("/services/:serviceId", async (req, res) => {
  const { serviceId } = req.params;
  const { status, fleetNumber } = req.body || {};
  const allowedStatus = ["pending", "in_progress", "awaiting_handover", "completed", "cancelled"];

  if (status && !allowedStatus.includes(status)) {
    return res.status(400).json({ message: "Estado invalido." });
  }

  const parsedFleetNumber =
    fleetNumber == null || fleetNumber === ""
      ? null
      : Number(fleetNumber);

  if (fleetNumber != null && fleetNumber !== "" && Number.isNaN(parsedFleetNumber)) {
    return res.status(400).json({ message: "Frota invalida." });
  }

  if (status == null && fleetNumber == null) {
    return res.status(400).json({ message: "Nenhum campo para atualizar." });
  }

  try {
    const result = await db.query(
      `UPDATE services
       SET status = COALESCE($2, status),
           fleet_number = COALESCE($3, fleet_number)
       WHERE id = $1
       RETURNING id, driver_id, plate_number, service_schedule, line_code, fleet_number, status, started_at, ended_at, total_km, route_deviation_m, is_off_route`,
      [serviceId, status || null, parsedFleetNumber]
    );
    if (!result.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    return res.json(result.rows[0]);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao ajustar servico." });
  }
});

router.get("/services/:serviceId/stop-passages", async (req, res) => {
  const serviceId = Number(req.params.serviceId);
  const radiusMeters = Math.min(Math.max(Number(req.query.radiusM) || 85, 40), 200);
  if (!Number.isFinite(serviceId) || serviceId <= 0) {
    return res.status(400).json({ message: "Identificador de servico invalido." });
  }

  try {
    const svcRes = await db.query(
      `SELECT s.id, s.driver_id, s.gtfs_trip_id, s.line_code, s.service_schedule,
              s.plate_number, s.fleet_number, s.status, s.started_at, s.ended_at,
              u.name AS driver_name
       FROM services s
       JOIN users u ON u.id = s.driver_id
       WHERE s.id = $1
       LIMIT 1`,
      [serviceId]
    );
    if (!svcRes.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    const svc = svcRes.rows[0];
    let tripId = svc.gtfs_trip_id;
    if (!tripId) {
      const best = await findBestTripForLine(svc.line_code, svc.service_schedule);
      tripId = best?.trip_id || null;
    }
    if (!tripId) {
      return res.status(404).json({
        message: "Nao foi possivel determinar o trip GTFS deste servico (linha/horario).",
        service: {
          id: svc.id,
          line_code: svc.line_code,
          service_schedule: svc.service_schedule,
        },
      });
    }

    const stops = await getStopsByTripId(tripId);
    const pointsRes = await db.query(
      `SELECT lat, lng, captured_at
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at ASC`,
      [serviceId]
    );
    const points = pointsRes.rows;

    if (!stops.length) {
      return res.json({
        service: {
          id: svc.id,
          driver_name: svc.driver_name,
          line_code: svc.line_code,
          service_schedule: svc.service_schedule,
          plate_number: svc.plate_number,
          fleet_number: svc.fleet_number,
          gtfs_trip_id: tripId,
          status: svc.status,
          started_at: svc.started_at,
          ended_at: svc.ended_at,
        },
        trip_id: tripId,
        threshold_meters: radiusMeters,
        gps_points_count: points.length,
        summary: { stops_matched: 0, stops_total: 0, pct: 0 },
        stops: [],
        note: "Sem paragens GTFS para este trip.",
      });
    }

    const { rows, matched, total } = matchGpsPointsToGtfsStops(stops, points, {
      radiusMeters,
      serviceStartedAt: svc.started_at,
    });

    return res.json({
      service: {
        id: svc.id,
        driver_name: svc.driver_name,
        line_code: svc.line_code,
        service_schedule: svc.service_schedule,
        plate_number: svc.plate_number,
        fleet_number: svc.fleet_number,
        gtfs_trip_id: tripId,
        status: svc.status,
        started_at: svc.started_at,
        ended_at: svc.ended_at,
      },
      trip_id: tripId,
      threshold_meters: radiusMeters,
      gps_points_count: points.length,
      summary: {
        stops_matched: matched,
        stops_total: total,
        pct: total ? Math.round((matched / total) * 1000) / 10 : 0,
      },
      stops: rows,
    });
  } catch (error) {
    console.error("stop-passages", error);
    return res.status(500).json({ message: "Erro ao analisar passagem pelas paragens." });
  }
});

router.get("/services/:serviceId/details", async (req, res) => {
  const { serviceId } = req.params;
  try {
    const serviceResult = await db.query(
      `SELECT
         s.id,
         s.driver_id,
         u.name AS driver_name,
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
       JOIN users u ON u.id = s.driver_id
       WHERE s.id = $1
       LIMIT 1`,
      [serviceId]
    );
    if (!serviceResult.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }

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
         h.status,
         h.created_at,
         h.completed_at,
         uf.name AS from_driver_name,
         ut.name AS to_driver_name
       FROM service_handover_events h
       JOIN users uf ON uf.id = h.from_driver_id
       LEFT JOIN users ut ON ut.id = h.to_driver_id
       WHERE h.service_id = $1
       ORDER BY h.created_at ASC`,
      [serviceId]
    );

    const pointsResult = await db.query(
      `SELECT lat, lng, captured_at, service_segment_id
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at ASC`,
      [serviceId]
    );

    return res.json({
      service: serviceResult.rows[0],
      handovers: handoversResult.rows,
      points: pointsResult.rows,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao obter detalhe do servico." });
  }
});

router.get("/services/:serviceId/reference-route", async (req, res) => {
  const serviceId = Number(req.params.serviceId);
  if (!Number.isFinite(serviceId) || serviceId <= 0) {
    return res.status(400).json({ message: "Identificador de servico invalido." });
  }
  try {
    const serviceResult = await db.query(
      `SELECT id, gtfs_trip_id, line_code, service_schedule
       FROM services
       WHERE id = $1
       LIMIT 1`,
      [serviceId]
    );
    if (!serviceResult.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }

    const service = serviceResult.rows[0];
    let tripId = service.gtfs_trip_id;
    if (!tripId) {
      const bestTrip = await findBestTripForLine(service.line_code, service.service_schedule);
      tripId = bestTrip?.trip_id || null;
      if (tripId) {
        await db.query(`UPDATE services SET gtfs_trip_id = $1 WHERE id = $2 AND gtfs_trip_id IS NULL`, [
          tripId,
          serviceId,
        ]);
      }
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
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao obter rota de referencia GTFS." });
  }
});

router.post("/services/:serviceId/force-end", async (req, res) => {
  const { serviceId } = req.params;
  try {
    const current = await db.query(
      `SELECT id, status, planned_service_id
       FROM services
       WHERE id = $1
       LIMIT 1`,
      [serviceId]
    );
    if (!current.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    if (current.rows[0].status === "completed") {
      return res.status(409).json({ message: "Servico ja finalizado." });
    }

    await closeActiveSegment(serviceId, "completed");

    const pointsResult = await db.query(
      `SELECT lat, lng
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at ASC`,
      [serviceId]
    );
    const points = pointsResult.rows.map((p) => ({
      lat: Number(p.lat),
      lng: Number(p.lng),
    }));
    const totalKmGps = calculatePathDistance(points);
    const totalKm = await resolveTotalKmWithPlannedFallback(totalKmGps, current.rows[0].planned_service_id);

    const updated = await db.query(
      `UPDATE services
       SET status = 'completed',
           ended_at = NOW(),
           total_km = $2
       WHERE id = $1
       RETURNING id, driver_id, plate_number, service_schedule, line_code, fleet_number, status, started_at, ended_at, total_km`,
      [serviceId, totalKm]
    );

    return res.json(updated.rows[0]);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao finalizar servico." });
  }
});

router.post("/services/:serviceId/cancel", async (req, res) => {
  const { serviceId } = req.params;
  try {
    const current = await db.query(
      `SELECT id, status, planned_service_id, driver_id
       FROM services
       WHERE id = $1
       LIMIT 1`,
      [serviceId]
    );
    if (!current.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    if (current.rows[0].status !== "in_progress") {
      return res.status(409).json({ message: "Apenas servicos em curso podem ser anulados." });
    }

    await closeActiveSegment(serviceId, "cancelled");

    const pointsResult = await db.query(
      `SELECT lat, lng
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at ASC`,
      [serviceId]
    );
    const points = pointsResult.rows.map((p) => ({
      lat: Number(p.lat),
      lng: Number(p.lng),
    }));
    const totalKm = calculatePathDistance(points);

    const updated = await db.query(
      `UPDATE services
       SET status = 'cancelled',
           ended_at = NOW(),
           total_km = $2
       WHERE id = $1
       RETURNING id, driver_id, plate_number, service_schedule, line_code, fleet_number, status, started_at, ended_at, total_km`,
      [serviceId, totalKm]
    );

    if (current.rows[0]?.planned_service_id) {
      await db.query(
        `UPDATE daily_roster
         SET status = 'pending'
         WHERE driver_id = $1
           AND planned_service_id = $2
           AND service_date = CURRENT_DATE`,
        [current.rows[0].driver_id, current.rows[0].planned_service_id]
      );
    }

    return res.json(updated.rows[0]);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao anular servico." });
  }
});

router.post("/services/:serviceId/handover", async (req, res) => {
  const { serviceId } = req.params;
  const { toDriverId, toFleetNumber, reason, notes, handoverLocationText } = req.body || {};
  const toDriverIdNum = Number(toDriverId);

  if (!Number.isFinite(toDriverIdNum) || toDriverIdNum <= 0) {
    return res.status(400).json({ message: "Indique o motorista destino." });
  }
  if (!String(reason || "").trim()) {
    return res.status(400).json({ message: "Indique o motivo da transferencia." });
  }

  try {
    const serviceResult = await db.query(
      `SELECT id, driver_id, fleet_number, status, line_code, service_schedule
       FROM services
       WHERE id = $1
       LIMIT 1`,
      [serviceId]
    );
    if (!serviceResult.rowCount) {
      return res.status(404).json({ message: "Servico nao encontrado." });
    }
    const service = serviceResult.rows[0];
    if (service.status !== "in_progress") {
      return res.status(409).json({ message: "Servico nao esta em curso." });
    }

    const toDriverResult = await db.query(
      `SELECT id, name
       FROM users
       WHERE id = $1
         AND role = 'driver'
         AND is_active = TRUE
       LIMIT 1`,
      [toDriverIdNum]
    );
    if (!toDriverResult.rowCount) {
      return res.status(404).json({ message: "Motorista destino nao encontrado ou inativo." });
    }
    const toDriver = toDriverResult.rows[0];
    if (toDriver.id === service.driver_id) {
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

    const closedSegment = await closeActiveSegment(serviceId, "completed");
    const latestPointResult = await db.query(
      `SELECT lat, lng
       FROM service_points
       WHERE service_id = $1
       ORDER BY captured_at DESC
       LIMIT 1`,
      [serviceId]
    );
    const latestPoint = latestPointResult.rows[0] || {};
    const nextFleetNumber = toFleetNumber || service.fleet_number;

    const handover = await db.query(
      `INSERT INTO service_handover_events (
         service_id, from_segment_id, from_driver_id, to_driver_id,
         from_fleet_number, to_fleet_number, reason, notes,
         handover_lat, handover_lng, handover_location_text, status, completed_at
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, 'completed', NOW())
       RETURNING id, service_id, reason, status, to_driver_id, to_fleet_number, created_at, completed_at`,
      [
        serviceId,
        closedSegment?.id || null,
        service.driver_id,
        toDriver.id,
        service.fleet_number,
        nextFleetNumber,
        String(reason).trim(),
        notes || null,
        latestPoint.lat || null,
        latestPoint.lng || null,
        handoverLocationText || null,
      ]
    );

    await db.query(
      `INSERT INTO service_segments (service_id, driver_id, fleet_number, status)
       VALUES ($1, $2, $3, 'in_progress')`,
      [serviceId, toDriver.id, nextFleetNumber]
    );

    const updatedService = await db.query(
      `UPDATE services
       SET driver_id = $2,
           fleet_number = $3,
           status = 'in_progress'
       WHERE id = $1
       RETURNING id, driver_id, plate_number, service_schedule, line_code, fleet_number, status, started_at`,
      [serviceId, toDriver.id, nextFleetNumber]
    );

    return res.json({
      message: "Transferencia realizada com sucesso.",
      service: updatedService.rows[0],
      handover: handover.rows[0],
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao transferir servico." });
  }
});

router.get("/services/export.csv", async (req, res) => {
  const { driverId, lineCode, status, fromDate, toDate } = req.query;
  const where = [];
  const values = [];
  let i = 1;

  if (driverId) {
    where.push(`s.driver_id = $${i}`);
    values.push(driverId);
    i += 1;
  }
  if (lineCode) {
    where.push(`s.line_code = $${i}`);
    values.push(lineCode);
    i += 1;
  }
  if (status) {
    where.push(`s.status = $${i}`);
    values.push(status);
    i += 1;
  }
  const dayFilter = serviceActivityLisbonDayFilter(fromDate, toDate, i);
  if (dayFilter) {
    where.push(dayFilter.sql);
    dayFilter.values.forEach((v) => values.push(v));
    i = dayFilter.nextIndex;
  }

  where.push(`(s.planned_service_id IS NULL OR COALESCE(ps.kms_carga, 0) > 0)`);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  try {
    const result = await db.query(
      `SELECT
         s.id,
         u.name AS driver_name,
         s.plate_number,
         s.service_schedule,
         s.line_code,
         s.fleet_number,
         s.status,
         s.gtfs_trip_id,
         s.started_at,
         s.ended_at,
         s.total_km
       FROM services s
       JOIN users u ON u.id = s.driver_id
       LEFT JOIN planned_services ps ON ps.id = s.planned_service_id
       ${whereSql}
       ORDER BY s.started_at DESC`,
      values
    );
    const rowsWithMetrics = await enrichServiceRowsForExport(result.rows);

    const header = [
      "service_id",
      "motorista",
      "chapa",
      "dia_execucao",
      "horario",
      "linha",
      "frota",
      "estado",
      "inicio",
      "fim",
      "kms",
      "kms_programados_linha",
      "kms_realizados_servico",
      "handover_existe",
      "motorista_continuacao",
      "kms_motorista_inicial",
      "kms_motorista_continuacao",
      "kms_soma_handover",
      "delta_previsto_vs_soma_handover",
      "delta_realizado_vs_soma_handover",
    ];

    const rows = rowsWithMetrics.map((r) =>
      [
        r.id,
        r.driver_name,
        r.plate_number,
        r.execution_day,
        r.service_schedule,
        r.line_code,
        r.fleet_number,
        r.status,
        formatLisbonTimeOnly(r.started_at),
        formatLisbonTimeOnly(r.ended_at),
        r.total_km,
        r.planned_km_line == null ? "" : Number(r.planned_km_line).toFixed(3),
        r.realized_km_service == null ? "" : Number(r.realized_km_service).toFixed(3),
        r.had_handover ? "sim" : "nao",
        r.continuation_driver_name || "",
        Number(r.km_initial_driver || 0).toFixed(3),
        Number(r.km_continuation_driver || 0).toFixed(3),
        Number(r.km_handover_sum || 0).toFixed(3),
        (Number(r.planned_km_line || 0) - Number(r.km_handover_sum || 0)).toFixed(3),
        (Number(r.realized_km_service || 0) - Number(r.km_handover_sum || 0)).toFixed(3),
      ]
        .map(csvEscape)
        .join(",")
    );

    const csv = [header.map(csvEscape).join(","), ...rows].join("\n");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=servicos.csv");
    return res.status(200).send(csv);
  } catch (error) {
    return res.status(500).json({ message: "Erro ao exportar CSV." });
  }
});

router.get("/services/export.xlsx", async (req, res) => {
  const { driverId, lineCode, status, fromDate, toDate } = req.query;
  const where = [];
  const values = [];
  let i = 1;

  if (driverId) {
    where.push(`s.driver_id = $${i}`);
    values.push(driverId);
    i += 1;
  }
  if (lineCode) {
    where.push(`s.line_code = $${i}`);
    values.push(lineCode);
    i += 1;
  }
  if (status) {
    where.push(`s.status = $${i}`);
    values.push(status);
    i += 1;
  }
  const dayFilter = serviceActivityLisbonDayFilter(fromDate, toDate, i);
  if (dayFilter) {
    where.push(dayFilter.sql);
    dayFilter.values.forEach((v) => values.push(v));
  }
  where.push(`(s.planned_service_id IS NULL OR COALESCE(ps.kms_carga, 0) > 0)`);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  try {
    const result = await db.query(
      `SELECT
         s.id,
         u.name AS driver_name,
         s.plate_number,
         s.service_schedule,
         s.line_code,
         s.fleet_number,
         s.status,
         s.gtfs_trip_id,
         s.started_at,
         s.ended_at,
         s.total_km
       FROM services s
       JOIN users u ON u.id = s.driver_id
       LEFT JOIN planned_services ps ON ps.id = s.planned_service_id
       ${whereSql}
       ORDER BY s.started_at DESC`,
      values
    );
    const rowsWithMetrics = await enrichServiceRowsForExport(result.rows);

    const servicesRows = rowsWithMetrics.map((r) => ({
      service_id: r.id,
      motorista: r.driver_name || "",
      chapa: r.plate_number || "",
      dia_execucao: r.execution_day || "",
      horario: r.service_schedule || "",
      linha: r.line_code || "",
      frota: r.fleet_number || "",
      estado: r.status || "",
      inicio: formatLisbonTimeOnly(r.started_at),
      fim: formatLisbonTimeOnly(r.ended_at),
      kms: r.total_km ?? "",
      kms_programados_linha: r.planned_km_line == null ? "" : Number(r.planned_km_line).toFixed(3),
      kms_realizados_servico: r.realized_km_service == null ? "" : Number(r.realized_km_service).toFixed(3),
      handover_existe: r.had_handover ? "sim" : "nao",
      motorista_continuacao: r.continuation_driver_name || "",
      kms_motorista_inicial: Number(r.km_initial_driver || 0).toFixed(3),
      kms_motorista_continuacao: Number(r.km_continuation_driver || 0).toFixed(3),
      kms_soma_handover: Number(r.km_handover_sum || 0).toFixed(3),
      delta_previsto_vs_soma_handover: (Number(r.planned_km_line || 0) - Number(r.km_handover_sum || 0)).toFixed(3),
      delta_realizado_vs_soma_handover: (Number(r.realized_km_service || 0) - Number(r.km_handover_sum || 0)).toFixed(3),
    }));

    const dailyMap = new Map();
    for (const row of rowsWithMetrics) {
      const key = row.execution_day || "sem_data";
      if (!dailyMap.has(key)) {
        dailyMap.set(key, {
          dia_execucao: key,
          total_servicos: 0,
          kms_programados_total: 0,
          kms_realizados_total: 0,
        });
      }
      const acc = dailyMap.get(key);
      acc.total_servicos += 1;
      acc.kms_programados_total += Number(row.planned_km_line || 0);
      acc.kms_realizados_total += Number(row.realized_km_service || 0);
    }

    const summaryRows = [...dailyMap.values()]
      .sort((a, b) => String(a.dia_execucao).localeCompare(String(b.dia_execucao)))
      .map((r) => ({
        ...r,
        kms_programados_total: Number(r.kms_programados_total).toFixed(3),
        kms_realizados_total: Number(r.kms_realizados_total).toFixed(3),
      }));

    const totalServices = rowsWithMetrics.length;
    const totalPlanned = rowsWithMetrics.reduce((sum, r) => sum + Number(r.planned_km_line || 0), 0);
    const totalRealized = rowsWithMetrics.reduce((sum, r) => sum + Number(r.realized_km_service || 0), 0);
    summaryRows.push({
      dia_execucao: "TOTAL",
      total_servicos: totalServices,
      kms_programados_total: Number(totalPlanned).toFixed(3),
      kms_realizados_total: Number(totalRealized).toFixed(3),
    });

    const workbook = XLSX.utils.book_new();
    const servicesSheet = XLSX.utils.json_to_sheet(servicesRows);
    const summarySheet = XLSX.utils.json_to_sheet(summaryRows);
    XLSX.utils.book_append_sheet(workbook, servicesSheet, "servicos");
    XLSX.utils.book_append_sheet(workbook, summarySheet, "resumo_dia_total");
    const buffer = XLSX.write(workbook, { type: "buffer", bookType: "xlsx" });

    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader("Content-Disposition", "attachment; filename=servicos.xlsx");
    return res.status(200).send(buffer);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar Excel de serviços." });
  }
});

module.exports = router;
