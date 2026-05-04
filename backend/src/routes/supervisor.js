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

let serviceCloseModeColumnExistsCache = null;
async function hasServiceCloseModeColumn() {
  if (serviceCloseModeColumnExistsCache != null) return serviceCloseModeColumnExistsCache;
  try {
    const result = await db.query(
      `SELECT 1
       FROM information_schema.columns
       WHERE table_name = 'services'
         AND column_name = 'close_mode'
       LIMIT 1`
    );
    serviceCloseModeColumnExistsCache = result.rowCount > 0;
  } catch (_error) {
    serviceCloseModeColumnExistsCache = false;
  }
  return serviceCloseModeColumnExistsCache;
}

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

async function ensureUsersEmailNullable() {
  await db.query(`ALTER TABLE users ALTER COLUMN email DROP NOT NULL`);
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

function lisbonDayKey(dateLike) {
  const d = new Date(dateLike);
  if (Number.isNaN(d.getTime())) return null;
  return new Intl.DateTimeFormat("en-CA", {
    timeZone: "Europe/Lisbon",
    year: "numeric",
    month: "2-digit",
    day: "2-digit",
  }).format(d);
}

function lisbonMinutesOfDay(dateLike) {
  const d = new Date(dateLike);
  if (Number.isNaN(d.getTime())) return null;
  const parts = new Intl.DateTimeFormat("en-GB", {
    timeZone: "Europe/Lisbon",
    hour: "2-digit",
    minute: "2-digit",
    hour12: false,
  }).formatToParts(d);
  const hh = Number(parts.find((p) => p.type === "hour")?.value || 0);
  const mm = Number(parts.find((p) => p.type === "minute")?.value || 0);
  if (!Number.isFinite(hh) || !Number.isFinite(mm)) return null;
  return hh * 60 + mm;
}

function haversineDistanceKm(lat1, lng1, lat2, lng2) {
  const aLat = Number(lat1);
  const aLng = Number(lng1);
  const bLat = Number(lat2);
  const bLng = Number(lng2);
  if (![aLat, aLng, bLat, bLng].every(Number.isFinite)) return null;
  const toRad = (deg) => (deg * Math.PI) / 180;
  const earthRadiusKm = 6371;
  const dLat = toRad(bLat - aLat);
  const dLng = toRad(bLng - aLng);
  const h =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRad(aLat)) * Math.cos(toRad(bLat)) * Math.sin(dLng / 2) ** 2;
  return 2 * earthRadiusKm * Math.asin(Math.sqrt(h));
}

function parseGtfsTimeToRelativeMinutes(rawTime) {
  const match = String(rawTime || "")
    .trim()
    .match(/^(\d{1,2}):(\d{2})(?::(\d{2}))?$/);
  if (!match) return null;
  const hours = Number(match[1]);
  const minutes = Number(match[2]);
  if (!Number.isFinite(hours) || !Number.isFinite(minutes)) return null;
  return hours * 60 + minutes;
}

function parseServiceScheduleRange(rawSchedule) {
  const text = String(rawSchedule || "").trim();
  const match = text.match(/^(\d{1,2}):(\d{2})\s*-\s*(\d{1,2}):(\d{2})$/);
  if (!match) return null;
  const start = Number(match[1]) * 60 + Number(match[2]);
  const end = Number(match[3]) * 60 + Number(match[4]);
  if (!Number.isFinite(start) || !Number.isFinite(end)) return null;
  if (end <= start) return null;
  return { start, end };
}

function formatDateIso(dateObj) {
  if (!(dateObj instanceof Date) || Number.isNaN(dateObj.getTime())) return null;
  const y = dateObj.getUTCFullYear();
  const m = String(dateObj.getUTCMonth() + 1).padStart(2, "0");
  const d = String(dateObj.getUTCDate()).padStart(2, "0");
  return `${y}-${m}-${d}`;
}

function enumerateDates(fromIso, toIso, hardLimit = 62) {
  const from = parseDateOnly(fromIso);
  const to = parseDateOnly(toIso);
  if (!from || !to) return [];
  const start = new Date(`${from}T00:00:00Z`);
  const end = new Date(`${to}T00:00:00Z`);
  if (Number.isNaN(start.getTime()) || Number.isNaN(end.getTime()) || end < start) return [];
  const out = [];
  for (let d = new Date(start); d <= end; d.setUTCDate(d.getUTCDate() + 1)) {
    out.push(formatDateIso(d));
    if (out.length >= hardLimit) break;
  }
  return out;
}

function normalizePlanMode(rawMode) {
  const mode = String(rawMode || "").trim().toLowerCase();
  if (["conservative", "balanced", "aggressive"].includes(mode)) return mode;
  return "balanced";
}

function resolveModeWeights(mode) {
  if (mode === "conservative") {
    return { waitWeight: 0.04, deadheadWeight: 2.0, lineChangePenalty: 18 };
  }
  if (mode === "aggressive") {
    return { waitWeight: 0.015, deadheadWeight: 1.0, lineChangePenalty: 4 };
  }
  return { waitWeight: 0.025, deadheadWeight: 1.4, lineChangePenalty: 10 };
}

function parseServiceWindowFromGtfs(row) {
  const startMin = parseGtfsTimeToRelativeMinutes(row.first_departure_time);
  const endMin = parseGtfsTimeToRelativeMinutes(row.last_departure_time);
  if (!Number.isFinite(startMin) || !Number.isFinite(endMin)) return null;
  const normalizedEnd = endMin <= startMin ? endMin + 24 * 60 : endMin;
  return {
    start_min: startMin,
    end_min: normalizedEnd,
    drive_min: Math.max(1, normalizedEnd - startMin),
  };
}

function estimateCompatCost(prevService, nextService, policy) {
  const waitMin = Math.max(0, Number(nextService.start_min) - Number(prevService.end_min));
  const deadheadKm = haversineDistanceKm(prevService.end_lat, prevService.end_lng, nextService.start_lat, nextService.start_lng) || 0;
  const lineChangePenalty = String(prevService.line_code || "") === String(nextService.line_code || "") ? 0 : policy.lineChangePenalty;
  const score = waitMin * policy.waitWeight + deadheadKm * policy.deadheadWeight + lineChangePenalty;
  return { score, wait_min: waitMin, deadhead_km: deadheadKm };
}

function computeDepotVehicleDeadheadSummary(depot, vehicle) {
  if (!vehicle?.services?.length || !depot) return null;
  const first = vehicle.services[0];
  const last = vehicle.services[vehicle.services.length - 1];
  const toFirst = haversineDistanceKm(depot.lat, depot.lng, first.start_lat, first.start_lng) || 0;
  const fromLast = haversineDistanceKm(last.end_lat, last.end_lng, depot.lat, depot.lng) || 0;
  const total = toFirst + fromLast;
  return {
    depot_id: depot.id,
    depot_name: depot.depot_name,
    deadhead_to_first_km: Number(toFirst.toFixed(3)),
    deadhead_back_to_depot_km: Number(fromLast.toFixed(3)),
    total_deadhead_km: Number(total.toFixed(3)),
  };
}

function chooseBestDepotForVehicle(vehicle, depots) {
  if (!Array.isArray(depots) || !depots.length || !vehicle?.services?.length) return null;
  let best = null;
  for (const depot of depots) {
    const row = computeDepotVehicleDeadheadSummary(depot, vehicle);
    if (!row) continue;
    if (!best || row.total_deadhead_km < best.total_deadhead_km) best = row;
  }
  return best || null;
}

function resolveForcedBaseDepot(depotsRows, opts) {
  if (!Array.isArray(depotsRows) || !depotsRows.length) return null;
  const depotIdRaw = Number(opts?.baseDepotId);
  if (Number.isFinite(depotIdRaw) && depotIdRaw > 0) {
    const row = depotsRows.find((d) => Number(d.id) === depotIdRaw);
    if (row) return row;
  }
  const nameHint = String(opts?.baseDepotName || "").trim().toLowerCase();
  if (nameHint) {
    const hit = depotsRows.find((d) => String(d.depot_name || "").toLowerCase().includes(nameHint));
    if (hit) return hit;
  }
  return null;
}

function normalizeLineCodesInput(raw) {
  if (raw == null || raw === "") return [];
  const chunks = Array.isArray(raw) ? raw : [raw];
  const out = new Set();
  chunks.forEach((item) => {
    String(item)
      .split(/[,;\n]/)
      .map((token) => String(token || "").trim())
      .filter(Boolean)
      .forEach((token) => out.add(token.toLowerCase()));
  });
  return [...out];
}

function canonicalNumericLineCodeToken(raw) {
  const token = String(raw || "")
    .trim()
    .replace(/\s+/g, "");
  if (!token || !/^\d+$/.test(token)) return null;
  const normalized = token.replace(/^0+/, "");
  return normalized || "0";
}

function parseGtfsChapasPlanningParams(queryLike = {}) {
  const mode = normalizePlanMode(queryLike.mode);
  const minTurnaroundMin = Number.isFinite(Number(queryLike.minTurnaroundMin))
    ? Math.min(Math.max(Math.round(Number(queryLike.minTurnaroundMin)), 3), 120)
    : 10;
  const maxVehiclesRaw = Number(queryLike.maxVehicles);
  const maxVehicles = Number.isFinite(maxVehiclesRaw) && maxVehiclesRaw > 0 ? Math.floor(maxVehiclesRaw) : null;
  const fleetCapFromTrackers = parseBooleanLike(queryLike.fleetCapFromTrackers, false);
  const planningClassFilter = String(queryLike.planningClass || "").trim();
  let maxDriveBlockMin = Number(queryLike.maxDriveBlockMin);
  if (!Number.isFinite(maxDriveBlockMin)) maxDriveBlockMin = 270;
  maxDriveBlockMin = Math.min(Math.max(Math.round(maxDriveBlockMin), 0), 900);
  let minRestBetweenBlocksMin = Number(queryLike.minRestBetweenBlocksMin);
  if (!Number.isFinite(minRestBetweenBlocksMin)) minRestBetweenBlocksMin = 45;
  minRestBetweenBlocksMin = Math.min(Math.max(Math.round(minRestBetweenBlocksMin), 0), 240);
  /** Pausa obrigatória ininterrupta (Reg. UE 561/2006 tipo: 45 min após 4h30 condução). */
  let minUninterruptedBreakMin = Number(
    queryLike.minUninterruptedBreakMin ?? queryLike.mandatoryBreakMin ?? queryLike.minMandatoryBreakMin
  );
  if (!Number.isFinite(minUninterruptedBreakMin)) minUninterruptedBreakMin = minRestBetweenBlocksMin;
  minUninterruptedBreakMin = Math.min(Math.max(Math.round(minUninterruptedBreakMin), 0), 240);
  const euSplitBreak = parseBooleanLike(queryLike.euSplitBreak, false);
  let splitBreakFirstSegmentMin = Number(queryLike.splitBreakFirstSegmentMin);
  if (!Number.isFinite(splitBreakFirstSegmentMin)) splitBreakFirstSegmentMin = 15;
  splitBreakFirstSegmentMin = Math.min(Math.max(Math.round(splitBreakFirstSegmentMin), 1), 120);
  let splitBreakSecondSegmentMin = Number(queryLike.splitBreakSecondSegmentMin);
  if (!Number.isFinite(splitBreakSecondSegmentMin)) splitBreakSecondSegmentMin = 30;
  splitBreakSecondSegmentMin = Math.min(Math.max(Math.round(splitBreakSecondSegmentMin), 1), 120);
  /** Zera contador de bloco após intervalo longo entre serviços sem obrigação UE pendente (0 = desligado). Útil para pernoita ou folga. */
  let operativeIdleResetMin = Number(queryLike.operativeIdleResetMin ?? queryLike.overnightIdleResetMin);
  if (!Number.isFinite(operativeIdleResetMin)) operativeIdleResetMin = 0;
  operativeIdleResetMin = Math.min(Math.max(Math.round(operativeIdleResetMin), 0), 24 * 60);
  /** Parque físico obrigatório para vazio inicial/final (fase sem Teltonika / base única). */
  const baseDepotId = Number(queryLike.baseDepotId);
  const baseDepotName = String(queryLike.baseDepotName || "").trim();
  const useBaseDepotCapacityAsCap = parseBooleanLike(queryLike.useBaseDepotCapacityAsCap, false);
  /** Chave do feed GTFS (ex.: urbano_leiria). Vazio = automático: último feed activo por data de actualização. */
  const feedKey = String(queryLike.feedKey ?? queryLike.feed_key ?? "").trim();
  /** Após o plano normal, cria viaturas extra (uma por serviço) para tudo o que ficou por cupo/regras — cobertura total de trips do dia. */
  const assignAllServices = parseBooleanLike(queryLike.assignAllServices, false);
  const lineCodesNormalized = normalizeLineCodesInput(queryLike.lineCodes ?? queryLike.lines ?? queryLike.line);
  return {
    mode,
    minTurnaroundMin,
    fleetCapFromTrackers,
    planningClassFilter,
    maxVehicles,
    maxDriveBlockMin,
    minRestBetweenBlocksMin,
    minUninterruptedBreakMin,
    euSplitBreak,
    splitBreakFirstSegmentMin,
    splitBreakSecondSegmentMin,
    operativeIdleResetMin,
    baseDepotId: Number.isFinite(baseDepotId) && baseDepotId > 0 ? Math.floor(baseDepotId) : null,
    baseDepotName,
    useBaseDepotCapacityAsCap,
    assignAllServices,
    feedKey: feedKey || null,
    lineCodesNormalized,
  };
}

async function resolveFleetVehicleCap(planningOpts) {
  await ensureTrackerTables();
  let cap = planningOpts?.maxVehicles != null ? planningOpts.maxVehicles : null;
  if (planningOpts?.fleetCapFromTrackers) {
    const cls = String(planningOpts.planningClassFilter || "").trim();
    const counted = await db.query(
      `SELECT COUNT(*)::int AS c
       FROM tracker_devices
       WHERE is_active = TRUE
         AND (
           TRIM(COALESCE($1::text, '')) = ''
           OR LOWER(TRIM(COALESCE(planning_class, ''))) = LOWER(TRIM($1::text)))
       `,
      [cls]
    );
    const trackersCount = counted.rows[0]?.c ?? 0;
    /** Sem rastreadores activos: não forçar cupo 0 (bloqueava qualquer chapa). Mantém-se só maxVehicles, se houver. */
    if (trackersCount <= 0) {
      return cap;
    }
    cap = cap == null ? trackersCount : Math.min(cap, trackersCount);
  }
  return cap;
}

/** Rótulo legível das opções UE (informação apenas). */
function plannerRestPolicyDescription(opts) {
  if (!opts?.euSplitBreak) {
    return `Pausa única (${opts?.minUninterruptedBreakMin || 45} min) após atingir limite UE de condução continua (${opts?.maxDriveBlockMin || 270} min)`;
  }
  const a = opts.splitBreakFirstSegmentMin || 15;
  const b = opts.splitBreakSecondSegmentMin || 30;
  const u = opts.minUninterruptedBreakMin ?? opts.minRestBetweenBlocksMin ?? 45;
  return `Limite UE ${opts?.maxDriveBlockMin || 270} min; depois: ${u} min ininterruptos OU ${a} min num intervalo entre serviços + ${b} min noutro intervalo entre serviços`;
}

/** Estado interno planeamento viatura (motorista UE simplificado nos intervalos entre serviços). */
function clonePlannerDutyState(src) {
  return {
    drive_acc_since_qualifying_rest_min: Number(src.drive_acc_since_qualifying_rest_min || 0),
    mandatory_break_pending: !!src.mandatory_break_pending,
    eu_split_waiting_second_pause: !!src.eu_split_waiting_second_pause,
  };
}

function plannerOperationalCarryIdleReset(sim, idleGapMinutes, plannerOpts) {
  const idle = Math.max(0, Number(idleGapMinutes) || 0);
  /** Política operacional opcional: intervalos longos (ex. pernoita) zeram bloco mesmo sem modelo UE estrito — desactivável com operativeIdleResetMin=0. */
  const resetThr = plannerOpts.operativeIdleResetMin;
  if (!Number.isFinite(resetThr) || resetThr <= 0 || idle < resetThr) return;
  if (sim.mandatory_break_pending) return;
  sim.drive_acc_since_qualifying_rest_min = 0;
  sim.eu_split_waiting_second_pause = false;
}

/**
 * Consumo de intervalos entre dois serviços consecutivos pela mesma viatura.
 * Só conta como descanso o tempo disponível desde o `fim` do anterior até à primeira partida seguinte no GTFS chapeado.
 */
function plannerConsumeDutyIdleGap(sim, idleGapMinutes, plannerOpts) {
  plannerOperationalCarryIdleReset(sim, idleGapMinutes, plannerOpts);
  const idle = Math.max(0, Number(idleGapMinutes) || 0);
  const maxDrive = plannerOpts.maxDriveBlockMin;
  const minUninterrupted = plannerOpts.minUninterruptedBreakMin ?? plannerOpts.minRestBetweenBlocksMin ?? 45;
  /** Obrigatoriedade forte: após exceder o limite de condução. */
  if (!sim.mandatory_break_pending || maxDrive <= 0 || idle <= 0) return;
  if (!plannerOpts.euSplitBreak) {
    if (idle >= minUninterrupted) {
      sim.drive_acc_since_qualifying_rest_min = 0;
      sim.mandatory_break_pending = false;
      sim.eu_split_waiting_second_pause = false;
    }
    return;
  }
  const firstSeg = plannerOpts.splitBreakFirstSegmentMin || 15;
  const secondSeg = plannerOpts.splitBreakSecondSegmentMin || 30;
  /** Uma só janela pode ser suficiente (ex.: 45 contíguos dispensam parcelas 15/30 seguidas nos mesmos 45…). Preferência explícito: sempre que idle >= uninterrupted, fecha. */
  if (idle >= minUninterrupted) {
    sim.drive_acc_since_qualifying_rest_min = 0;
    sim.mandatory_break_pending = false;
    sim.eu_split_waiting_second_pause = false;
    return;
  }
  if (!sim.eu_split_waiting_second_pause) {
    if (idle >= firstSeg) {
      sim.eu_split_waiting_second_pause = true;
      return;
    }
    return;
  }
  /** Aguardamos segundo intervenção de segunda pausa (nova janela entre serviços). */
  if (idle >= secondSeg) {
    sim.drive_acc_since_qualifying_rest_min = 0;
    sim.mandatory_break_pending = false;
    sim.eu_split_waiting_second_pause = false;
  }
}

/** Após cada servício conduzido, atualiza obrigações. */
function plannerRegisterTripDrive(sim, tripDriveMinutes, plannerOpts) {
  const dm = Math.max(0, Number(tripDriveMinutes) || 0);
  sim.drive_acc_since_qualifying_rest_min = Number(sim.drive_acc_since_qualifying_rest_min || 0) + dm;
  const maxDrive = plannerOpts.maxDriveBlockMin;
  if (maxDrive > 0 && sim.drive_acc_since_qualifying_rest_min >= maxDrive) {
    sim.mandatory_break_pending = true;
    sim.eu_split_waiting_second_pause = false;
  }
}

function gtfsPlannerDriverRulesAllow(vehicle, svc, plannerOpts) {
  const prev = vehicle.services[vehicle.services.length - 1];
  const maxBlock = plannerOpts.maxDriveBlockMin;
  const svcDrive = Number(svc.drive_min || 0);
  if (!prev) {
    if (maxBlock > 0 && svcDrive > maxBlock) return false;
    return true;
  }
  if (Number(prev.end_min) + plannerOpts.minTurnaroundMin > Number(svc.start_min)) return false;
  const idleGap = Number(svc.start_min) - Number(prev.end_min);
  const sim = clonePlannerDutyState({
    drive_acc_since_qualifying_rest_min: Number(vehicle.drive_acc_since_qualifying_rest_min || 0),
    mandatory_break_pending: vehicle.mandatory_break_pending,
    eu_split_waiting_second_pause: vehicle.eu_split_waiting_second_pause,
  });
  plannerConsumeDutyIdleGap(sim, idleGap, plannerOpts);
  /** Condução do próximo bloco só permitida quando não ficou pendência UE. */
  if (sim.mandatory_break_pending) return false;
  /** Após ciclo válido não pode já abrir próximo ciclo dentro do próprio GTFS-trip (sem intervalo). */
  if (maxBlock > 0 && Number(sim.drive_acc_since_qualifying_rest_min || 0) + svcDrive > maxBlock) {
    return false;
  }
  return true;
}

function gtfsPlannerUpdateDutyStateWithTrip(vehicle, svc, plannerOpts) {
  const prev = vehicle.services[vehicle.services.length - 1];
  vehicle.drive_acc_since_qualifying_rest_min = Number(vehicle.drive_acc_since_qualifying_rest_min || 0);
  vehicle.mandatory_break_pending = !!vehicle.mandatory_break_pending;
  vehicle.eu_split_waiting_second_pause = !!vehicle.eu_split_waiting_second_pause;
  if (!prev) {
    plannerRegisterTripDrive(vehicle, svc.drive_min, plannerOpts);
    return;
  }
  const idleGap = Number(svc.start_min) - Number(prev.end_min);
  plannerConsumeDutyIdleGap(vehicle, idleGap, plannerOpts);
  plannerRegisterTripDrive(vehicle, svc.drive_min, plannerOpts);
}

function gtfsPlannerUpdateDriveAccumulator(vehicle, svc, plannerOpts) {
  gtfsPlannerUpdateDutyStateWithTrip(vehicle, svc, plannerOpts);
}

function plannerInitDutyVehicleSkeleton() {
  return {
    drive_acc_since_qualifying_rest_min: 0,
    mandatory_break_pending: false,
    eu_split_waiting_second_pause: false,
  };
}

function buildGtfsChapasFlatRows(plan) {
  const date = plan.date;
  const rows = [];
  (plan.vehicle_plans || []).forEach((vp) => {
    (vp.services || []).forEach((svc) => {
      rows.push({
        data: date,
        viatura_planeada: vp.vehicle_plan_id,
        trip_id: svc.trip_id,
        service_id_gtfs: svc.service_id,
        linha: svc.line_code,
        headsign: svc.trip_headsign,
        origem_paragem: svc.start_stop_name,
        destino_paragem: svc.end_stop_name,
        primeira_partida: svc.first_departure_time,
        ultima_partida: svc.last_departure_time,
        duracao_servico_min: svc.drive_min,
        espera_desde_servico_ant_min: svc.waiting_from_previous_min,
        vazio_desde_servico_ant_km: svc.deadhead_from_previous_km,
        parque_sugerido: vp.depot_name,
      });
    });
  });
  return rows;
}

function buildGtfsChapasUnassignedRows(plan) {
  const date = plan.date;
  return (plan.unassigned_services || []).map((svc) => ({
    data: date,
    trip_id: svc.trip_id,
    service_id_gtfs: svc.service_id,
    linha: svc.line_code,
    primeira_partida: svc.first_departure_time,
    ultima_partida: svc.last_departure_time,
    motivo: svc.unassigned_reason || "sem_slot_compativel",
    detalhe: svc.unassigned_detail || "",
  }));
}

async function respondGtfsChapasDailyXlsx(res, planningOpts, serviceDate, plan) {
  const wb = XLSX.utils.book_new();
  const bd = plan.base_depot || null;
  const summarySheet = XLSX.utils.json_to_sheet([
    {
      data: plan.date,
      modo: plan.mode,
      turnaround_minimo: plan.min_turnaround_min,
      limite_bloco_conducao_min: planningOpts.maxDriveBlockMin,
      pausa_ue_ininterrupta_min: planningOpts.minUninterruptedBreakMin,
      pausa_minima_entre_blocos_min: planningOpts.minRestBetweenBlocksMin,
      pausa_ue_frac_15_30: planningOpts.euSplitBreak ? "sim" : "nao",
      reset_contador_intervalo_oper_min: planningOpts.operativeIdleResetMin || 0,
      linhas_filtradas: (plan.line_codes_filter || []).join(", ") || "(todas)",
      parque_base_id: bd?.id ?? "",
      parque_base_nome: bd?.depot_name ?? "",
      parque_capacidade_registada: bd?.capacity_total ?? "",
      capacidade_parque_aplicada_ao_cupo: plan.depot_capacity_used_as_fleet_cap ? "sim" : "nao",
      primeira_partida_dia_preview: plan.day_schedule_anchor?.first_services_preview?.[0]?.first_departure_time ?? "",
      trip_primeira_do_dia: plan.day_schedule_anchor?.first_services_preview?.[0]?.trip_id ?? "",
      feed_gtfs_chave: plan.feed_key_used ?? "",
      feed_gtfs_nome: plan.feed_name_used ?? "",
      feed_gtfs_seleccao_automatica: plan.feed_auto_selected ? "sim" : "nao",
      cupo_max_viaturas: plan.fleet_cap_applied ?? "",
      ...(plan.summary || {}),
    },
  ]);
  XLSX.utils.book_append_sheet(wb, summarySheet, "Resumo");
  const chapasSheet = XLSX.utils.json_to_sheet(buildGtfsChapasFlatRows(plan));
  XLSX.utils.book_append_sheet(wb, chapasSheet, "Chapas");
  const unmatched = buildGtfsChapasUnassignedRows(plan);
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(unmatched.length ? unmatched : [{ info: "Sem serviços por atribuir." }]),
    "Por_atribuir"
  );
  const buffer = XLSX.write(wb, { bookType: "xlsx", type: "buffer" });
  const safeDate = String(plan.date || serviceDate || "dia").replace(/[^\d-]/g, "");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="chapas-gtfs-${safeDate}-${plan.mode}.xlsx"`
  );
  res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  return res.send(buffer);
}

async function respondGtfsChapasRangeXlsx(res, planningOpts, fromDate, toDate, bundle) {
  const wb = XLSX.utils.book_new();
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet([
      {
        periodo_ini: bundle.from_date,
        periodo_fim: bundle.to_date,
        modo: bundle.mode,
        turnaround_minimo: bundle.min_turnaround_min,
        limite_bloco_conducao_min: planningOpts.maxDriveBlockMin,
        pausa_ue_ininterrupta_min: planningOpts.minUninterruptedBreakMin,
        pausa_minima_entre_blocos_min: planningOpts.minRestBetweenBlocksMin,
        pausa_ue_frac_15_30: planningOpts.euSplitBreak ? "sim" : "nao",
        reset_contador_intervalo_oper_min: planningOpts.operativeIdleResetMin || 0,
        linhas_filtradas: (planningOpts.lineCodesNormalized || []).join(", ") || "(todas)",
        parque_base_id_pedido: planningOpts.baseDepotId ?? "",
        parque_base_nome_pedido: planningOpts.baseDepotName || "",
        capacidade_parque_aplicada_ao_cupo: planningOpts.useBaseDepotCapacityAsCap ? "sim" : "nao",
        parque_base_resolvido_nome:
          bundle.detailed_daily_plans?.[0]?.base_depot?.depot_name ?? "",
        parque_base_resolvido_id: bundle.detailed_daily_plans?.[0]?.base_depot?.id ?? "",
        feed_gtfs_usado: bundle.detailed_daily_plans?.[0]?.feed_key_used ?? "",
        feed_gtfs_nome: bundle.detailed_daily_plans?.[0]?.feed_name_used ?? "",
        feed_gtfs_auto: bundle.detailed_daily_plans?.[0]?.feed_auto_selected ? "sim" : "nao",
        ...bundle.totals,
      },
    ]),
    "Resumo"
  );
  const diaRows =
    bundle.daily_plans?.map((entry) => ({
      data: entry.date,
      viaturas: entry.summary?.vehicles_required,
      servicos_gtfs_atribuidos: entry.summary?.services_assigned,
      servicos_por_atribuir: entry.summary?.services_unassigned,
      conducao_total_min: entry.summary?.total_drive_min,
      vazio_total_km: entry.summary?.total_deadhead_km,
    })) || [];
  XLSX.utils.book_append_sheet(wb, XLSX.utils.json_to_sheet(diaRows.length ? diaRows : [{ info: "Sem dados." }]), "Por_dia");
  const detalhe = [];
  (bundle.detailed_daily_plans || []).forEach((plan) => {
    detalhe.push(...buildGtfsChapasFlatRows(plan));
  });
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(detalhe.length ? detalhe : [{ info: "Sem chapas geradas." }]),
    "Detalhe_servicos"
  );
  const unRows = [];
  (bundle.detailed_daily_plans || []).forEach((plan) => {
    unRows.push(...buildGtfsChapasUnassignedRows(plan));
  });
  XLSX.utils.book_append_sheet(
    wb,
    XLSX.utils.json_to_sheet(unRows.length ? unRows : [{ info: "Sem filas por atribuir." }]),
    "Por_atribuir"
  );
  const buffer = XLSX.write(wb, { bookType: "xlsx", type: "buffer" });
  const safeFrom = String(fromDate || "ini").replace(/[^\d-]/g, "");
  const safeTo = String(toDate || "fim").replace(/[^\d-]/g, "");
  res.setHeader(
    "Content-Disposition",
    `attachment; filename="chapas-gtfs-${safeFrom}_${safeTo}-${bundle.mode}.xlsx"`
  );
  res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
  return res.send(buffer);
}

function hasScheduleOverlap(scheduleA, scheduleB) {
  const a = parseServiceScheduleRange(scheduleA);
  const b = parseServiceScheduleRange(scheduleB);
  if (!a || !b) return false;
  return a.start < b.end && b.start < a.end;
}

function diffDays(dayA, dayB) {
  if (!dayA || !dayB) return null;
  const d1 = new Date(`${dayA}T00:00:00Z`);
  const d2 = new Date(`${dayB}T00:00:00Z`);
  if (Number.isNaN(d1.getTime()) || Number.isNaN(d2.getTime())) return null;
  return Math.round((d1.getTime() - d2.getTime()) / 86400000);
}

function buildOperationalSuggestions(summary, criticalStops) {
  const suggestions = [];
  if ((summary.avg_delay_min || 0) >= 7) {
    suggestions.push(
      "Atraso médio elevado: rever tempos de percurso e janelas de partida para alinhar o horário planeado com o tempo real."
    );
  }
  if ((summary.delay_p90_min || 0) >= 12) {
    suggestions.push(
      "Variabilidade elevada (P90): definir margens operacionais e reforço de monitorização nos períodos de maior congestão."
    );
  }
  if ((summary.missed_stop_rate_pct || 0) >= 20) {
    suggestions.push(
      "Taxa de paragens sem passagem GPS acima do desejável: validar cobertura GPS e cumprimento de paragem operacional."
    );
  }
  if (criticalStops[0] && Number(criticalStops[0].avg_delay_min || 0) >= 10) {
    suggestions.push(
      `Paragem crítica principal (${criticalStops[0].stop_name}): avaliar intervenção local (regulação, tempo de paragem ou ajuste de percurso).`
    );
  }
  if (!suggestions.length) {
    suggestions.push(
      "Desempenho estável no período analisado. Manter configuração atual e monitorizar tendências semanais para deteção precoce de degradação."
    );
  }
  return suggestions;
}

function parseCsvText(csvText) {
  const lines = String(csvText || "")
    .split(/\r?\n/)
    .map((l) => l.trim())
    .filter(Boolean);
  if (lines.length < 2) return [];

  // Remove UTF-8 BOM if present (common when CSV is saved by Excel).
  lines[0] = lines[0].replace(/^\uFEFF/, "");
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

async function enrichServiceRowsForExportSafe(rows) {
  try {
    return await enrichServiceRowsForExport(rows);
  } catch (_error) {
    return Array.isArray(rows) ? rows : [];
  }
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

async function ensureDeadheadTablesForOverview() {
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
}

async function ensureTrackerTables() {
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
  await db.query(`ALTER TABLE tracker_devices ADD COLUMN IF NOT EXISTS planning_class VARCHAR(80)`);
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_tracker_devices_fleet
     ON tracker_devices(fleet_number)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_tracker_devices_plate
     ON tracker_devices(plate_number)`
  );
}

async function ensureDepotTables() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS depots (
      id BIGSERIAL PRIMARY KEY,
      depot_code VARCHAR(40) UNIQUE,
      depot_name VARCHAR(120) NOT NULL,
      lat DOUBLE PRECISION NOT NULL,
      lng DOUBLE PRECISION NOT NULL,
      capacity_total INT NOT NULL DEFAULT 0,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `ALTER TABLE tracker_devices
       ADD COLUMN IF NOT EXISTS depot_id BIGINT REFERENCES depots(id) ON DELETE SET NULL`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_depots_active
     ON depots(is_active, depot_name)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_tracker_devices_depot
     ON tracker_devices(depot_id)`
  );
}

async function ensureDeadheadTables() {
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
    `CREATE INDEX IF NOT EXISTS idx_deadhead_points_movement
     ON deadhead_points(movement_id, captured_at ASC)`
  );
}

function emptyOverviewPayload() {
  return {
    report_date: null,
    total_services: 0,
    completed_services: 0,
    in_progress_services: 0,
    total_km: 0,
    avg_km: 0,
    planned_roster_count: 0,
    realized_roster_slots: 0,
    not_realized_count: 0,
    deadhead_km: 0,
    total_km_with_deadhead: 0,
    estimated_planned_km_today: 0,
    km_not_realized_estimate: 0,
    degraded: true,
  };
}

router.get("/overview", async (req, res) => {
  try {
    await ensureDeadheadTablesForOverview();
    const result = await db.query(OVERVIEW_TODAY_SQL);
    return res.json(result.rows[0] || emptyOverviewPayload());
  } catch (error) {
    try {
      await ensureDeadheadTablesForOverview();
      const retry = await db.query(OVERVIEW_TODAY_SQL);
      return res.json(retry.rows[0] || emptyOverviewPayload());
    } catch (_retryError) {
      return res.json(emptyOverviewPayload());
    }
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
      alert_type VARCHAR(50) NOT NULL DEFAULT 'roster_conflict',
      roster_id INT,
      planned_service_id INT,
      affected_driver_id INT REFERENCES users(id) ON DELETE SET NULL,
      affected_planned_service_id INT,
      service_schedule VARCHAR(80),
      line_code VARCHAR(40),
      conflict_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
      unassigned_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
      notes TEXT,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `ALTER TABLE supervisor_conflict_alerts
       ADD COLUMN IF NOT EXISTS alert_type VARCHAR(50) NOT NULL DEFAULT 'roster_conflict'`
  );
  await db.query(
    `ALTER TABLE supervisor_conflict_alerts
       ADD COLUMN IF NOT EXISTS affected_driver_id INT REFERENCES users(id) ON DELETE SET NULL`
  );
  await db.query(
    `ALTER TABLE supervisor_conflict_alerts
       ADD COLUMN IF NOT EXISTS affected_planned_service_id INT`
  );
  await db.query(
    `ALTER TABLE supervisor_conflict_alerts
       ADD COLUMN IF NOT EXISTS unassigned_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_supervisor_conflict_alerts_created
     ON supervisor_conflict_alerts(created_at DESC)`
  );
}

async function ensureServiceRouteIncidentsTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS service_route_incidents (
      id BIGSERIAL PRIMARY KEY,
      service_id BIGINT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
      driver_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      line_code VARCHAR(50),
      fleet_number VARCHAR(50),
      plate_number VARCHAR(50),
      gtfs_trip_id VARCHAR(120),
      threshold_m NUMERIC(8,2) NOT NULL DEFAULT 150,
      max_deviation_m NUMERIC(10,2) NOT NULL DEFAULT 0,
      first_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      last_detected_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      resolved_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_service_route_incidents_service_open
     ON service_route_incidents(service_id, resolved_at, last_detected_at DESC)`
  );
}

function buildRouteIncidentsFilterSql(query = {}, options = {}) {
  const allowLimit = options.allowLimit !== false;
  const where = [];
  const values = [];
  let i = 1;

  const status = String(query.status || "open").trim().toLowerCase();
  if (status === "open") {
    where.push(`sri.resolved_at IS NULL`);
  } else if (status === "resolved") {
    where.push(`sri.resolved_at IS NOT NULL`);
  }

  const lineCode = String(query.lineCode || "").trim();
  if (lineCode) {
    where.push(`LOWER(COALESCE(sri.line_code, '')) LIKE LOWER($${i})`);
    values.push(`%${lineCode}%`);
    i += 1;
  }

  const fleet = String(query.fleet || "").trim();
  if (fleet) {
    where.push(
      `(LOWER(COALESCE(sri.fleet_number, '')) LIKE LOWER($${i}) OR LOWER(COALESCE(sri.plate_number, '')) LIKE LOWER($${i}))`
    );
    values.push(`%${fleet}%`);
    i += 1;
  }

  const fromDate = String(query.fromDate || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(fromDate)) {
    where.push(`(sri.first_detected_at AT TIME ZONE 'Europe/Lisbon')::date >= $${i}::date`);
    values.push(fromDate);
    i += 1;
  }

  const toDate = String(query.toDate || "").trim();
  if (/^\d{4}-\d{2}-\d{2}$/.test(toDate)) {
    where.push(`(sri.first_detected_at AT TIME ZONE 'Europe/Lisbon')::date <= $${i}::date`);
    values.push(toDate);
    i += 1;
  }

  let limitSql = "";
  if (allowLimit) {
    const limitRaw = Number(query.limit);
    const limit = Number.isFinite(limitRaw) && limitRaw > 0 ? Math.min(5000, Math.round(limitRaw)) : 200;
    limitSql = `LIMIT $${i}`;
    values.push(limit);
  }

  return {
    whereSql: where.length ? `WHERE ${where.join(" AND ")}` : "",
    values,
    limitSql,
  };
}

async function ensureOpsMessagesTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS ops_messages (
      id BIGSERIAL PRIMARY KEY,
      from_user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      to_user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
      message_text TEXT NOT NULL,
      preset_code VARCHAR(60),
      is_traffic_alert BOOLEAN NOT NULL DEFAULT FALSE,
      related_service_id BIGINT REFERENCES services(id) ON DELETE SET NULL,
      read_at TIMESTAMPTZ,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_ops_messages_to_user_created
     ON ops_messages(to_user_id, created_at DESC)`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_ops_messages_users_created
     ON ops_messages(from_user_id, to_user_id, created_at DESC)`
  );
}

const BUILTIN_SUPERVISOR_MESSAGE_PRESETS = [
  { code: "ack_received", label: "Recebido. Continue em segurança." },
  { code: "reroute", label: "Siga pelo desvio indicado pela central." },
  { code: "hold_position", label: "Aguarde instruções na posição atual." },
  { code: "priority_support", label: "Apoio em deslocação para o local." },
  { code: "normal_resume", label: "Situação normalizada. Retome o percurso." },
];

const BUILTIN_DRIVER_MESSAGE_PRESETS = [
  { code: "delay_traffic", label: "Atraso por trânsito intenso" },
  { code: "breakdown", label: "Avaria na viatura" },
  { code: "accident", label: "Acidente no percurso" },
  { code: "route_blocked", label: "Via cortada/desvio necessário" },
  { code: "request_support", label: "Preciso de apoio operacional" },
];

async function ensureOpsMessagePresetsTable() {
  await db.query(
    `CREATE TABLE IF NOT EXISTS ops_message_presets (
      id BIGSERIAL PRIMARY KEY,
      scope VARCHAR(20) NOT NULL,
      code VARCHAR(60) NOT NULL,
      label VARCHAR(200) NOT NULL,
      default_message_text TEXT,
      is_active BOOLEAN NOT NULL DEFAULT TRUE,
      created_by_user_id INT REFERENCES users(id) ON DELETE SET NULL,
      created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
      UNIQUE (scope, code)
    )`
  );
  await db.query(
    `CREATE INDEX IF NOT EXISTS idx_ops_message_presets_scope_active
     ON ops_message_presets(scope, is_active)`
  );
}

function mergeMessagePresets(builtin, dbRows) {
  const byCode = new Map();
  builtin.forEach((p) => {
    if (!p?.code) return;
    byCode.set(p.code, { ...p, source: "builtin" });
  });
  (Array.isArray(dbRows) ? dbRows : []).forEach((row) => {
    if (!row?.code) return;
    byCode.set(row.code, {
      id: row.id,
      code: row.code,
      label: row.label,
      defaultText: row.default_message_text,
      isActive: row.is_active,
      source: "custom",
    });
  });
  return [...byCode.values()].sort((a, b) => String(a.label || "").localeCompare(String(b.label || ""), "pt"));
}

function normalizePresetCodeInput(raw) {
  const s = String(raw || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_]+/g, "_")
    .replace(/^_+|_+$/g, "");
  return s || null;
}

function makeUniquePresetCodeFromLabel(label) {
  const base = normalizePresetCodeInput(label.replace(/\s+/g, "_")) || "preset";
  return `${base}_${Date.now()}`;
}

async function listMessagePresetsForScope(scope) {
  if (scope === "supervisor") {
    const custom = await db.query(
      `SELECT id, code, label, default_message_text, is_active
       FROM ops_message_presets
       WHERE scope = 'supervisor' AND is_active = TRUE
       ORDER BY label ASC`
    );
    return mergeMessagePresets(BUILTIN_SUPERVISOR_MESSAGE_PRESETS, custom.rows);
  }
  if (scope === "driver") {
    const custom = await db.query(
      `SELECT id, code, label, default_message_text, is_active
       FROM ops_message_presets
       WHERE scope = 'driver' AND is_active = TRUE
       ORDER BY label ASC`
    );
    return mergeMessagePresets(BUILTIN_DRIVER_MESSAGE_PRESETS, custom.rows);
  }
  return [];
}

router.get("/message-presets", async (req, res) => {
  try {
    await ensureOpsMessagePresetsTable();
    const scope = String(req.query.scope || "supervisor").trim().toLowerCase();
    if (scope !== "supervisor" && scope !== "driver") {
      return res.status(400).json({ message: "Indique scope=supervisor ou driver." });
    }
    const list = await listMessagePresetsForScope(scope);
    return res.json(list);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar predefinidas de mensagem." });
  }
});

router.get("/message-presets/manage", async (req, res) => {
  try {
    await ensureOpsMessagePresetsTable();
    const scope = String(req.query.scope || "supervisor").trim().toLowerCase();
    if (scope !== "supervisor" && scope !== "driver") {
      return res.status(400).json({ message: "Indique scope=supervisor ou driver." });
    }
    const result = await db.query(
      `SELECT id, scope, code, label, default_message_text, is_active, created_at, updated_at
       FROM ops_message_presets
       WHERE scope = $1
       ORDER BY is_active DESC, label ASC`,
      [scope]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar predefinidas (gestão)." });
  }
});

router.post("/message-presets", async (req, res) => {
  try {
    await ensureOpsMessagePresetsTable();
    const scope = String(req.body?.scope || "").trim().toLowerCase();
    if (scope !== "supervisor" && scope !== "driver") {
      return res.status(400).json({ message: "scope inválido (use supervisor ou driver)." });
    }
    const label = String(req.body?.label || "").trim();
    if (label.length < 2) {
      return res.status(400).json({ message: "Indique um rótulo com pelo menos 2 caracteres." });
    }
    const defaultText = String(req.body?.defaultText || "").trim() || label;
    const isActive = req.body?.isActive !== false;
    let code = normalizePresetCodeInput(req.body?.code) || makeUniquePresetCodeFromLabel(label);
    if (code.length > 60) {
      return res.status(400).json({ message: "Código demasiado longo (máx. 60 caracteres)." });
    }
    const builtinSet = new Set(
      (scope === "supervisor" ? BUILTIN_SUPERVISOR_MESSAGE_PRESETS : BUILTIN_DRIVER_MESSAGE_PRESETS).map((p) => p.code)
    );
    if (builtinSet.has(code)) {
      return res.status(409).json({ message: "Esse código está reservado (predefinida de fábrica). Escolha outro." });
    }
    const inserted = await db.query(
      `INSERT INTO ops_message_presets (scope, code, label, default_message_text, is_active, created_by_user_id, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, NOW())
       ON CONFLICT (scope, code) DO UPDATE
       SET label = EXCLUDED.label,
           default_message_text = EXCLUDED.default_message_text,
           is_active = EXCLUDED.is_active,
           created_by_user_id = COALESCE(ops_message_presets.created_by_user_id, EXCLUDED.created_by_user_id),
           updated_at = NOW()
       RETURNING id, scope, code, label, default_message_text, is_active, created_at, updated_at`,
      [scope, code, label, defaultText, isActive, req.user.id]
    );
    return res.status(201).json({ preset: inserted.rows[0] });
  } catch (error) {
    if (String(error?.code) === "23505") {
      return res.status(409).json({ message: "Já existe uma predefinida com esse código para este contexto." });
    }
    return res.status(500).json({ message: "Erro ao criar predefinida." });
  }
});

router.patch("/message-presets/:presetId", async (req, res) => {
  try {
    await ensureOpsMessagePresetsTable();
    const presetId = Number(req.params.presetId);
    if (!Number.isFinite(presetId) || presetId <= 0) {
      return res.status(400).json({ message: "Identificador inválido." });
    }
    const label = String(req.body?.label || "").trim();
    const defaultText = String(req.body?.defaultText || "").trim();
    if (label && label.length < 2) {
      return res.status(400).json({ message: "Rótulo inválido." });
    }
    if (req.body?.isActive === undefined && !label && !defaultText) {
      return res.status(400).json({ message: "Nada a atualizar." });
    }
    const updated = await db.query(
      `UPDATE ops_message_presets
       SET
         label = COALESCE(NULLIF($2::text, ''), label),
         default_message_text = COALESCE(NULLIF($3::text, ''), default_message_text),
         is_active = COALESCE($4::boolean, is_active),
         updated_at = NOW()
       WHERE id = $1
       RETURNING id, scope, code, label, default_message_text, is_active, created_at, updated_at`,
      [presetId, label || null, defaultText || null, typeof req.body?.isActive === "boolean" ? req.body.isActive : null]
    );
    if (!updated.rowCount) {
      return res.status(404).json({ message: "Predefinida não encontrada." });
    }
    return res.json({ preset: updated.rows[0] });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar predefinida." });
  }
});

router.delete("/message-presets/:presetId", async (req, res) => {
  try {
    await ensureOpsMessagePresetsTable();
    const presetId = Number(req.params.presetId);
    if (!Number.isFinite(presetId) || presetId <= 0) {
      return res.status(400).json({ message: "Identificador inválido." });
    }
    const result = await db.query(`DELETE FROM ops_message_presets WHERE id = $1 RETURNING id`, [presetId]);
    if (!result.rowCount) {
      return res.status(404).json({ message: "Predefinida não encontrada." });
    }
    return res.json({ ok: true, id: result.rows[0].id });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao remover predefinida." });
  }
});

router.get("/messages/threads", async (_req, res) => {
  try {
    await ensureOpsMessagesTable();
    const result = await db.query(
      `SELECT
         u.id AS driver_id,
         u.name AS driver_name,
         u.mechanic_number,
         MAX(m.created_at) AS last_message_at,
         COUNT(*) FILTER (
           WHERE m.to_user_id = $1
             AND LOWER(TRIM(fu.role::text)) = 'driver'
             AND m.read_at IS NULL
         ) AS unread_from_driver
       FROM ops_messages m
       JOIN users fu ON fu.id = m.from_user_id
       JOIN users u ON u.id = CASE WHEN m.from_user_id = $1 THEN m.to_user_id ELSE m.from_user_id END
       WHERE (m.from_user_id = $1 OR m.to_user_id = $1)
         AND LOWER(TRIM(u.role::text)) = 'driver'
       GROUP BY u.id, u.name, u.mechanic_number
       ORDER BY MAX(m.created_at) DESC
       LIMIT 200`,
      [_req.user.id]
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar conversas com motoristas." });
  }
});

router.get("/messages", async (req, res) => {
  try {
    await ensureOpsMessagesTable();
    const driverId = Number(req.query.driverId);
    if (!Number.isFinite(driverId) || driverId <= 0) {
      return res.status(400).json({ message: "Indique driverId válido." });
    }
    const driverCheck = await db.query(
      `SELECT id, name, mechanic_number
       FROM users
       WHERE id = $1
         AND LOWER(TRIM(role::text)) = 'driver'
       LIMIT 1`,
      [driverId]
    );
    if (!driverCheck.rowCount) {
      return res.status(404).json({ message: "Motorista não encontrado." });
    }

    const result = await db.query(
      `SELECT
         m.id,
         m.from_user_id,
         m.to_user_id,
         m.message_text,
         m.preset_code,
         m.is_traffic_alert,
         m.related_service_id,
         m.read_at,
         m.created_at,
         fu.name AS from_name,
         fu.role::text AS from_role,
         tu.name AS to_name,
         tu.role::text AS to_role
       FROM ops_messages m
       JOIN users fu ON fu.id = m.from_user_id
       JOIN users tu ON tu.id = m.to_user_id
       WHERE (
         (m.from_user_id = $1 AND m.to_user_id = $2)
         OR (m.from_user_id = $2 AND m.to_user_id = $1)
       )
       ORDER BY m.created_at DESC
       LIMIT 300`,
      [req.user.id, driverId]
    );
    return res.json({
      driver: driverCheck.rows[0],
      messages: result.rows,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar mensagens com o motorista." });
  }
});

router.post("/messages", async (req, res) => {
  try {
    await ensureOpsMessagesTable();
    const driverId = Number(req.body?.driverId);
    const messageText = String(req.body?.message || "").trim();
    const presetCode = String(req.body?.presetCode || "").trim() || null;
    const isTrafficAlert = req.body?.isTrafficAlert === true;
    const relatedServiceIdRaw = Number(req.body?.relatedServiceId);
    const relatedServiceId = Number.isFinite(relatedServiceIdRaw) && relatedServiceIdRaw > 0 ? relatedServiceIdRaw : null;

    if (!Number.isFinite(driverId) || driverId <= 0) {
      return res.status(400).json({ message: "driverId inválido." });
    }
    if (!messageText) {
      return res.status(400).json({ message: "Mensagem obrigatória." });
    }
    const driverCheck = await db.query(
      `SELECT id, name
       FROM users
       WHERE id = $1
         AND is_active = TRUE
         AND LOWER(TRIM(role::text)) = 'driver'
       LIMIT 1`,
      [driverId]
    );
    if (!driverCheck.rowCount) {
      return res.status(404).json({ message: "Motorista não encontrado/ativo." });
    }

    const inserted = await db.query(
      `INSERT INTO ops_messages (
         from_user_id, to_user_id, message_text, preset_code, is_traffic_alert, related_service_id
       )
       VALUES ($1, $2, $3, $4, $5, $6)
       RETURNING id, created_at`,
      [req.user.id, driverId, messageText, presetCode, isTrafficAlert, relatedServiceId]
    );
    return res.status(201).json({
      message: "Mensagem enviada ao motorista.",
      id: inserted.rows[0].id,
      createdAt: inserted.rows[0].created_at,
      driver: driverCheck.rows[0],
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao enviar mensagem ao motorista." });
  }
});

router.patch("/messages/:messageId/read", async (req, res) => {
  try {
    await ensureOpsMessagesTable();
    const messageId = Number(req.params.messageId);
    if (!Number.isFinite(messageId) || messageId <= 0) {
      return res.status(400).json({ message: "Identificador inválido." });
    }
    const updated = await db.query(
      `UPDATE ops_messages
       SET read_at = NOW()
       WHERE id = $1
         AND to_user_id = $2
         AND read_at IS NULL
       RETURNING id`,
      [messageId, req.user.id]
    );
    if (!updated.rowCount) {
      return res.status(404).json({ message: "Mensagem não encontrada." });
    }
    return res.json({ ok: true, id: updated.rows[0].id });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao marcar mensagem como lida." });
  }
});

router.get("/conflict-alerts", async (_req, res) => {
  try {
    await ensureSupervisorConflictAlertsTable();
    const result = await db.query(
      `SELECT
         a.id,
         a.driver_id,
         a.alert_type,
         u.name AS driver_name,
         u.mechanic_number AS driver_mechanic_number,
         a.roster_id,
         a.planned_service_id,
         ps.service_code,
         COALESCE(a.service_schedule, ps.service_schedule) AS service_schedule,
         COALESCE(a.line_code, ps.line_code) AS line_code,
         ps.start_location,
         ps.end_location,
         a.affected_driver_id,
         u2.name AS affected_driver_name,
         u2.mechanic_number AS affected_driver_mechanic_number,
         a.affected_planned_service_id,
         ps2.service_code AS affected_service_code,
         a.conflict_planned_service_ids,
         a.unassigned_planned_service_ids,
         a.notes,
         a.created_at
       FROM supervisor_conflict_alerts a
       JOIN users u ON u.id = a.driver_id
       LEFT JOIN planned_services ps ON ps.id = a.planned_service_id
       LEFT JOIN users u2 ON u2.id = a.affected_driver_id
       LEFT JOIN planned_services ps2 ON ps2.id = a.affected_planned_service_id
       ORDER BY a.created_at DESC
       LIMIT 200`
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar alertas de conflito." });
  }
});

router.get("/route-incidents", async (req, res) => {
  try {
    await ensureServiceRouteIncidentsTable();
    const filters = buildRouteIncidentsFilterSql(req.query, { allowLimit: true });
    const result = await db.query(
      `SELECT
         sri.id,
         sri.service_id,
         sri.driver_id,
         u.name AS driver_name,
         u.mechanic_number AS driver_mechanic_number,
         sri.line_code,
         sri.fleet_number,
         sri.plate_number,
         sri.gtfs_trip_id,
         sri.threshold_m,
         sri.max_deviation_m,
         sri.first_detected_at,
         sri.last_detected_at,
         sri.resolved_at,
         s.status AS service_status
       FROM service_route_incidents sri
       JOIN users u ON u.id = sri.driver_id
       LEFT JOIN services s ON s.id = sri.service_id
       ${filters.whereSql}
       ORDER BY COALESCE(sri.resolved_at, sri.last_detected_at) DESC
       ${filters.limitSql}`,
      filters.values
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar incidências de desvio de rota." });
  }
});

router.get("/route-incidents/export.csv", async (req, res) => {
  try {
    await ensureServiceRouteIncidentsTable();
    const filters = buildRouteIncidentsFilterSql(req.query, { allowLimit: false });
    const result = await db.query(
      `SELECT
         sri.id,
         sri.service_id,
         sri.driver_id,
         u.name AS driver_name,
         u.mechanic_number AS driver_mechanic_number,
         sri.line_code,
         sri.fleet_number,
         sri.plate_number,
         sri.gtfs_trip_id,
         sri.threshold_m,
         sri.max_deviation_m,
         sri.first_detected_at,
         sri.last_detected_at,
         sri.resolved_at,
         s.status AS service_status
       FROM service_route_incidents sri
       JOIN users u ON u.id = sri.driver_id
       LEFT JOIN services s ON s.id = sri.service_id
       ${filters.whereSql}
       ORDER BY COALESCE(sri.resolved_at, sri.last_detected_at) DESC
       LIMIT 5000`,
      filters.values
    );
    const header = [
      "incidencia_id",
      "servico_id",
      "estado_incidencia",
      "estado_servico",
      "motorista_id",
      "motorista",
      "numero_mecanografico",
      "linha",
      "frota",
      "chapa",
      "trip_gtfs",
      "limiar_m",
      "desvio_maximo_m",
      "primeira_detecao",
      "ultima_detecao",
      "resolvida_em",
    ];
    const rows = result.rows.map((r) =>
      [
        r.id,
        r.service_id,
        r.resolved_at ? "resolvida" : "em_aberto",
        r.service_status || "",
        r.driver_id,
        r.driver_name || "",
        r.driver_mechanic_number || "",
        r.line_code || "",
        r.fleet_number || "",
        r.plate_number || "",
        r.gtfs_trip_id || "",
        r.threshold_m == null ? "" : Number(r.threshold_m).toFixed(2),
        r.max_deviation_m == null ? "" : Number(r.max_deviation_m).toFixed(2),
        r.first_detected_at ? new Date(r.first_detected_at).toISOString() : "",
        r.last_detected_at ? new Date(r.last_detected_at).toISOString() : "",
        r.resolved_at ? new Date(r.resolved_at).toISOString() : "",
      ]
        .map(csvEscape)
        .join(",")
    );
    const csv = [header.map(csvEscape).join(","), ...rows].join("\n");
    res.setHeader("Content-Type", "text/csv; charset=utf-8");
    res.setHeader("Content-Disposition", "attachment; filename=incidencias_desvio_rota.csv");
    return res.status(200).send(csv);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar incidências de desvio (CSV)." });
  }
});

router.get("/route-incidents/export.xlsx", async (req, res) => {
  try {
    await ensureServiceRouteIncidentsTable();
    const filters = buildRouteIncidentsFilterSql(req.query, { allowLimit: false });
    const result = await db.query(
      `SELECT
         sri.id,
         sri.service_id,
         sri.driver_id,
         u.name AS driver_name,
         u.mechanic_number AS driver_mechanic_number,
         sri.line_code,
         sri.fleet_number,
         sri.plate_number,
         sri.gtfs_trip_id,
         sri.threshold_m,
         sri.max_deviation_m,
         sri.first_detected_at,
         sri.last_detected_at,
         sri.resolved_at,
         s.status AS service_status
       FROM service_route_incidents sri
       JOIN users u ON u.id = sri.driver_id
       LEFT JOIN services s ON s.id = sri.service_id
       ${filters.whereSql}
       ORDER BY COALESCE(sri.resolved_at, sri.last_detected_at) DESC
       LIMIT 5000`,
      filters.values
    );
    const rows = result.rows.map((r) => ({
      incidencia_id: r.id,
      servico_id: r.service_id,
      estado_incidencia: r.resolved_at ? "resolvida" : "em_aberto",
      estado_servico: r.service_status || "",
      motorista_id: r.driver_id,
      motorista: r.driver_name || "",
      numero_mecanografico: r.driver_mechanic_number || "",
      linha: r.line_code || "",
      frota: r.fleet_number || "",
      chapa: r.plate_number || "",
      trip_gtfs: r.gtfs_trip_id || "",
      limiar_m: r.threshold_m == null ? "" : Number(r.threshold_m).toFixed(2),
      desvio_maximo_m: r.max_deviation_m == null ? "" : Number(r.max_deviation_m).toFixed(2),
      primeira_detecao: r.first_detected_at ? new Date(r.first_detected_at).toISOString() : "",
      ultima_detecao: r.last_detected_at ? new Date(r.last_detected_at).toISOString() : "",
      resolvida_em: r.resolved_at ? new Date(r.resolved_at).toISOString() : "",
    }));
    const wb = XLSX.utils.book_new();
    const ws = XLSX.utils.json_to_sheet(rows);
    XLSX.utils.book_append_sheet(wb, ws, "incidencias");
    const buffer = XLSX.write(wb, { bookType: "xlsx", type: "buffer" });
    res.setHeader(
      "Content-Type",
      "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet"
    );
    res.setHeader("Content-Disposition", "attachment; filename=incidencias_desvio_rota.xlsx");
    return res.status(200).send(buffer);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar incidências de desvio (Excel)." });
  }
});

router.get("/handover-alerts", async (_req, res) => {
  try {
    const result = await db.query(
      `SELECT
         h.id,
         h.service_id,
         h.reason,
         h.notes,
         h.status,
         h.created_at,
         fu.id AS from_driver_id,
         fu.name AS from_driver_name,
         fu.mechanic_number AS from_driver_mechanic_number,
         tu.id AS to_driver_id,
         tu.name AS to_driver_name
       FROM service_handover_events h
       JOIN users fu ON fu.id = h.from_driver_id
       LEFT JOIN users tu ON tu.id = h.to_driver_id
       WHERE LOWER(TRIM(h.status::text)) = 'pending'
       ORDER BY h.created_at DESC
       LIMIT 200`
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar alertas de handover." });
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

router.get("/roster/overdue", async (req, res) => {
  try {
    const dateRaw = String(req.query.date || "").trim();
    const serviceDate = dateRaw && /^\d{4}-\d{2}-\d{2}$/.test(dateRaw) ? dateRaw : null;
    const graceRaw = Number(req.query.graceMin);
    const graceMin = Number.isFinite(graceRaw) ? Math.min(Math.max(Math.round(graceRaw), 0), 120) : 5;

    await ensurePlannedServiceLocationColumns();
    await ensureTrackerTables();
    await ensureDeadheadTables();

    const result = await db.query(
      `WITH now_lisbon AS (
         SELECT (NOW() AT TIME ZONE 'Europe/Lisbon') AS ts
       ),
       roster_base AS (
         SELECT
           dr.id AS roster_id,
           dr.service_date,
           dr.status AS roster_status,
           dr.driver_id,
           u.name AS driver_name,
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
           substring(COALESCE(ps.service_schedule, '') FROM '(\\d{1,2}:\\d{2})') AS hhmm
         FROM daily_roster dr
         JOIN planned_services ps ON ps.id = dr.planned_service_id
         JOIN users u ON u.id = dr.driver_id
         WHERE dr.service_date = COALESCE($1::date, CURRENT_DATE)
           AND COALESCE(ps.kms_carga, 0) > 0
       ),
       roster_eval AS (
         SELECT
           rb.*,
           CASE
             WHEN rb.hhmm ~ '^\\d{1,2}:\\d{2}$' THEN split_part(rb.hhmm, ':', 1)::int
             ELSE NULL
           END AS hh,
           CASE
             WHEN rb.hhmm ~ '^\\d{1,2}:\\d{2}$' THEN split_part(rb.hhmm, ':', 2)::int
             ELSE NULL
           END AS mm
         FROM roster_base rb
       ),
       roster_due AS (
         SELECT
           re.*,
           (re.hh * 60 + re.mm) AS scheduled_start_min,
           (
             SELECT EXTRACT(HOUR FROM n.ts)::int * 60 + EXTRACT(MINUTE FROM n.ts)::int
             FROM now_lisbon n
           ) AS now_minute_lisbon
         FROM roster_eval re
         WHERE re.hh IS NOT NULL
           AND re.mm IS NOT NULL
       )
       SELECT
         rd.roster_id,
         rd.service_date,
         rd.roster_status,
         rd.driver_id,
         rd.driver_name,
         rd.driver_mechanic_number,
         rd.planned_service_id,
         rd.service_code,
         rd.line_code,
         rd.fleet_number,
         rd.plate_number,
         rd.service_schedule,
         rd.start_location,
         rd.end_location,
         rd.kms_carga,
         rd.scheduled_start_min,
         rd.now_minute_lisbon,
         GREATEST(0, rd.now_minute_lisbon - rd.scheduled_start_min) AS overdue_minutes,
         td.imei AS tracker_imei,
         lp.captured_at AS tracker_last_point_at,
         lp.speed_kmh AS tracker_last_speed_kmh,
         CASE
           WHEN lp.captured_at IS NULL THEN 'unknown'
           WHEN lp.captured_at < NOW() - INTERVAL '5 minutes' THEN 'unknown'
           WHEN COALESCE(lp.speed_kmh, 0) >= 3 THEN 'moving'
           ELSE 'stopped'
         END AS vehicle_motion_status
       FROM roster_due rd
       LEFT JOIN LATERAL (
         SELECT t.imei
         FROM tracker_devices t
         WHERE t.is_active = TRUE
           AND (
             (
               COALESCE(NULLIF(TRIM(rd.fleet_number), ''), '') <> ''
               AND LOWER(TRIM(COALESCE(t.fleet_number, ''))) = LOWER(TRIM(COALESCE(rd.fleet_number, '')))
             )
             OR (
               COALESCE(NULLIF(TRIM(rd.plate_number), ''), '') <> ''
               AND LOWER(TRIM(COALESCE(t.plate_number, ''))) = LOWER(TRIM(COALESCE(rd.plate_number, '')))
             )
           )
         ORDER BY t.updated_at DESC
         LIMIT 1
       ) td ON TRUE
       LEFT JOIN LATERAL (
         SELECT dp.captured_at, dp.speed_kmh
         FROM deadhead_movements dm
         JOIN deadhead_points dp ON dp.movement_id = dm.id
         WHERE dm.imei = td.imei
         ORDER BY dp.captured_at DESC
         LIMIT 1
       ) lp ON TRUE
       WHERE rd.now_minute_lisbon >= rd.scheduled_start_min + $2::int
         AND COALESCE(NULLIF(LOWER(TRIM(rd.roster_status::text)), ''), 'pending') IN (
           'pending', 'assigned', 'pendente', 'atribuido', 'atribuído', 'delayed', 'atrasado'
         )
         AND NOT EXISTS (
           SELECT 1
           FROM services s
           WHERE s.planned_service_id = rd.planned_service_id
             AND s.driver_id = rd.driver_id
             AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = rd.service_date
         )
       ORDER BY rd.scheduled_start_min ASC, rd.driver_name ASC`,
      [serviceDate, graceMin]
    );

    return res.json({
      date: serviceDate || null,
      graceMin,
      items: result.rows,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar serviços por iniciar fora da hora." });
  }
});

router.patch("/roster/:rosterId/status", async (req, res) => {
  const rosterId = Number(req.params.rosterId);
  const status = String(req.body?.status || "")
    .trim()
    .toLowerCase();
  const allowed = new Set(["delayed", "atrasado", "cancelled", "not_realized"]);

  if (!Number.isFinite(rosterId) || rosterId <= 0) {
    return res.status(400).json({ message: "Identificador de escala inválido." });
  }
  if (!allowed.has(status)) {
    return res.status(400).json({ message: "Estado inválido. Use delayed, cancelled ou not_realized." });
  }

  const normalizedStatus = status === "atrasado" ? "delayed" : status;
  try {
    const updated = await db.query(
      `UPDATE daily_roster
       SET status = $2
       WHERE id = $1
       RETURNING id AS roster_id, status, service_date, driver_id, planned_service_id`,
      [rosterId, normalizedStatus]
    );
    if (!updated.rowCount) {
      return res.status(404).json({ message: "Linha de escala não encontrada." });
    }
    return res.json({
      message: "Estado da linha de escala atualizado.",
      roster: updated.rows[0],
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao atualizar estado da linha de escala." });
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
    await ensureSupervisorConflictAlertsTable();
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

    const involvedRosterRes = await client.query(
      `SELECT
         dr.id AS roster_id,
         dr.driver_id,
         u.name AS driver_name,
         dr.planned_service_id,
         ps.service_code,
         ps.service_schedule,
         ps.line_code
       FROM daily_roster dr
       JOIN planned_services ps ON ps.id = dr.planned_service_id
       JOIN users u ON u.id = dr.driver_id
       WHERE dr.service_date = $1::date
         AND dr.driver_id IN ($2, $3)
       ORDER BY dr.driver_id ASC, ps.service_schedule ASC NULLS LAST`,
      [dr.service_date, dr.driver_id, newId]
    );

    const rowsByDriver = new Map();
    for (const row of involvedRosterRes.rows) {
      const key = Number(row.driver_id);
      if (!rowsByDriver.has(key)) rowsByDriver.set(key, []);
      rowsByDriver.get(key).push(row);
    }

    const overlapAlerts = [];
    for (const [, rows] of rowsByDriver.entries()) {
      for (let i = 0; i < rows.length; i += 1) {
        for (let j = i + 1; j < rows.length; j += 1) {
          const a = rows[i];
          const b = rows[j];
          if (!hasScheduleOverlap(a.service_schedule, b.service_schedule)) continue;
          overlapAlerts.push({
            driver_id: Number(a.driver_id),
            roster_id: Number(a.roster_id),
            planned_service_id: Number(a.planned_service_id),
            affected_driver_id: Number(b.driver_id),
            affected_planned_service_id: Number(b.planned_service_id),
            service_schedule: a.service_schedule || null,
            line_code: a.line_code || null,
            conflict_planned_service_ids: [Number(b.planned_service_id)].filter((v) => Number.isFinite(v) && v > 0),
            notes: `Conflito de horário após troca: ${a.service_code || a.planned_service_id} (${a.service_schedule || "-"}) com ${b.service_code || b.planned_service_id} (${b.service_schedule || "-"}) para o motorista ${a.driver_name || "-"}.`,
          });
        }
      }
    }

    for (const alertItem of overlapAlerts) {
      await client.query(
        `INSERT INTO supervisor_conflict_alerts (
           driver_id, alert_type, roster_id, planned_service_id, affected_driver_id, affected_planned_service_id,
           service_schedule, line_code, conflict_planned_service_ids, notes
         ) VALUES ($1, 'driver_swap_overlap', $2, $3, $4, $5, $6, $7, $8::jsonb, $9)`,
        [
          alertItem.driver_id,
          alertItem.roster_id,
          alertItem.planned_service_id,
          alertItem.affected_driver_id,
          alertItem.affected_planned_service_id,
          alertItem.service_schedule,
          alertItem.line_code,
          JSON.stringify(alertItem.conflict_planned_service_ids || []),
          alertItem.notes,
        ]
      );
    }

    const unassignedRes = await client.query(
      `SELECT ps.id, ps.service_code
       FROM planned_services ps
       LEFT JOIN daily_roster dr
         ON dr.planned_service_id = ps.id
        AND dr.service_date = $1::date
       WHERE dr.id IS NULL
         AND LOWER(TRIM(COALESCE(ps.line_code, ''))) = LOWER(TRIM(COALESCE($2, ps.line_code)))
       ORDER BY ps.service_code ASC NULLS LAST, ps.id ASC
       LIMIT 100`,
      [dr.service_date, dr.line_code || null]
    );
    if (unassignedRes.rowCount > 0) {
      const missingIds = unassignedRes.rows
        .map((r) => Number(r.id))
        .filter((v) => Number.isFinite(v) && v > 0);
      await client.query(
        `INSERT INTO supervisor_conflict_alerts (
           driver_id, alert_type, roster_id, planned_service_id, service_schedule, line_code,
           conflict_planned_service_ids, unassigned_planned_service_ids, notes
         ) VALUES ($1, 'unassigned_service_after_swap', $2, $3, $4, $5, '[]'::jsonb, $6::jsonb, $7)`,
        [
          newId,
          rosterId,
          dr.planned_service_id,
          dr.service_schedule || null,
          dr.line_code || null,
          JSON.stringify(missingIds),
          `Serviços sem motorista atribuído detetados para ${dr.service_date} na linha ${dr.line_code || "-"}: ${unassignedRes.rows
            .map((r) => r.service_code || `#${r.id}`)
            .join(", ")}.`,
        ]
      );
    }

    await client.query("COMMIT");
    return rosterReassignJson(res, 200, {
      roster_id: rosterId,
      new_driver_id: newId,
      new_driver_name: nd.name,
      conflict_alerts_created: overlapAlerts.length + (unassignedRes?.rowCount > 0 ? 1 : 0),
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
    await ensureUsersEmailNullable();
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
  await ensureUsersEmailNullable();
  let inserted = 0;
  let updated = 0;
  const errors = [];
  const rowReports = [];
  for (let rowIndex = 0; rowIndex < rows.length; rowIndex += 1) {
    const row = rows[rowIndex];
    const by = rowToHeaderLookup(row);
    const name = row.name || row.nome || by.name || by.nome || "";
    const username =
      row.username ||
      row.utilizador ||
      by.username ||
      by.utilizador ||
      by.nome_utilizador ||
      by.nome_de_utilizador ||
      "";
    const email = String(row.email || by.email || "").trim() || null;
    const mechanicNumberRaw =
      row.mechanic_number ||
      row.mecanografico ||
      row.numero_mecanografico ||
      by.mechanic_number ||
      by.mecanografico ||
      by.numero_mecanografico ||
      by.numero_mecanico ||
      "";
    const mechanicNumber = String(mechanicNumberRaw || "").trim();
    const passwordRaw = row.password || row.senha || by.password || by.senha || "0000";
    const password = String(passwordRaw || "").trim() || "0000";
    const companyName = row.company_name || row.empresa || by.company_name || by.empresa || defaultCompany || null;
    const isActive = parseBooleanLike(row.is_active ?? row.ativo ?? by.is_active ?? by.ativo, true);
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
           OR ($3::text IS NOT NULL AND email = $3::text)
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
          : error?.code === "23502" && error?.column === "email"
            ? "Base de dados ainda exige email. Execute a atualizacao que torna email opcional."
            : `Erro inesperado ao processar a linha${error?.code ? ` (${error.code})` : ""}.`;
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
      const plateNumber = String(
        row.numero_chapa ||
          row.numerochapa ||
          row.matricula ||
          row.matricula_viatura ||
          row.plate_number ||
          row.plateNumber ||
          ""
      ).trim();
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
    await ensurePlannedServiceLocationColumns();
    const selectCloseMode = (await hasServiceCloseModeColumn()) ? "s.close_mode" : "NULL::text AS close_mode";
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
         ${selectCloseMode},
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
    const rowsWithMetrics = await enrichServiceRowsForExportSafe(result.rows);
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
         s.plate_number,
         s.service_schedule,
         s.status,
         s.route_deviation_m,
         s.is_off_route,
         u.name AS driver_name,
         u.mechanic_number AS driver_mechanic_number,
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

router.get("/depots", async (_req, res) => {
  try {
    await ensureTrackerTables();
    await ensureDepotTables();
    const result = await db.query(
      `SELECT
         d.id,
         d.depot_code,
         d.depot_name,
         d.lat,
         d.lng,
         d.capacity_total,
         d.is_active,
         d.notes,
         d.updated_at,
         COUNT(td.id)::int AS assigned_vehicles_count
       FROM depots d
       LEFT JOIN tracker_devices td ON td.depot_id = d.id AND td.is_active = TRUE
       GROUP BY d.id
       ORDER BY d.depot_name ASC, d.id ASC`
    );
    return res.json(result.rows);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao listar parques de pernoita." });
  }
});

router.post("/depots", async (req, res) => {
  const code = String(req.body?.depotCode || "").trim() || null;
  const name = String(req.body?.depotName || "").trim();
  const lat = Number(req.body?.lat);
  const lng = Number(req.body?.lng);
  const capacityTotalRaw = Number(req.body?.capacityTotal);
  const isActive = req.body?.isActive !== false;
  const notes = String(req.body?.notes || "").trim() || null;
  if (!name) return res.status(400).json({ message: "Indique o nome do parque." });
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) {
    return res.status(400).json({ message: "Indique latitude/longitude válidas." });
  }
  const capacityTotal = Number.isFinite(capacityTotalRaw) ? Math.max(0, Math.round(capacityTotalRaw)) : 0;
  try {
    await ensureTrackerTables();
    await ensureDepotTables();
    const result = await db.query(
      `INSERT INTO depots (depot_code, depot_name, lat, lng, capacity_total, is_active, notes, updated_at)
       VALUES ($1, $2, $3, $4, $5, $6, $7, NOW())
       RETURNING id, depot_code, depot_name, lat, lng, capacity_total, is_active, notes, updated_at`,
      [code, name, lat, lng, capacityTotal, isActive, notes]
    );
    return res.status(201).json(result.rows[0]);
  } catch (error) {
    if (error?.code === "23505") {
      return res.status(409).json({ message: "Já existe um parque com esse código." });
    }
    return res.status(500).json({ message: "Erro ao criar parque de pernoita." });
  }
});

router.patch("/depots/:depotId", async (req, res) => {
  const depotId = Number(req.params.depotId);
  if (!Number.isFinite(depotId) || depotId <= 0) {
    return res.status(400).json({ message: "Identificador de parque inválido." });
  }
  const code = req.body?.depotCode == null ? null : String(req.body.depotCode).trim() || null;
  const name = req.body?.depotName == null ? null : String(req.body.depotName).trim();
  const lat = req.body?.lat == null ? null : Number(req.body.lat);
  const lng = req.body?.lng == null ? null : Number(req.body.lng);
  const capacity = req.body?.capacityTotal == null ? null : Number(req.body.capacityTotal);
  const isActive = typeof req.body?.isActive === "boolean" ? req.body.isActive : null;
  const notes = req.body?.notes == null ? null : String(req.body.notes).trim() || null;
  if (lat != null && !Number.isFinite(lat)) return res.status(400).json({ message: "Latitude inválida." });
  if (lng != null && !Number.isFinite(lng)) return res.status(400).json({ message: "Longitude inválida." });
  if (capacity != null && !Number.isFinite(capacity)) return res.status(400).json({ message: "Capacidade inválida." });
  try {
    await ensureTrackerTables();
    await ensureDepotTables();
    const result = await db.query(
      `UPDATE depots
       SET depot_code = COALESCE($2, depot_code),
           depot_name = COALESCE(NULLIF($3, ''), depot_name),
           lat = COALESCE($4, lat),
           lng = COALESCE($5, lng),
           capacity_total = COALESCE($6, capacity_total),
           is_active = COALESCE($7, is_active),
           notes = COALESCE($8, notes),
           updated_at = NOW()
       WHERE id = $1
       RETURNING id, depot_code, depot_name, lat, lng, capacity_total, is_active, notes, updated_at`,
      [depotId, code, name, lat, lng, capacity == null ? null : Math.max(0, Math.round(capacity)), isActive, notes]
    );
    if (!result.rowCount) return res.status(404).json({ message: "Parque não encontrado." });
    return res.json(result.rows[0]);
  } catch (error) {
    if (error?.code === "23505") {
      return res.status(409).json({ message: "Já existe um parque com esse código." });
    }
    return res.status(500).json({ message: "Erro ao atualizar parque de pernoita." });
  }
});

router.patch("/vehicles/:imei/depot", async (req, res) => {
  const imei = String(req.params.imei || "").replace(/\D/g, "");
  const depotId = Number(req.body?.depotId);
  if (!imei) return res.status(400).json({ message: "IMEI inválido." });
  if (!Number.isFinite(depotId) || depotId <= 0) {
    return res.status(400).json({ message: "Selecione um parque válido." });
  }
  try {
    await ensureTrackerTables();
    await ensureDepotTables();
    const depotRes = await db.query(`SELECT id FROM depots WHERE id = $1 LIMIT 1`, [depotId]);
    if (!depotRes.rowCount) return res.status(404).json({ message: "Parque não encontrado." });
    const result = await db.query(
      `UPDATE tracker_devices
       SET depot_id = $2, updated_at = NOW()
       WHERE imei = $1
       RETURNING imei, fleet_number, plate_number, depot_id`,
      [imei, depotId]
    );
    if (!result.rowCount) return res.status(404).json({ message: "Viatura não encontrada para o IMEI indicado." });
    return res.json(result.rows[0]);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao associar viatura ao parque." });
  }
});

async function resolveGtfsPlanningFeedKey(explicitFeedKeyRaw) {
  const explicit = String(explicitFeedKeyRaw || "").trim();
  if (explicit) {
    const hit = await db.query(
      `SELECT feed_key, feed_name, is_active FROM gtfs_feeds WHERE feed_key = $1 LIMIT 1`,
      [explicit]
    );
    if (!hit.rowCount) {
      return {
        ok: false,
        message: `Feed GTFS "${explicit}" não encontrado. Indique a chave exacta (Dados → feeds) ou use modo automático.`,
      };
    }
    if (!hit.rows[0].is_active) {
      return {
        ok: false,
        message: `O feed "${explicit}" está inactivo. Active-o na lista de feeds GTFS ou escolha outro feed.`,
      };
    }
    return {
      ok: true,
      feed_key: hit.rows[0].feed_key,
      feed_name: hit.rows[0].feed_name || hit.rows[0].feed_key,
      auto_selected: false,
    };
  }
  const auto = await db.query(
    `SELECT feed_key, feed_name
     FROM gtfs_feeds
     WHERE is_active = TRUE
     ORDER BY updated_at DESC NULLS LAST, created_at DESC NULLS LAST
     LIMIT 1`
  );
  if (!auto.rowCount) {
    return {
      ok: false,
      message:
        "Não existe nenhum feed GTFS activo. Importe um GTFS em Dados ou active um feed na lista — o gerador de chapas usa um único feed de cada vez.",
    };
  }
  return {
    ok: true,
    feed_key: auto.rows[0].feed_key,
    feed_name: auto.rows[0].feed_name || auto.rows[0].feed_key,
    auto_selected: true,
  };
}

async function fetchGtfsOperationalServicesForDate(serviceDate, lineCodesNormalized = [], resolvedFeedKey) {
  const dateIso = parseDateOnly(serviceDate) || parseDateOnly(new Date().toISOString().slice(0, 10));
  if (!dateIso) return [];
  const feedKey = String(resolvedFeedKey || "").trim();
  if (!feedKey) return [];
  const dateObj = new Date(`${dateIso}T00:00:00Z`);
  const jsWeekday = dateObj.getUTCDay();
  const weekday = jsWeekday === 0 ? 7 : jsWeekday; // 1..7, Monday-first
  const lineFilter = Array.isArray(lineCodesNormalized)
    ? lineCodesNormalized.map((code) => String(code || "").trim().toLowerCase()).filter(Boolean)
    : [];
  const lineFilterNumericCanonical = [
    ...new Set(lineFilter.map((code) => canonicalNumericLineCodeToken(code)).filter(Boolean)),
  ];
  const result = await db.query(
    `WITH trip_base AS (
       SELECT
         t.trip_id,
         t.service_id,
         t.feed_key,
         COALESCE(NULLIF(TRIM(r.route_short_name), ''), NULLIF(TRIM(r.route_long_name), ''), t.route_id) AS line_code,
         t.trip_headsign
       FROM gtfs_trips t
       JOIN gtfs_routes r ON r.feed_key = t.feed_key AND r.route_id = t.route_id
       WHERE t.feed_key = $4::text
     ),
     active_flags AS (
       SELECT
         tb.trip_id,
         tb.service_id,
         tb.feed_key,
         tb.line_code,
         tb.trip_headsign,
         EXISTS (
           SELECT 1
           FROM gtfs_calendars c
           WHERE c.feed_key = tb.feed_key
             AND c.service_id = tb.service_id
             AND c.is_active = TRUE
             AND $1::date BETWEEN c.start_date AND c.end_date
             AND CASE $2::int
                   WHEN 1 THEN c.monday
                   WHEN 2 THEN c.tuesday
                   WHEN 3 THEN c.wednesday
                   WHEN 4 THEN c.thursday
                   WHEN 5 THEN c.friday
                   WHEN 6 THEN c.saturday
                   WHEN 7 THEN c.sunday
                   ELSE 0
                 END = 1
         ) AS base_active,
         EXISTS (
           SELECT 1
           FROM gtfs_calendar_dates cd
           WHERE cd.feed_key = tb.feed_key
             AND cd.service_id = tb.service_id
             AND cd.calendar_date = $1::date
             AND cd.exception_type = 1
         ) AS added_by_exception,
         EXISTS (
           SELECT 1
           FROM gtfs_calendar_dates cd
           WHERE cd.feed_key = tb.feed_key
             AND cd.service_id = tb.service_id
             AND cd.calendar_date = $1::date
             AND cd.exception_type = 2
         ) AS removed_by_exception
       FROM trip_base tb
     )
     SELECT
       af.trip_id,
       af.service_id,
       af.line_code,
       af.trip_headsign,
       fst.departure_time AS first_departure_time,
       lst.departure_time AS last_departure_time,
       fs.stop_name AS start_stop_name,
       fs.stop_lat AS start_lat,
       fs.stop_lon AS start_lng,
       ls.stop_name AS end_stop_name,
       ls.stop_lat AS end_lat,
       ls.stop_lon AS end_lng
     FROM active_flags af
     LEFT JOIN LATERAL (
       SELECT st.departure_time, st.stop_id
       FROM gtfs_stop_times st
       WHERE st.feed_key = af.feed_key
         AND st.trip_id = af.trip_id
       ORDER BY st.stop_sequence ASC
       LIMIT 1
     ) fst ON TRUE
     LEFT JOIN LATERAL (
       SELECT st.departure_time, st.stop_id
       FROM gtfs_stop_times st
       WHERE st.feed_key = af.feed_key
         AND st.trip_id = af.trip_id
       ORDER BY st.stop_sequence DESC
       LIMIT 1
     ) lst ON TRUE
    LEFT JOIN gtfs_stops fs ON fs.feed_key = af.feed_key AND fs.stop_id = fst.stop_id
    LEFT JOIN gtfs_stops ls ON ls.feed_key = af.feed_key AND ls.stop_id = lst.stop_id
    WHERE (af.base_active OR af.added_by_exception)
      AND NOT af.removed_by_exception
      AND (
        COALESCE(array_length($3::text[], 1), 0) = 0
        OR LOWER(TRIM(COALESCE(af.line_code, ''))) = ANY ($3::text[])
        OR (
          COALESCE(array_length($5::text[], 1), 0) > 0
          AND COALESCE(
            NULLIF(REGEXP_REPLACE(LOWER(TRIM(COALESCE(af.line_code, ''))), '^0+', ''), ''),
            '0'
          ) = ANY ($5::text[])
        )
      )`,
    [dateIso, weekday, lineFilter, feedKey, lineFilterNumericCanonical]
  );
  return result.rows
    .map((row) => {
      const window = parseServiceWindowFromGtfs(row);
      if (!window) return null;
      return {
        ...row,
        service_code: row.trip_id,
        ...window,
      };
    })
    .filter(Boolean)
    .sort((a, b) => Number(a.start_min) - Number(b.start_min));
}

async function buildGtfsChapasZeroCandidatesDiagnostics(serviceDate, feedKey, lineCodesNormalized) {
  const noLineFiltered = await fetchGtfsOperationalServicesForDate(serviceDate, [], feedKey);
  const nCal = noLineFiltered.length;
  const wanted = Array.isArray(lineCodesNormalized)
    ? lineCodesNormalized.map((c) => String(c || "").trim().toLowerCase()).filter(Boolean)
    : [];
  if (nCal === 0) {
    return {
      reason: "no_calendar_trips_for_date",
      trips_if_no_line_filter: 0,
      line_codes_filter_applied: wanted,
      hint_pt:
        "Neste feed e data não há trips GTFS válidas segundo o calendário (datas de vigência / dia da semana / calendar_dates). Experimente outro dia útil ou confira os calendários no editor GTFS.",
    };
  }
  if (wanted.length > 0) {
    const seenLc = new Set();
    const uniqueDisplay = [];
    for (const r of noLineFiltered) {
      const c = String(r.line_code || "").trim();
      const k = c.toLowerCase();
      if (!k || seenLc.has(k)) continue;
      seenLc.add(k);
      uniqueDisplay.push(c);
      if (uniqueDisplay.length >= 15) break;
    }
    return {
      reason: "line_filter_excludes_all",
      trips_if_no_line_filter: nCal,
      line_codes_filter_applied: wanted,
      example_public_line_codes_today: uniqueDisplay.length ? uniqueDisplay : null,
      hint_pt:
        `O filtro de linhas (${wanted.join(", ")}) não coincide com nenhuma trip neste dia: há ${nCal} serviço(s) no feed sem esse filtro. ` +
          (uniqueDisplay.length
            ? `Exemplos de códigos de linha neste dia no GTFS (route_short_name ou fallback): ${uniqueDisplay.join(", ")}. `
            : "") +
          "Deixe o campo «Linhas» em branco para ver todas.",
    };
  }
  return {
    reason: "all_trips_filtered_by_time_parse",
    trips_if_no_line_filter: nCal,
    line_codes_filter_applied: [],
    hint_pt:
      "Há trips no calendário mas nenhuma com horários válidos nas stop_times para este dia — verifique importação GTFS ou partidas inconsistentes.",
  };
}

async function buildAutonomousChapasPlan(serviceDate, options = {}) {
  const minTurnaroundMin = Number.isFinite(Number(options.minTurnaroundMin))
    ? Math.min(Math.max(Math.round(Number(options.minTurnaroundMin)), 3), 120)
    : 10;
  const mode = normalizePlanMode(options.mode);
  const modePolicy = resolveModeWeights(mode);
  const lineCodesNormalized = Array.isArray(options.lineCodesNormalized) ? options.lineCodesNormalized : [];
  const maxDriveBlockMin = Number.isFinite(Number(options.maxDriveBlockMin))
    ? Math.min(Math.max(Math.round(Number(options.maxDriveBlockMin)), 0), 900)
    : 270;
  const minRestBetweenBlocksMin = Number.isFinite(Number(options.minRestBetweenBlocksMin))
    ? Math.min(Math.max(Math.round(Number(options.minRestBetweenBlocksMin)), 0), 240)
    : 45;
  const minUninterruptedBreakMin = Number.isFinite(Number(options.minUninterruptedBreakMin))
    ? Math.min(Math.max(Math.round(Number(options.minUninterruptedBreakMin)), 0), 240)
    : minRestBetweenBlocksMin;
  const euSplitBreak = parseBooleanLike(options.euSplitBreak, false);
  const splitBreakFirstSegmentMin = Number.isFinite(Number(options.splitBreakFirstSegmentMin))
    ? Math.min(Math.max(Math.round(Number(options.splitBreakFirstSegmentMin)), 1), 120)
    : 15;
  const splitBreakSecondSegmentMin = Number.isFinite(Number(options.splitBreakSecondSegmentMin))
    ? Math.min(Math.max(Math.round(Number(options.splitBreakSecondSegmentMin)), 1), 120)
    : 30;
  let operativeIdleResetMin = Number(options.operativeIdleResetMin);
  if (!Number.isFinite(operativeIdleResetMin)) operativeIdleResetMin = 0;
  operativeIdleResetMin = Math.min(Math.max(Math.round(operativeIdleResetMin), 0), 24 * 60);
  const plannerDriverOpts = {
    minTurnaroundMin,
    maxDriveBlockMin,
    minRestBetweenBlocksMin,
    minUninterruptedBreakMin,
    euSplitBreak,
    splitBreakFirstSegmentMin,
    splitBreakSecondSegmentMin,
    operativeIdleResetMin,
  };
  const feedRes = await resolveGtfsPlanningFeedKey(options.feedKey);
  if (!feedRes.ok) {
    const err = new Error(feedRes.message);
    err.statusCode = 400;
    throw err;
  }
  await ensureDepotTables();
  const depotsRes = await db.query(
    `SELECT id, depot_code, depot_name, lat, lng, capacity_total
     FROM depots
     WHERE is_active = TRUE
     ORDER BY depot_name ASC`
  );
  const depots = depotsRes.rows || [];
  const forcedBaseDepot = resolveForcedBaseDepot(depots, options);
  let fleetCap = await resolveFleetVehicleCap(options);
  if (forcedBaseDepot && parseBooleanLike(options.useBaseDepotCapacityAsCap, false)) {
    const capCt = Math.max(0, Math.round(Number(forcedBaseDepot.capacity_total || 0)));
    if (capCt > 0) {
      fleetCap = fleetCap == null ? capCt : Math.min(fleetCap, capCt);
    }
  }
  const candidates = await fetchGtfsOperationalServicesForDate(serviceDate, lineCodesNormalized, feedRes.feed_key);
  const chapasDiagnostics = !candidates.length
    ? await buildGtfsChapasZeroCandidatesDiagnostics(serviceDate, feedRes.feed_key, lineCodesNormalized)
    : null;
  const unassignedServices = [];
  const vehicles = [];
  let nextVehicleId = 1;
  const canOpenNewVehicle = () => fleetCap == null || vehicles.length < fleetCap;

  for (const svc of candidates) {
    let bestVehicle = null;
    let bestMeta = null;
    for (const vehicle of vehicles) {
      if (!vehicle?.services?.length) continue;
      const last = vehicle.services[vehicle.services.length - 1];
      if (!last) continue;
      if (!gtfsPlannerDriverRulesAllow(vehicle, svc, plannerDriverOpts)) continue;
      const meta = estimateCompatCost(last, svc, modePolicy);
      if (!bestMeta || meta.score < bestMeta.score) {
        bestMeta = meta;
        bestVehicle = vehicle;
      }
    }
    let chosen = bestVehicle;
    let meta = bestMeta;
    if (!chosen && canOpenNewVehicle()) {
      chosen = {
        vehicle_plan_id: `AUTO-${String(nextVehicleId).padStart(3, "0")}`,
        services: [],
        ...plannerInitDutyVehicleSkeleton(),
      };
      nextVehicleId += 1;
      vehicles.push(chosen);
      meta = { score: 0, wait_min: 0, deadhead_km: 0 };
    }
    if (!chosen) {
      unassignedServices.push({
        ...svc,
        unassigned_reason: fleetCap != null ? "cupo_viaturas_atribuicao_impossivel" : "sem_viatura_compative",
        unassigned_detail:
          fleetCap != null
            ? `Limite de ${fleetCap} viatura(s) atingido: não cabem mais serviços cumprindo turnaround e regras UE entre serviços da mesma viatura. Use «atribuir todos os serviços» ou aumente o cupo.`
            : "Sem viatura válida cumprindo turnaround e pausas (caso raro: contacte suporte).",
      });
      continue;
    }
    const prevSvc = chosen.services[chosen.services.length - 1];
    let waitMin = 0;
    let deadKm = 0;
    if (prevSvc && meta) {
      waitMin = Math.round(Number(meta.wait_min ?? 0));
      deadKm = Number((meta.deadhead_km ?? 0).toFixed(3));
    }
    chosen.services.push({
      ...svc,
      deadhead_from_previous_km: deadKm,
      waiting_from_previous_min: waitMin,
    });
    gtfsPlannerUpdateDriveAccumulator(chosen, svc, plannerDriverOpts);
  }

  let coverageOverflowVehicles = 0;
  if (parseBooleanLike(options.assignAllServices, false) && unassignedServices.length) {
    const spill = unassignedServices.splice(0, unassignedServices.length);
    coverageOverflowVehicles = spill.length;
    for (const u of spill) {
      const v = {
        vehicle_plan_id: `AUTO-${String(nextVehicleId).padStart(3, "0")}`,
        services: [],
        ...plannerInitDutyVehicleSkeleton(),
      };
      nextVehicleId += 1;
      vehicles.push(v);
      const { unassigned_reason: _ur, unassigned_detail: _ud, ...clean } = u;
      v.services.push({
        ...clean,
        deadhead_from_previous_km: 0,
        waiting_from_previous_min: 0,
        coverage_overflow: true,
      });
      gtfsPlannerUpdateDriveAccumulator(v, clean, plannerDriverOpts);
    }
  }

  const vehiclePlans = vehicles.map((vehicle) => {
    const assignedDepot = forcedBaseDepot
      ? computeDepotVehicleDeadheadSummary(forcedBaseDepot, vehicle)
      : chooseBestDepotForVehicle(vehicle, depots);
    const driveMin = vehicle.services.reduce((acc, s) => acc + Number(s.drive_min || 0), 0);
    const interServiceDeadhead = vehicle.services.reduce((acc, s) => acc + Number(s.deadhead_from_previous_km || 0), 0);
    return {
      vehicle_plan_id: vehicle.vehicle_plan_id,
      depot_id: assignedDepot?.depot_id || null,
      depot_name: assignedDepot?.depot_name || null,
      deadhead_to_first_km: assignedDepot?.deadhead_to_first_km ?? null,
      deadhead_back_to_depot_km: assignedDepot?.deadhead_back_to_depot_km ?? null,
      total_deadhead_km: Number((interServiceDeadhead + Number(assignedDepot?.total_deadhead_km || 0)).toFixed(3)),
      total_drive_min: Math.round(driveMin),
      services_count: vehicle.services.length,
      services: vehicle.services,
    };
  });

  const summary = vehiclePlans.reduce(
    (acc, plan) => {
      acc.vehicles_required += 1;
      acc.services_total += Number(plan.services_count || 0);
      acc.total_drive_min += Number(plan.total_drive_min || 0);
      acc.total_deadhead_km += Number(plan.total_deadhead_km || 0);
      if (!plan.depot_id) acc.vehicles_without_depot += 1;
      return acc;
    },
    {
      vehicles_required: 0,
      services_total: 0,
      total_drive_min: 0,
      total_deadhead_km: 0,
      vehicles_without_depot: 0,
    }
  );
  const dayWindow = candidates.length
    ? {
        note:
          "Construção das chapas por ordem cronológica das primeiras partidas GTFS do dia; vazio parque↔1.ª paragem usa coordenadas da primeira stop_time (sem Teltonika nesta fase).",
        earliest_start_min: Number(candidates[0].start_min),
        first_services_preview: candidates.slice(0, Math.min(12, candidates.length)).map((c) => ({
          trip_id: c.trip_id,
          line_code: c.line_code,
          first_departure_time: c.first_departure_time,
          start_stop_name: c.start_stop_name,
          start_lat: c.start_lat,
          start_lng: c.start_lng,
        })),
      }
    : null;

  const warnings = [];
  if (chapasDiagnostics?.hint_pt) {
    warnings.push(chapasDiagnostics.hint_pt);
  }
  if ((options.baseDepotId || String(options.baseDepotName || "").trim()) && !forcedBaseDepot) {
    warnings.push("Parque base (baseDepotId / baseDepotName) não corresponde a nenhum parque activo.");
  }
  if (forcedBaseDepot && candidates.some((c) => !Number.isFinite(Number(c.start_lat)) || !Number.isFinite(Number(c.start_lng)))) {
    warnings.push("Algumas trips sem coordenadas na primeira paragem GTFS: vazio parque↔origem pode ficar indisponível.");
  }
  if (maxDriveBlockMin > 0) {
    const longBlocks = candidates.filter((trip) => Number(trip.drive_min) > maxDriveBlockMin);
    if (longBlocks.length) {
      warnings.push(`${longBlocks.length} serviços excedem o limite ${maxDriveBlockMin} min (condução contínua do trip GTFS).`);
    }
  }
  if (coverageOverflowVehicles > 0) {
    const capTxt = fleetCap != null ? `${fleetCap} viaturas` : "cupo anterior";
    warnings.push(
      `Cobertura total: ${coverageOverflowVehicles} viatura(s) extra criada(s) para serviços que excediam ${capTxt}. Revise cupo (parque / máx. viaturas / Teltonika) se quiser reduzir frota.`
    );
  } else if (unassignedServices.length) {
    warnings.push(
      `${unassignedServices.length} serviço(s) por atribuir. Active «Atribuir todos os serviços» para forçar uma viatura por serviço em falta, ou aumente o cupo (desactive limite do parque, «Cupar pela frota Teltonika» se não houver rastreadores, ou indique um máximo de viaturas maior).`
    );
  }
  return {
    date: parseDateOnly(serviceDate),
    mode,
    min_turnaround_min: minTurnaroundMin,
    line_codes_filter: lineCodesNormalized,
    feed_key_used: feedRes.feed_key,
    feed_name_used: feedRes.feed_name,
    feed_auto_selected: feedRes.auto_selected,
    feed_selection_hint: feedRes.auto_selected
      ? "Modo automático: está a usar o feed GTFS activo mais recentemente actualizado. Com vários feeds importados, seleccione explicitamente o feed no selector «GTFS» do gerador de chapas se os serviços não corresponderem à operação."
      : null,
    chapas_diagnostics: chapasDiagnostics,
    fleet_cap_applied: fleetCap,
    assign_all_services: parseBooleanLike(options.assignAllServices, false),
    coverage_overflow_vehicles: coverageOverflowVehicles,
    base_depot: forcedBaseDepot
      ? {
          id: forcedBaseDepot.id,
          depot_name: forcedBaseDepot.depot_name,
          depot_code: forcedBaseDepot.depot_code ?? null,
          lat: forcedBaseDepot.lat,
          lng: forcedBaseDepot.lng,
          capacity_total: forcedBaseDepot.capacity_total,
        }
      : null,
    depot_capacity_used_as_fleet_cap: !!(forcedBaseDepot && parseBooleanLike(options.useBaseDepotCapacityAsCap, false)),
    day_schedule_anchor: dayWindow,
    phase_note:
      "Fase inicial: construção a partir das primeiras partidas GTFS do dia; vazio parque↔origem usa 1.ª stop_time da trip (Teltonika não necessário para este modelo).",
    fleet_cap_from_trackers: parseBooleanLike(options.fleetCapFromTrackers, false),
    planning_class_filter: options.planningClassFilter || "",
    constraints: {
      ...plannerDriverOpts,
      regulatory_reference_hint:
        "Alinhamento simplificado aos tempos obrigatórios de pausa após o limiar de condução contínua (Reg. UE 561/2006 tipo 4h30 + 45 min); viagens GTFS inteiras são atómicas (sem parte do trip).",
      policy_description_pt: plannerRestPolicyDescription(plannerDriverOpts),
    },
    warnings,
    unassigned_services: unassignedServices,
    summary: {
      ...summary,
      services_gtfs_candidates: candidates.length,
      services_assigned: summary.services_total,
      services_unassigned: unassignedServices.length,
      total_deadhead_km: Number(summary.total_deadhead_km.toFixed(3)),
      average_drive_min_per_vehicle: summary.vehicles_required
        ? Math.round(summary.total_drive_min / summary.vehicles_required)
        : 0,
    },
    vehicle_plans: vehiclePlans,
  };
}

async function buildGtfsServiceDiagnostics(fromDate, toDate, options = {}) {
  const days = enumerateDates(fromDate, toDate, 92);
  if (!days.length) {
    const err = new Error("Período inválido. Indique fromDate e toDate válidas.");
    err.statusCode = 400;
    throw err;
  }
  const feedRes = await resolveGtfsPlanningFeedKey(options.feedKey);
  if (!feedRes.ok) {
    const err = new Error(feedRes.message);
    err.statusCode = 400;
    throw err;
  }
  const lineCodesNormalized = Array.isArray(options.lineCodesNormalized) ? options.lineCodesNormalized : [];
  const daily_rows = [];
  const lineTotalsMap = new Map();
  for (const day of days) {
    // eslint-disable-next-line no-await-in-loop
    const services = await fetchGtfsOperationalServicesForDate(day, lineCodesNormalized, feedRes.feed_key);
    const byLine = new Map();
    services.forEach((svc) => {
      const lc = String(svc.line_code || "").trim() || "-";
      byLine.set(lc, (byLine.get(lc) || 0) + 1);
      lineTotalsMap.set(lc, (lineTotalsMap.get(lc) || 0) + 1);
    });
    const top_lines = [...byLine.entries()]
      .sort((a, b) => b[1] - a[1] || String(a[0]).localeCompare(String(b[0])))
      .slice(0, 8)
      .map(([line_code, services_count]) => ({ line_code, services_count }));
    daily_rows.push({
      date: day,
      services_count: services.length,
      unique_lines_count: byLine.size,
      top_lines,
    });
  }
  const line_totals = [...lineTotalsMap.entries()]
    .sort((a, b) => b[1] - a[1] || String(a[0]).localeCompare(String(b[0])))
    .slice(0, 20)
    .map(([line_code, services_count]) => ({ line_code, services_count }));
  const total_services = daily_rows.reduce((acc, r) => acc + Number(r.services_count || 0), 0);
  const days_with_services = daily_rows.filter((r) => Number(r.services_count || 0) > 0).length;
  return {
    from_date: fromDate,
    to_date: toDate,
    feed_key_used: feedRes.feed_key,
    feed_name_used: feedRes.feed_name,
    feed_auto_selected: feedRes.auto_selected,
    line_codes_filter: lineCodesNormalized,
    totals: {
      days: days.length,
      days_with_services,
      days_without_services: days.length - days_with_services,
      total_services,
      average_services_per_day: days.length ? Number((total_services / days.length).toFixed(2)) : 0,
      unique_lines_in_period: lineTotalsMap.size,
    },
    line_totals,
    daily_rows,
  };
}

router.get("/planning/gtfs-autonomous-chapas", async (req, res) => {
  try {
    const plannerOpts = parseGtfsChapasPlanningParams(req.query);
    const date =
      parseDateOnly(String(req.query.date || "").trim()) || parseDateOnly(new Date().toISOString().slice(0, 10));
    const plan = await buildAutonomousChapasPlan(date, plannerOpts);
    if (String(req.query.format || "").trim().toLowerCase() === "xlsx") {
      return respondGtfsChapasDailyXlsx(res, plannerOpts, date, plan);
    }
    return res.json(plan);
  } catch (error) {
    const code = Number(error?.statusCode);
    if (code === 400) {
      return res.status(400).json({ message: error.message || "Pedido inválido para chapas GTFS." });
    }
    return res.status(500).json({ message: "Erro ao gerar chapas automáticas por GTFS." });
  }
});

router.get("/planning/gtfs-services-diagnostics", async (req, res) => {
  try {
    const fromDate = parseDateOnly(String(req.query.fromDate || "").trim());
    const toDate = parseDateOnly(String(req.query.toDate || "").trim());
    const plannerOpts = parseGtfsChapasPlanningParams(req.query);
    const diag = await buildGtfsServiceDiagnostics(fromDate, toDate, plannerOpts);
    return res.json(diag);
  } catch (error) {
    const code = Number(error?.statusCode);
    if (code === 400) {
      return res.status(400).json({ message: error.message || "Parâmetros inválidos para diagnóstico GTFS." });
    }
    return res.status(500).json({ message: "Erro ao gerar diagnóstico de serviços GTFS." });
  }
});

router.get("/planning/gtfs-autonomous-chapas-range", async (req, res) => {
  try {
    const fromDate = parseDateOnly(String(req.query.fromDate || "").trim());
    const toDate = parseDateOnly(String(req.query.toDate || "").trim());
    const plannerOpts = parseGtfsChapasPlanningParams(req.query);
    const days = enumerateDates(fromDate, toDate, 92);
    if (!days.length) {
      return res.status(400).json({ message: "Período inválido. Indique fromDate e toDate válidas." });
    }
    if (String(req.query.format || "").trim().toLowerCase() === "xlsx" && days.length > 62) {
      return res.status(400).json({ message: "Exportação Excel limitada a 62 dias nesta versão. Reduza o período." });
    }
    const dailyPlansFull = [];
    for (const day of days) {
      // Serie diaria para KPIs mensais e exportação detalhada.
      // eslint-disable-next-line no-await-in-loop
      dailyPlansFull.push(await buildAutonomousChapasPlan(day, plannerOpts));
    }
    const totals = dailyPlansFull.reduce(
      (acc, p) => {
        acc.days += 1;
        acc.services_total += Number(p?.summary?.services_total || 0);
        acc.services_unassigned_total += Number(p?.summary?.services_unassigned || 0);
        acc.vehicle_days += Number(p?.summary?.vehicles_required || 0);
        acc.total_drive_min += Number(p?.summary?.total_drive_min || 0);
        acc.total_deadhead_km += Number(p?.summary?.total_deadhead_km || 0);
        return acc;
      },
      {
        days: 0,
        services_total: 0,
        services_unassigned_total: 0,
        vehicle_days: 0,
        total_drive_min: 0,
        total_deadhead_km: 0,
      }
    );
    const bundle = {
      from_date: fromDate,
      to_date: toDate,
      mode: plannerOpts.mode,
      min_turnaround_min: plannerOpts.minTurnaroundMin,
      feed_key_used: dailyPlansFull[0]?.feed_key_used ?? null,
      feed_name_used: dailyPlansFull[0]?.feed_name_used ?? null,
      feed_auto_selected: dailyPlansFull[0]?.feed_auto_selected ?? null,
      feed_selection_hint: dailyPlansFull[0]?.feed_selection_hint ?? null,
      chapas_diagnostics: dailyPlansFull[0]?.chapas_diagnostics ?? null,
      totals: {
        ...totals,
        total_deadhead_km: Number(totals.total_deadhead_km.toFixed(3)),
        average_vehicles_per_day: totals.days ? Number((totals.vehicle_days / totals.days).toFixed(2)) : 0,
      },
      daily_plans: dailyPlansFull.map((p) => ({
        date: p.date,
        summary: p.summary,
      })),
      detailed_daily_plans: dailyPlansFull,
    };

    if (String(req.query.format || "").trim().toLowerCase() === "xlsx") {
      return respondGtfsChapasRangeXlsx(res, plannerOpts, fromDate, toDate, bundle);
    }
    return res.json(bundle);
  } catch (error) {
    const code = Number(error?.statusCode);
    if (code === 400) {
      return res.status(400).json({ message: error.message || "Pedido inválido para chapas GTFS em período." });
    }
    return res.status(500).json({ message: "Erro ao gerar cenário GTFS para o período." });
  }
});

router.get("/depots/deadhead-estimate", async (req, res) => {
  try {
    const dateRaw = String(req.query.date || "").trim();
    const serviceDate = dateRaw && /^\d{4}-\d{2}-\d{2}$/.test(dateRaw) ? dateRaw : null;
    await ensureTrackerTables();
    await ensureDepotTables();
    await ensurePlannedServiceLocationColumns();
    const result = await db.query(
      `WITH roster_base AS (
         SELECT
           dr.service_date,
           td.imei,
           td.fleet_number,
           td.plate_number,
           td.depot_id,
           d.depot_name,
           d.lat AS depot_lat,
           d.lng AS depot_lng,
           ps.start_location,
           ps.end_location,
           ps.service_schedule,
           substring(COALESCE(ps.service_schedule, '') FROM '(\\d{1,2}:\\d{2})') AS hhmm_start,
           substring(COALESCE(ps.service_schedule, '') FROM '-\\s*(\\d{1,2}:\\d{2})') AS hhmm_end
         FROM daily_roster dr
         JOIN planned_services ps ON ps.id = dr.planned_service_id
         JOIN tracker_devices td
           ON (
             LOWER(TRIM(COALESCE(td.fleet_number, ''))) = LOWER(TRIM(COALESCE(ps.fleet_number, '')))
             OR LOWER(TRIM(COALESCE(td.plate_number, ''))) = LOWER(TRIM(COALESCE(ps.plate_number, '')))
           )
         LEFT JOIN depots d ON d.id = td.depot_id
         WHERE dr.service_date = COALESCE($1::date, CURRENT_DATE)
           AND td.is_active = TRUE
           AND COALESCE(ps.kms_carga, 0) > 0
       ),
       starts AS (
         SELECT DISTINCT ON (imei)
           imei, fleet_number, plate_number, depot_id, depot_name, depot_lat, depot_lng, service_date, start_location
         FROM roster_base
         ORDER BY imei, hhmm_start ASC NULLS LAST
       ),
       ends AS (
         SELECT DISTINCT ON (imei)
           imei, end_location
         FROM roster_base
         ORDER BY imei, hhmm_end DESC NULLS LAST
       )
       SELECT
         s.imei,
         s.fleet_number,
         s.plate_number,
         s.depot_id,
         s.depot_name,
         s.depot_lat,
         s.depot_lng,
         s.service_date,
         s.start_location,
         e.end_location,
         fp.first_lat,
         fp.first_lng,
         lp.last_lat,
         lp.last_lng
       FROM starts s
       LEFT JOIN ends e ON e.imei = s.imei
       LEFT JOIN LATERAL (
         SELECT sp.lat AS first_lat, sp.lng AS first_lng
         FROM services sv
         JOIN service_points sp ON sp.service_id = sv.id
         WHERE ((sv.started_at AT TIME ZONE 'Europe/Lisbon')::date) = s.service_date
           AND (
             LOWER(TRIM(COALESCE(sv.fleet_number, ''))) = LOWER(TRIM(COALESCE(s.fleet_number, '')))
             OR LOWER(TRIM(COALESCE(sv.plate_number, ''))) = LOWER(TRIM(COALESCE(s.plate_number, '')))
           )
         ORDER BY sp.captured_at ASC
         LIMIT 1
       ) fp ON TRUE
       LEFT JOIN LATERAL (
         SELECT sp.lat AS last_lat, sp.lng AS last_lng
         FROM services sv
         JOIN service_points sp ON sp.service_id = sv.id
         WHERE ((sv.started_at AT TIME ZONE 'Europe/Lisbon')::date) = s.service_date
           AND (
             LOWER(TRIM(COALESCE(sv.fleet_number, ''))) = LOWER(TRIM(COALESCE(s.fleet_number, '')))
             OR LOWER(TRIM(COALESCE(sv.plate_number, ''))) = LOWER(TRIM(COALESCE(s.plate_number, '')))
           )
         ORDER BY sp.captured_at DESC
         LIMIT 1
       ) lp ON TRUE
       ORDER BY s.fleet_number ASC NULLS LAST, s.imei ASC`,
      [serviceDate]
    );

    const rows = result.rows.map((row) => {
      const startDistanceKm = haversineDistanceKm(row.depot_lat, row.depot_lng, row.first_lat, row.first_lng);
      const endDistanceKm = haversineDistanceKm(row.last_lat, row.last_lng, row.depot_lat, row.depot_lng);
      return {
        ...row,
        deadhead_to_first_service_km: startDistanceKm == null ? null : Number(startDistanceKm.toFixed(3)),
        deadhead_back_to_depot_km: endDistanceKm == null ? null : Number(endDistanceKm.toFixed(3)),
      };
    });
    const totals = rows.reduce(
      (acc, row) => {
        acc.vehicles += 1;
        acc.estimated_deadhead_km += Number(row.deadhead_to_first_service_km || 0) + Number(row.deadhead_back_to_depot_km || 0);
        if (!row.depot_id) acc.vehicles_without_depot += 1;
        return acc;
      },
      { vehicles: 0, vehicles_without_depot: 0, estimated_deadhead_km: 0 }
    );

    return res.json({
      date: serviceDate || null,
      totals: {
        ...totals,
        estimated_deadhead_km: Number(totals.estimated_deadhead_km.toFixed(3)),
      },
      rows,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao calcular estimativa de vazio por parque." });
  }
});

router.get("/planning/vehicle-continuity", async (req, res) => {
  try {
    const dateRaw = String(req.query.date || "").trim();
    const serviceDate = dateRaw && /^\d{4}-\d{2}-\d{2}$/.test(dateRaw) ? dateRaw : null;
    const maxDriveRaw = Number(req.query.maxDriveMin);
    const maxDriveMin = Number.isFinite(maxDriveRaw) ? Math.min(Math.max(Math.round(maxDriveRaw), 60), 900) : 540;
    await ensurePlannedServiceLocationColumns();
    const result = await db.query(
      `SELECT
         dr.id AS roster_id,
         dr.service_date,
         ps.id AS planned_service_id,
         ps.service_code,
         ps.line_code,
         ps.fleet_number,
         ps.plate_number,
         ps.service_schedule,
         ps.start_location,
         ps.end_location,
         ps.kms_carga
       FROM daily_roster dr
       JOIN planned_services ps ON ps.id = dr.planned_service_id
       WHERE dr.service_date = COALESCE($1::date, CURRENT_DATE)
         AND COALESCE(ps.kms_carga, 0) > 0
       ORDER BY ps.line_code ASC NULLS LAST, ps.service_schedule ASC NULLS LAST`,
      [serviceDate]
    );

    const services = result.rows.map((row) => {
      const range = parseServiceScheduleRange(row.service_schedule);
      const driveMin = range ? Math.max(1, range.end - range.start) : 240;
      const startMin = range ? range.start : 9999;
      return {
        ...row,
        drive_min: driveMin,
        start_min: startMin,
      };
    });

    const byLine = new Map();
    const proposedVehicleMinutes = new Map();
    const currentVehicleMinutes = new Map();
    services.forEach((svc) => {
      const line = String(svc.line_code || "").trim() || "(sem linha)";
      if (!byLine.has(line)) byLine.set(line, []);
      byLine.get(line).push({ ...svc });
      const fleet = String(svc.fleet_number || "").trim() || "(sem frota)";
      currentVehicleMinutes.set(fleet, Number(currentVehicleMinutes.get(fleet) || 0) + Number(svc.drive_min || 0));
      proposedVehicleMinutes.set(fleet, Number(proposedVehicleMinutes.get(fleet) || 0) + Number(svc.drive_min || 0));
    });

    const linePlans = [];
    let totalCurrentTransfers = 0;
    let totalProposedTransfers = 0;
    let totalSuggestedReassignments = 0;

    for (const [lineCode, lineRowsRaw] of byLine.entries()) {
      const lineRows = [...lineRowsRaw].sort((a, b) => Number(a.start_min) - Number(b.start_min));
      const fleetDurationMap = new Map();
      lineRows.forEach((svc) => {
        const fleet = String(svc.fleet_number || "").trim() || "(sem frota)";
        fleetDurationMap.set(fleet, Number(fleetDurationMap.get(fleet) || 0) + Number(svc.drive_min || 0));
      });
      const dominantFleet = [...fleetDurationMap.entries()].sort((a, b) => b[1] - a[1])[0]?.[0] || "(sem frota)";

      let currentTransfers = 0;
      for (let i = 1; i < lineRows.length; i += 1) {
        if (String(lineRows[i - 1].fleet_number || "") !== String(lineRows[i].fleet_number || "")) currentTransfers += 1;
      }

      const proposedRows = lineRows.map((r) => ({ ...r, proposed_fleet_number: r.fleet_number }));
      const suggestedReassignments = [];
      for (const svc of proposedRows) {
        const fromFleet = String(svc.proposed_fleet_number || "").trim() || "(sem frota)";
        if (fromFleet === dominantFleet) continue;
        const duration = Number(svc.drive_min || 0);
        const dominantCurrent = Number(proposedVehicleMinutes.get(dominantFleet) || 0);
        if (dominantCurrent + duration > maxDriveMin) continue;
        proposedVehicleMinutes.set(dominantFleet, dominantCurrent + duration);
        proposedVehicleMinutes.set(fromFleet, Math.max(0, Number(proposedVehicleMinutes.get(fromFleet) || 0) - duration));
        svc.proposed_fleet_number = dominantFleet;
        suggestedReassignments.push({
          planned_service_id: svc.planned_service_id,
          service_code: svc.service_code,
          service_schedule: svc.service_schedule,
          from_fleet_number: fromFleet,
          to_fleet_number: dominantFleet,
          drive_min: duration,
        });
      }

      let proposedTransfers = 0;
      for (let i = 1; i < proposedRows.length; i += 1) {
        if (String(proposedRows[i - 1].proposed_fleet_number || "") !== String(proposedRows[i].proposed_fleet_number || "")) {
          proposedTransfers += 1;
        }
      }

      totalCurrentTransfers += currentTransfers;
      totalProposedTransfers += proposedTransfers;
      totalSuggestedReassignments += suggestedReassignments.length;
      linePlans.push({
        line_code: lineCode,
        dominant_fleet_number: dominantFleet,
        services_count: lineRows.length,
        current_transfers: currentTransfers,
        proposed_transfers: proposedTransfers,
        transfer_reduction: currentTransfers - proposedTransfers,
        suggested_reassignments: suggestedReassignments,
      });
    }

    linePlans.sort((a, b) => b.transfer_reduction - a.transfer_reduction);
    return res.json({
      date: serviceDate || null,
      max_drive_min_per_vehicle: maxDriveMin,
      summary: {
        lines: linePlans.length,
        current_transfers: totalCurrentTransfers,
        proposed_transfers: totalProposedTransfers,
        transfer_reduction: totalCurrentTransfers - totalProposedTransfers,
        suggested_reassignments: totalSuggestedReassignments,
      },
      line_plans: linePlans,
      vehicle_minutes: [...proposedVehicleMinutes.entries()]
        .map(([fleet_number, total_drive_min]) => ({ fleet_number, total_drive_min }))
        .sort((a, b) => b.total_drive_min - a.total_drive_min),
      current_vehicle_minutes: [...currentVehicleMinutes.entries()]
        .map(([fleet_number, total_drive_min]) => ({ fleet_number, total_drive_min }))
        .sort((a, b) => b.total_drive_min - a.total_drive_min),
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao gerar plano de continuidade por viatura." });
  }
});

router.post("/roster/:rosterId/start-service", async (req, res) => {
  const rosterId = Number(req.params.rosterId);
  if (!Number.isFinite(rosterId) || rosterId <= 0) {
    return res.status(400).json({ message: "Identificador de escala inválido." });
  }
  try {
    const rosterRes = await db.query(
      `SELECT
         dr.id AS roster_id,
         dr.driver_id,
         dr.service_date,
         dr.status AS roster_status,
         ps.id AS planned_service_id,
         ps.plate_number,
         ps.service_schedule,
         ps.line_code,
         ps.fleet_number
       FROM daily_roster dr
       JOIN planned_services ps ON ps.id = dr.planned_service_id
       WHERE dr.id = $1
       LIMIT 1`,
      [rosterId]
    );
    if (!rosterRes.rowCount) {
      return res.status(404).json({ message: "Linha de escala não encontrada." });
    }
    const row = rosterRes.rows[0];
    const rosterStatus = String(row.roster_status || "").trim().toLowerCase();
    if (!["assigned", "pending", "pendente", "atribuido", "atribuído"].includes(rosterStatus)) {
      return res.status(409).json({ message: "Só é possível iniciar automaticamente linhas por iniciar." });
    }

    const sameDayRes = await db.query(
      `SELECT 1
       FROM services s
       WHERE s.driver_id = $1
         AND LOWER(TRIM(s.status::text)) IN ('in_progress', 'awaiting_handover')
       LIMIT 1`,
      [row.driver_id]
    );
    if (sameDayRes.rowCount) {
      return res.status(409).json({ message: "O motorista já tem um serviço em execução." });
    }

    const alreadyStartedRes = await db.query(
      `SELECT 1
       FROM services s
       WHERE s.planned_service_id = $1
         AND s.driver_id = $2
         AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = $3::date
       LIMIT 1`,
      [row.planned_service_id, row.driver_id, row.service_date]
    );
    if (alreadyStartedRes.rowCount) {
      return res.status(409).json({ message: "Já existe um serviço iniciado na app para esta linha de escala." });
    }

    const gtfsTrip = await findBestTripForLine(row.line_code, row.service_schedule);
    const inserted = await db.query(
      `INSERT INTO services (
         driver_id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number, status
       )
       VALUES ($1, $2, $3, $4, $5, $6, $7, 'in_progress')
       RETURNING id, planned_service_id, gtfs_trip_id, plate_number, service_schedule, line_code, fleet_number, status, started_at`,
      [
        row.driver_id,
        row.planned_service_id,
        gtfsTrip?.trip_id || null,
        row.plate_number,
        row.service_schedule,
        row.line_code,
        row.fleet_number,
      ]
    );
    await db.query(
      `INSERT INTO service_segments (service_id, driver_id, fleet_number, status)
       VALUES ($1, $2, $3, 'in_progress')`,
      [inserted.rows[0].id, row.driver_id, row.fleet_number]
    );
    await db.query(`UPDATE daily_roster SET status = 'in_progress' WHERE id = $1`, [rosterId]);

    return res.status(201).json({
      message: "Serviço iniciado remotamente pelo supervisor.",
      service: inserted.rows[0],
      rosterId,
    });
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao iniciar serviço pela escala." });
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

async function computeOperationalPerformanceReport(query) {
  const { fromDate, toDate, lineCode, driverId } = query || {};
  const radiusMeters = Math.min(Math.max(Number(query?.radiusM) || 85, 40), 200);
  const maxServices = Math.min(Math.max(Number(query?.maxServices) || 120, 10), 400);
  const where = [];
  const values = [];
  let idx = 1;

  if (lineCode) {
    where.push(`s.line_code = $${idx}`);
    values.push(String(lineCode).trim());
    idx += 1;
  }
  if (driverId) {
    where.push(`s.driver_id = $${idx}`);
    values.push(Number(driverId));
    idx += 1;
  }
  const dayFilter = serviceActivityLisbonDayFilter(fromDate, toDate, idx);
  if (dayFilter) {
    where.push(dayFilter.sql);
    dayFilter.values.forEach((v) => values.push(v));
  }
  where.push(`s.started_at IS NOT NULL`);
  where.push(`s.status IN ('in_progress', 'completed', 'cancelled')`);
  const whereSql = where.length ? `WHERE ${where.join(" AND ")}` : "";

  const svcRes = await db.query(
    `SELECT
       s.id,
       s.gtfs_trip_id,
       s.line_code,
       s.service_schedule,
       s.fleet_number,
       s.started_at,
       s.ended_at,
       s.status,
       u.name AS driver_name
     FROM services s
     JOIN users u ON u.id = s.driver_id
     ${whereSql}
     ORDER BY s.started_at DESC
     LIMIT ${maxServices}`,
    values
  );
  const services = svcRes.rows;
  if (!services.length) {
    return {
      summary: {
        services_analyzed: 0,
        services_with_reference_stops: 0,
        total_stops_analyzed: 0,
        avg_delay_min: 0,
        delay_p90_min: 0,
        missed_stop_rate_pct: 0,
      },
      critical_stops: [],
      service_rank: [],
      ai_suggestions: ["Sem serviços no período selecionado."],
    };
  }

  const serviceIds = services.map((s) => Number(s.id)).filter((id) => Number.isFinite(id));
  const pointsRes = await db.query(
    `SELECT service_id, lat, lng, captured_at
     FROM service_points
     WHERE service_id = ANY($1::int[])
     ORDER BY service_id ASC, captured_at ASC`,
    [serviceIds]
  );
  const pointsByService = new Map();
  for (const p of pointsRes.rows) {
    if (!pointsByService.has(p.service_id)) pointsByService.set(p.service_id, []);
    pointsByService.get(p.service_id).push(p);
  }

  const stopsByTrip = new Map();
  const bestTripByHeader = new Map();
  const delays = [];
  const stopAgg = new Map();
  const serviceRank = [];
  let servicesWithStops = 0;
  let totalStops = 0;
  let totalMatched = 0;

  for (const svc of services) {
    let tripId = svc.gtfs_trip_id || null;
    if (!tripId) {
      const headerKey = `${String(svc.line_code || "").trim()}|${String(svc.service_schedule || "").trim()}`;
      if (!bestTripByHeader.has(headerKey)) {
        const best = await findBestTripForLine(svc.line_code, svc.service_schedule);
        bestTripByHeader.set(headerKey, best?.trip_id || null);
      }
      tripId = bestTripByHeader.get(headerKey);
    }
    if (!tripId) continue;

    if (!stopsByTrip.has(tripId)) {
      const stops = await getStopsByTripId(tripId);
      stopsByTrip.set(tripId, Array.isArray(stops) ? stops : []);
    }
    const stops = stopsByTrip.get(tripId) || [];
    if (!stops.length) continue;
    servicesWithStops += 1;

    const points = pointsByService.get(svc.id) || [];
    const matchedRows = matchGpsPointsToGtfsStops(stops, points, {
      radiusMeters,
      serviceStartedAt: svc.started_at,
    });
    totalStops += matchedRows.total;
    totalMatched += matchedRows.matched;

    const serviceDay = lisbonDayKey(svc.started_at);
    const serviceDelaySamples = [];
    for (const row of matchedRows.rows) {
      if (!row.passed_near_stop || !row.passed_at) continue;
      const scheduledRefMin = parseGtfsTimeToRelativeMinutes(row.scheduled_departure || row.scheduled_arrival);
      if (scheduledRefMin == null) continue;

      const passMin = lisbonMinutesOfDay(row.passed_at);
      const passDay = lisbonDayKey(row.passed_at);
      const passDayOffset = diffDays(passDay, serviceDay);
      if (passMin == null || passDayOffset == null) continue;
      const passRelativeMin = passDayOffset * 1440 + passMin;
      const delayMin = passRelativeMin - scheduledRefMin;
      if (!Number.isFinite(delayMin)) continue;
      delays.push(delayMin);
      serviceDelaySamples.push(delayMin);

      const key = `${row.stop_id || ""}|${row.stop_name || ""}`;
      if (!stopAgg.has(key)) {
        stopAgg.set(key, {
          stop_id: row.stop_id || "",
          stop_name: row.stop_name || "Paragem sem nome",
          samples: 0,
          delayed_samples: 0,
          severe_samples: 0,
          total_delay_min: 0,
        });
      }
      const acc = stopAgg.get(key);
      acc.samples += 1;
      acc.total_delay_min += delayMin;
      if (delayMin > 3) acc.delayed_samples += 1;
      if (delayMin > 8) acc.severe_samples += 1;
    }

    const avgDelay = serviceDelaySamples.length
      ? serviceDelaySamples.reduce((sum, val) => sum + val, 0) / serviceDelaySamples.length
      : null;
    const maxDelay = serviceDelaySamples.length ? Math.max(...serviceDelaySamples) : null;
    serviceRank.push({
      service_id: svc.id,
      line_code: svc.line_code,
      driver_name: svc.driver_name,
      fleet_number: svc.fleet_number,
      status: svc.status,
      started_at: svc.started_at,
      ended_at: svc.ended_at,
      stops_total: matchedRows.total,
      stops_matched: matchedRows.matched,
      avg_delay_min: avgDelay == null ? null : Math.round(avgDelay * 10) / 10,
      max_delay_min: maxDelay == null ? null : Math.round(maxDelay * 10) / 10,
    });
  }

  const sortedDelays = [...delays].sort((a, b) => a - b);
  const avgDelay = sortedDelays.length ? sortedDelays.reduce((sum, val) => sum + val, 0) / sortedDelays.length : 0;
  const p90Delay = sortedDelays.length ? sortedDelays[Math.floor((sortedDelays.length - 1) * 0.9)] : 0;

  const criticalStops = [...stopAgg.values()]
    .map((s) => {
      const av = s.samples ? s.total_delay_min / s.samples : 0;
      const delayedPct = s.samples ? (s.delayed_samples / s.samples) * 100 : 0;
      const severePct = s.samples ? (s.severe_samples / s.samples) * 100 : 0;
      const score = av * 0.65 + delayedPct * 0.2 + severePct * 0.15;
      return {
        stop_id: s.stop_id,
        stop_name: s.stop_name,
        samples: s.samples,
        avg_delay_min: Math.round(av * 10) / 10,
        delayed_rate_pct: Math.round(delayedPct * 10) / 10,
        severe_delay_rate_pct: Math.round(severePct * 10) / 10,
        criticality_score: Math.round(score * 10) / 10,
      };
    })
    .sort((a, b) => b.criticality_score - a.criticality_score)
    .slice(0, 20);

  const missedStopRate = totalStops ? ((totalStops - totalMatched) / totalStops) * 100 : 0;
  const summary = {
    services_analyzed: services.length,
    services_with_reference_stops: servicesWithStops,
    total_stops_analyzed: totalStops,
    avg_delay_min: Math.round(avgDelay * 10) / 10,
    delay_p90_min: Math.round(Number(p90Delay || 0) * 10) / 10,
    missed_stop_rate_pct: Math.round(missedStopRate * 10) / 10,
  };

  return {
    summary,
    critical_stops: criticalStops,
    service_rank: serviceRank
      .sort((a, b) => Number(b.avg_delay_min || -999) - Number(a.avg_delay_min || -999))
      .slice(0, 30),
    ai_suggestions: buildOperationalSuggestions(summary, criticalStops),
  };
}

router.get("/reports/performance", async (req, res) => {
  try {
    const data = await computeOperationalPerformanceReport(req.query);
    return res.json(data);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao gerar relatório de desempenho operacional." });
  }
});

router.get("/reports/performance.xlsx", async (req, res) => {
  try {
    const data = await computeOperationalPerformanceReport(req.query);
    const wb = XLSX.utils.book_new();
    const summarySheet = XLSX.utils.json_to_sheet([data.summary || {}]);
    XLSX.utils.book_append_sheet(wb, summarySheet, "Resumo");
    const crit = Array.isArray(data.critical_stops) ? data.critical_stops : [];
    const critSheet = XLSX.utils.json_to_sheet(crit.length ? crit : [{ info: "Sem paragens críticas no período." }]);
    XLSX.utils.book_append_sheet(wb, critSheet, "Paragens criticas");
    const rank = Array.isArray(data.service_rank) ? data.service_rank : [];
    const rankSheet = XLSX.utils.json_to_sheet(rank.length ? rank : [{ info: "Sem ranking no período." }]);
    XLSX.utils.book_append_sheet(wb, rankSheet, "Servicos atraso");
    const sugg = Array.isArray(data.ai_suggestions) ? data.ai_suggestions : [];
    const suggRows = sugg.map((text, i) => ({ ordem: i + 1, sugestao: text }));
    const suggSheet = XLSX.utils.json_to_sheet(suggRows.length ? suggRows : [{ sugestao: "Sem sugestões." }]);
    XLSX.utils.book_append_sheet(wb, suggSheet, "Sugestoes IA");
    const buf = XLSX.write(wb, { bookType: "xlsx", type: "buffer" });
    const from = String(req.query.fromDate || "").trim() || "inicio";
    const to = String(req.query.toDate || "").trim() || "fim";
    const fileName = `relatorio-ia_${from}_a_${to}.xlsx`;
    res.setHeader("Content-Disposition", `attachment; filename="${fileName}"`);
    res.setHeader("Content-Type", "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet");
    return res.status(200).send(buf);
  } catch (_error) {
    return res.status(500).json({ message: "Erro ao exportar relatório operacional (Excel)." });
  }
});

router.get("/services/:serviceId/details", async (req, res) => {
  const { serviceId } = req.params;
  try {
    const selectCloseMode = (await hasServiceCloseModeColumn()) ? "s.close_mode" : "NULL::text AS close_mode";
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
         ${selectCloseMode},
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
    await ensurePlannedServiceLocationColumns();
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
    const rowsWithMetrics = await enrichServiceRowsForExportSafe(result.rows);

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
    await ensurePlannedServiceLocationColumns();
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
    const rowsWithMetrics = await enrichServiceRowsForExportSafe(result.rows);

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
