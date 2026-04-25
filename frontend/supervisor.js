function resolveApiBase() {
  const configured = window.sessionStorage.getItem("api_base");
  if (configured) return configured;
  if (window.location.port === "4000") return window.location.origin;
  if (window.location.hostname === "localhost" || window.location.hostname === "127.0.0.1") {
    return `${window.location.protocol}//${window.location.hostname}:4000`;
  }
  return window.location.origin;
}

const API_BASE = resolveApiBase();

/** Data civil actual em Lisboa (YYYY-MM-DD), alinhada ao resumo da API. */
function todayISOInLisbon() {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Lisbon" }).format(new Date());
}

/** Valores booleanos vindos do PostgreSQL / JSON (incl. "t"/"f"). */
function asPgBool(value) {
  if (value === true || value === 1) return true;
  if (value === false || value === 0) return false;
  const s = String(value ?? "").trim().toLowerCase();
  if (s === "t" || s === "true" || s === "1") return true;
  if (s === "f" || s === "false" || s === "0" || s === "") return false;
  return false;
}

/** Linha da escala: pendente/atribuído permitem reatribuir (prioridade sobre flags inconsistentes da API). */
function rosterRowCanReassign(row) {
  const st = String(row.roster_status ?? "").trim().toLowerCase();
  if (["pending", "assigned", "pendente", "atribuido", "atribuído"].includes(st)) return true;

  const reason = String(row.reassign_blocked_reason ?? row.reassignBlockedReason ?? "").trim() || null;
  if (reason === "estado_escala" || reason === "execucao_app") return false;

  const rawCan = row.can_reassign ?? row.canReassign;
  if (rawCan !== undefined && rawCan !== null) return asPgBool(rawCan);
  return false;
}

function rosterRowBlockReason(row) {
  const r = row.reassign_blocked_reason ?? row.reassignBlockedReason;
  return String(r ?? "").trim() || null;
}

let supToken = "";
let activeQueryString = "";
let filterMode = "origin";
let currentServicesCache = [];
let selectedService = null;
const AUTH_SESSION_KEY = "auth_session";

const overviewEl = document.getElementById("overview");
const serviceCardsEl = document.getElementById("supServiceCards");
const driversListEl = document.getElementById("driversList");
const sessionWelcomeEl = document.getElementById("sessionWelcome");
const logoutBtnEl = document.getElementById("logoutBtn");
const loginCardEl = document.getElementById("supLoginCard");
const importDriversReportEl = document.getElementById("importDriversReport");
const servicesPieEl = document.getElementById("servicesPie");
const servicesStatsTextEl = document.getElementById("servicesStatsText");
const blocksPieEl = document.getElementById("blocksPie");
const blocksStatsTextEl = document.getElementById("blocksStatsText");
const servicesDailySummaryEl = document.getElementById("servicesDailySummary");
const importRosterReportEl = document.getElementById("importRosterReport");
const importGtfsReportEl = document.getElementById("importGtfsReport");
const serviceDrawerEl = document.getElementById("serviceDrawer");
const serviceDrawerBackdropEl = document.getElementById("serviceDrawerBackdrop");
const serviceDrawerInfoEl = document.getElementById("serviceDrawerInfo");
const drawerTitleEl = document.getElementById("drawerTitle");
const serviceRouteMiniMapEl = document.getElementById("serviceRouteMiniMap");
const serviceHandoversListEl = document.getElementById("serviceHandoversList");
const forceEndServiceBtn = document.getElementById("forceEndServiceBtn");
const transferServiceBtn = document.getElementById("transferServiceBtn");
const cancelServiceBtn = document.getElementById("cancelServiceBtn");
const serviceRouteTimesEl = document.getElementById("serviceRouteTimes");
const conflictAlertsListEl = document.getElementById("conflictAlertsList");
const supLiveMapEl = document.getElementById("supLiveMap");
const supLiveServicesListEl = document.getElementById("supLiveServicesList");
const liveServiceFilterSelectEl = document.getElementById("liveServiceFilterSelect");
const deadheadMovementsListEl = document.getElementById("deadheadMovementsList");
const deadheadMapEl = document.getElementById("deadheadMap");
const deadheadFromDateEl = document.getElementById("deadheadFromDate");
const deadheadToDateEl = document.getElementById("deadheadToDate");
const deadheadFleetFilterEl = document.getElementById("deadheadFleetFilter");
const trackerDevicesListEl = document.getElementById("trackerDevicesList");
const trackerDeviceFormEl = document.getElementById("trackerDeviceForm");
const trackerWebhookUrlPreviewEl = document.getElementById("trackerWebhookUrlPreview");
const trackerWebhookHeadersPreviewEl = document.getElementById("trackerWebhookHeadersPreview");
const trackerWebhookPayloadPreviewEl = document.getElementById("trackerWebhookPayloadPreview");
const trackerWebhookBatchPayloadPreviewEl = document.getElementById("trackerWebhookBatchPayloadPreview");
const reportSummaryCardsEl = document.getElementById("reportSummaryCards");
const reportAiSuggestionsEl = document.getElementById("reportAiSuggestions");
const reportCriticalStopsBodyEl = document.getElementById("reportCriticalStopsBody");
const reportServiceRankBodyEl = document.getElementById("reportServiceRankBody");
const opsThreadsListEl = document.getElementById("opsThreadsList");
const opsMessagesListEl = document.getElementById("opsMessagesList");
const opsConversationTitleEl = document.getElementById("opsConversationTitle");
const supMessageFormEl = document.getElementById("supMessageForm");
const supMessagePresetEl = document.getElementById("supMessagePreset");
const supMessageTextEl = document.getElementById("supMessageText");
const supMessageTrafficAlertEl = document.getElementById("supMessageTrafficAlert");
const supMessageRelatedServiceIdEl = document.getElementById("supMessageRelatedServiceId");
const refreshOpsThreadsBtnEl = document.getElementById("refreshOpsThreadsBtn");
const refreshOpsMessagesBtnEl = document.getElementById("refreshOpsMessagesBtn");
const supAlertSoundTypeEl = document.getElementById("supAlertSoundType");
const supAlertSoundVolumeEl = document.getElementById("supAlertSoundVolume");
const testSupAlertSoundBtnEl = document.getElementById("testSupAlertSoundBtn");
const supPresetFormEl = document.getElementById("supPresetForm");
const supPresetScopeEl = document.getElementById("supPresetScope");
const supPresetCodeEl = document.getElementById("supPresetCode");
const supPresetLabelEl = document.getElementById("supPresetLabel");
const supPresetDefaultTextEl = document.getElementById("supPresetDefaultText");
const supPresetIsActiveEl = document.getElementById("supPresetIsActive");
const supPresetListEl = document.getElementById("supPresetList");
const supPresetListScopeEl = document.getElementById("supPresetListScope");
const refreshSupPresetListBtnEl = document.getElementById("refreshSupPresetListBtn");
const gtfsEditorRouteSelectEl = document.getElementById("gtfsEditorRouteSelect");
const gtfsEditorFeedSelectEl = document.getElementById("gtfsEditorFeedSelect");
const gtfsEditorTripSelectEl = document.getElementById("gtfsEditorTripSelect");
const gtfsEditorSummaryEl = document.getElementById("gtfsEditorSummary");
const gtfsEditorStopsListEl = document.getElementById("gtfsEditorStopsList");
const gtfsEditorApplyScopeEl = document.getElementById("gtfsEditorApplyScope");
const gtfsAnalyticsFeedSelectEl = document.getElementById("gtfsAnalyticsFeedSelect");
const gtfsAnalyticsSummaryEl = document.getElementById("gtfsAnalyticsSummary");
const gtfsAnalyticsTableBodyEl = document.getElementById("gtfsAnalyticsTableBody");
const gtfsAnalyticsLineSelectEl = document.getElementById("gtfsAnalyticsLineSelect");
const gtfsAnalyticsTripSelectEl = document.getElementById("gtfsAnalyticsTripSelect");
const gtfsLineDetailSummaryEl = document.getElementById("gtfsLineDetailSummary");
const gtfsAnalyticsMapEl = document.getElementById("gtfsAnalyticsMap");
const gtfsRtPreviewReportEl = document.getElementById("gtfsRtPreviewReport");
const gtfsEffectiveFromEl = document.getElementById("gtfsEffectiveFrom");
const calendarEffectiveFromEl = document.getElementById("calendarEffectiveFrom");
const gtfsCalendarsListEl = document.getElementById("gtfsCalendarsList");
const gtfsFeedKeyEl = document.getElementById("gtfsFeedKey");
const gtfsFeedNameEl = document.getElementById("gtfsFeedName");
const gtfsReplaceFeedEl = document.getElementById("gtfsReplaceFeed");
const gtfsFeedsListEl = document.getElementById("gtfsFeedsList");
const vehicleRegistryFormEl = document.getElementById("vehicleRegistryForm");
const vehicleRegistryListEl = document.getElementById("vehicleRegistryList");
const odometerReconciliationListEl = document.getElementById("odometerReconciliationList");

let driversCache = [];
let usersCache = [];
let trackerDevicesCache = [];
let rosterDayCache = [];
let rosterDaySelectedDriverId = null;
let supLiveMap = null;
let supLiveMarkersLayer = null;
let supLiveRoutesLayer = null;
let supLiveRouteByServiceId = new Map();
let supLiveHighlightedServiceId = null;
let deadheadMap = null;
let deadheadRouteLayer = null;
let supLiveRefreshInterval = null;
let currentLiveServices = [];
let serviceDetailRouteMap = null;
let serviceDetailRouteLayer = null;
const LIVE_MAP_REFRESH_MS = 5000;
let supLiveMapUserAdjustedView = false;
let reportsLoadedOnce = false;
let selectedOpsDriverId = null;
let currentSupervisorUserId = null;
let selectedGtfsFeedKey = "";
let selectedGtfsAnalyticsFeedKey = "";
let gtfsAnalyticsRowsCache = [];
let gtfsLineTripsCache = [];
let gtfsAnalyticsMap = null;
let gtfsAnalyticsRouteLayer = null;
let gtfsAnalyticsSelectedRouteId = "";
let supervisorAlertsPollTimer = null;
let supervisorAlertBaselineReady = false;
let lastSupervisorAlertSignature = "";
let liveRealtimeStatusFilter = "all";
const TAB_MODULE_MAP = {
  tabResumo: "dashboard",
  tabTempoReal: "operacao",
  tabServicos: "operacao",
  tabEscalaDia: "planeamento",
  tabAlertasConflito: "operacao",
  tabCriarMotorista: "administracao",
  tabCriarAcesso: "administracao",
  tabImportarMotoristas: "dados",
  tabEditorGtfs: "dados",
  tabAnaliseGtfs: "dados",
  tabExportarMotoristas: "dados",
  tabListaMotoristas: "administracao",
  tabViaturas: "dados",
  tabIntegracoesGps: "dados",
  tabMensagensComunicacao: "comunicacao",
  tabMensagensPresets: "comunicacao",
  tabParagensServico: "operacao",
  tabRelatoriosIa: "dados",
};

function labelEstadoExecucaoServicoPt(code) {
  const k = String(code || "").toLowerCase();
  const map = {
    pending: "Pendente",
    in_progress: "Em curso",
    awaiting_handover: "Aguarda transferência",
    completed: "Concluído",
    cancelled: "Cancelado",
  };
  return map[k] || code || "—";
}

function labelCloseModePt(closeMode) {
  const k = String(closeMode || "").trim().toLowerCase();
  if (k === "auto_last_stop") return "Auto (última paragem)";
  if (k === "manual") return "Manual";
  return "—";
}

function closeModeBadgeClass(closeMode) {
  const k = String(closeMode || "").trim().toLowerCase();
  if (k === "auto_last_stop") return "status-completed";
  if (k === "manual") return "status-other";
  return "status-other";
}

function labelEstadoEscalaPt(code) {
  const k = String(code || "").toLowerCase();
  const map = {
    pending: "Pendente",
    assigned: "Escalado (por iniciar)",
    in_progress: "Viagem em curso",
    completed: "Concluído",
  };
  return map[k] || code || "—";
}

function labelEstadoTransferenciaPt(code) {
  const k = String(code || "").toLowerCase();
  const map = {
    pending: "Pendente",
    completed: "Concluída",
    cancelled: "Cancelada",
  };
  return map[k] || code || "—";
}

function labelPerfilAcessoPt(role) {
  const k = String(role || "").toLowerCase();
  const map = {
    viewer: "visualização",
    supervisor: "supervisor",
    admin: "administrador",
    driver: "motorista",
  };
  return map[k] || role || "—";
}

function getAuthHeaders() {
  return {
    "Content-Type": "application/json",
    Authorization: `Bearer ${supToken}`,
  };
}

const SUP_ALERT_SOUND_SETTINGS_KEY = "sup_alert_sound_settings_v1";

function loadSupAlertSoundSettings() {
  try {
    const raw = localStorage.getItem(SUP_ALERT_SOUND_SETTINGS_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return {
      type: parsed?.type || "beep",
      volume: Number.isFinite(Number(parsed?.volume)) ? Number(parsed.volume) : 70,
    };
  } catch (_error) {
    return { type: "beep", volume: 70 };
  }
}

function saveSupAlertSoundSettings(settings) {
  localStorage.setItem(SUP_ALERT_SOUND_SETTINGS_KEY, JSON.stringify(settings));
}

function getSupAlertSoundSettings() {
  return {
    type: String(supAlertSoundTypeEl?.value || "beep"),
    volume: Number(supAlertSoundVolumeEl?.value || 70),
  };
}

function playSupervisorTone(ctx, startAt, frequency, duration, volume, waveType) {
  const osc = ctx.createOscillator();
  const gain = ctx.createGain();
  osc.type = waveType;
  osc.frequency.value = frequency;
  gain.gain.setValueAtTime(volume, startAt);
  gain.gain.exponentialRampToValueAtTime(0.0001, startAt + duration);
  osc.connect(gain);
  gain.connect(ctx.destination);
  osc.start(startAt);
  osc.stop(startAt + duration);
}

function playSupervisorAlertSound() {
  const settings = getSupAlertSoundSettings();
  const AudioCtx = window.AudioContext || window.webkitAudioContext;
  if (!AudioCtx) return;
  const ctx = new AudioCtx();
  const now = ctx.currentTime;
  const volume = Math.max(0, Math.min(1, settings.volume / 100)) * 0.25;
  if (settings.type === "urgent") {
    playSupervisorTone(ctx, now, 820, 0.11, volume, "sawtooth");
    playSupervisorTone(ctx, now + 0.16, 740, 0.11, volume, "sawtooth");
    playSupervisorTone(ctx, now + 0.32, 920, 0.13, volume, "sawtooth");
  } else if (settings.type === "chime") {
    playSupervisorTone(ctx, now, 660, 0.12, volume, "triangle");
    playSupervisorTone(ctx, now + 0.15, 880, 0.18, volume, "triangle");
  } else {
    playSupervisorTone(ctx, now, 880, 0.15, volume, "square");
  }
}

function applySupAlertSoundSettingsToUi() {
  const settings = loadSupAlertSoundSettings();
  if (supAlertSoundTypeEl) supAlertSoundTypeEl.value = settings.type;
  if (supAlertSoundVolumeEl) supAlertSoundVolumeEl.value = String(settings.volume);
}

function updateSupervisorAlertSignature() {
  if (!supToken) return;
  Promise.allSettled([
    fetch(`${API_BASE}/supervisor/messages/threads`, { headers: getAuthHeaders() }).then((r) =>
      r.json().then((d) => ({ ok: r.ok, d }))
    ),
    fetch(`${API_BASE}/supervisor/handover-alerts`, { headers: getAuthHeaders() }).then((r) =>
      r.json().then((d) => ({ ok: r.ok, d }))
    ),
  ]).then((results) => {
    const threads = results[0]?.status === "fulfilled" && results[0].value.ok ? results[0].value.d : [];
    const handovers = results[1]?.status === "fulfilled" && results[1].value.ok ? results[1].value.d : [];
    const unreadByDriver = (Array.isArray(threads) ? threads : [])
      .filter((t) => Number(t.unread_from_driver || 0) > 0)
      .map((t) => `${t.driver_id}:${t.unread_from_driver}`)
      .sort();
    const pendingHandovers = (Array.isArray(handovers) ? handovers : []).map((h) => h.id).sort((a, b) => a - b);
    const signature = JSON.stringify({ unreadByDriver, pendingHandovers });
    if (supervisorAlertBaselineReady && signature !== lastSupervisorAlertSignature) {
      playSupervisorAlertSound();
    }
    lastSupervisorAlertSignature = signature;
    if (!supervisorAlertBaselineReady) supervisorAlertBaselineReady = true;
  });
}

function startSupervisorAlertsPolling() {
  stopSupervisorAlertsPolling();
  updateSupervisorAlertSignature();
  supervisorAlertsPollTimer = setInterval(() => {
    updateSupervisorAlertSignature();
  }, 15_000);
}

function stopSupervisorAlertsPolling() {
  if (supervisorAlertsPollTimer) {
    clearInterval(supervisorAlertsPollTimer);
    supervisorAlertsPollTimer = null;
  }
}

function applySessionAndLoad(user) {
  currentSupervisorUserId = Number(user?.id) || null;
  sessionWelcomeEl.textContent = `Bem-vindo ${user.username || user.name || "utilizador"}`;
  sessionWelcomeEl.classList.remove("hidden");
  logoutBtnEl.classList.remove("hidden");
  loginCardEl.classList.add("hidden");
  document.getElementById("supervisorTabs").classList.remove("hidden");
}

function readRoleFromJwt(token) {
  try {
    const payloadPart = String(token || "").split(".")[1];
    if (!payloadPart) return "";
    const normalizedBase64 = payloadPart.replace(/-/g, "+").replace(/_/g, "/");
    const json = atob(normalizedBase64);
    const payload = JSON.parse(json);
    return String(payload?.role || "").trim().toLowerCase();
  } catch (_error) {
    return "";
  }
}

function buildQueryString() {
  const params = new URLSearchParams();
  const driverId = document.getElementById("fDriverId").value.trim();
  const lineCode = document.getElementById("fLineCode").value.trim();
  const status = document.getElementById("fStatus").value.trim();
  const onlyCancelled = document.getElementById("fOnlyCancelled")?.checked;
  const fromDate = document.getElementById("fFromDate").value;
  const toDate = document.getElementById("fToDate").value;

  if (driverId) params.set("driverId", driverId);
  if (lineCode) params.set("lineCode", lineCode);
  if (onlyCancelled) {
    params.set("status", "cancelled");
  } else if (status) {
    params.set("status", status);
  }
  if (fromDate) params.set("fromDate", fromDate);
  if (toDate) params.set("toDate", toDate);
  if (!fromDate && !toDate) {
    const today = todayISOInLisbon();
    params.set("fromDate", today);
    params.set("toDate", today);
  }

  const serviceIdText = document.getElementById("fServiceId").value.trim();
  if (serviceIdText) params.set("serviceId", serviceIdText);

  const query = params.toString();
  return query ? `?${query}` : "";
}

function formatDate(value) {
  if (!value) return "-";
  return new Date(value).toLocaleTimeString();
}

function formatDateOnly(value) {
  if (!value) return "-";
  return new Date(value).toLocaleDateString();
}

function formatNumberPt(value, digits = 1) {
  const n = Number(value);
  if (!Number.isFinite(n)) return "-";
  return n.toLocaleString("pt-PT", {
    minimumFractionDigits: digits,
    maximumFractionDigits: digits,
  });
}

function formatDateTimePt(iso) {
  if (!iso) return "—";
  try {
    return new Date(iso).toLocaleString("pt-PT", { timeZone: "Europe/Lisbon" });
  } catch (_e) {
    return "—";
  }
}

function getStatusClass(status) {
  if (status === "completed") return "status-completed";
  if (status === "in_progress") return "status-in_progress";
  if (status === "awaiting_handover") return "status-awaiting_handover";
  return "status-other";
}

function computeDelayMinutes(service) {
  if (!service.started_at) return 0;
  const schedule = String(service.service_schedule || "");
  const parts = schedule.split("-");
  if (parts.length !== 2 || !parts[1].includes(":")) return 0;
  const [hh, mm] = parts[1].split(":");
  const expectedEnd = new Date(service.started_at);
  expectedEnd.setHours(Number(hh), Number(mm), 0, 0);
  const actual = service.ended_at ? new Date(service.ended_at) : new Date();
  const diffMs = actual - expectedEnd;
  return Math.round(diffMs / 60000);
}

function getDelayClass(delayMinutes) {
  if (delayMinutes <= 0) return "delay-ok";
  if (delayMinutes <= 10) return "delay-warn";
  return "delay-bad";
}

function formatDelay(delayMinutes) {
  if (delayMinutes === 0) return "00:00";
  const sign = delayMinutes > 0 ? "+" : "-";
  const abs = Math.abs(delayMinutes);
  const h = String(Math.floor(abs / 60)).padStart(2, "0");
  const m = String(abs % 60).padStart(2, "0");
  return `${sign}${h}:${m}`;
}

function renderServicesDailySummary(services) {
  if (!servicesDailySummaryEl) return;
  if (!Array.isArray(services) || !services.length) {
    servicesDailySummaryEl.textContent = "Sem dados.";
    return;
  }

  const byDay = new Map();
  services.forEach((s) => {
    const dayKey = String(s.execution_day || "-");
    if (!byDay.has(dayKey)) {
      byDay.set(dayKey, {
        totalServices: 0,
        plannedKm: 0,
        realizedKm: 0,
      });
    }
    const acc = byDay.get(dayKey);
    acc.totalServices += 1;
    acc.plannedKm += Number(s.planned_km_line || 0);
    acc.realizedKm += Number(s.realized_km_service || 0);
  });

  const lines = [];
  const sortedDays = [...byDay.keys()].sort((a, b) => String(a).localeCompare(String(b)));
  sortedDays.forEach((day) => {
    const d = byDay.get(day);
    lines.push(
      `${day} | Serviços: ${d.totalServices} | Km programados: ${d.plannedKm.toFixed(3)} | Km realizados: ${d.realizedKm.toFixed(3)}`
    );
  });

  const totalServices = services.length;
  const totalPlanned = services.reduce((sum, s) => sum + Number(s.planned_km_line || 0), 0);
  const totalRealized = services.reduce((sum, s) => sum + Number(s.realized_km_service || 0), 0);
  lines.push("---");
  lines.push(
    `TOTAL | Serviços: ${totalServices} | Km programados: ${totalPlanned.toFixed(3)} | Km realizados: ${totalRealized.toFixed(3)}`
  );
  servicesDailySummaryEl.textContent = lines.join("\n");
}

function buildPieBackground(parts) {
  const total = parts.reduce((sum, p) => sum + p.value, 0) || 1;
  let start = 0;
  const slices = parts.map((p) => {
    const pct = (p.value / total) * 100;
    const end = start + pct;
    const slice = `${p.color} ${start.toFixed(2)}% ${end.toFixed(2)}%`;
    start = end;
    return slice;
  });
  return `conic-gradient(${slices.join(", ")})`;
}

/** Pré-preenche o ID em «Paragens (serviço)» e muda para esse separador. */
function openStopPassagesTabForService(serviceId) {
  const id = Number(serviceId);
  if (!Number.isFinite(id) || id <= 0) return;
  const idInput = document.getElementById("stopPassageServiceId");
  if (idInput) idInput.value = String(id);
  const summaryEl = document.getElementById("stopPassageSummary");
  if (summaryEl) {
    summaryEl.textContent = `Serviço #${id} pré-seleccionado. Carregue «Carregar análise» para ver as paragens.`;
  }
  const wrapEl = document.getElementById("stopPassageTableWrap");
  if (wrapEl) wrapEl.classList.add("hidden");
  const tbody = document.getElementById("stopPassageTableBody");
  if (tbody) tbody.innerHTML = "";
  openSupervisorTab("tabParagensServico");
  if (idInput) idInput.focus();
}

function closeServiceDrawer() {
  serviceDrawerEl.classList.add("hidden");
  serviceDrawerBackdropEl.classList.add("hidden");
  selectedService = null;
  currentSupervisorUserId = null;
}

function openServiceDrawer(service) {
  selectedService = service;
  drawerTitleEl.textContent = `Serviço #${service.id}`;
  serviceDrawerInfoEl.textContent = "A carregar detalhe...";
  serviceRouteMiniMapEl.textContent = "A carregar rota...";
  serviceRouteTimesEl.textContent = "-";
  serviceHandoversListEl.innerHTML = "<li>A carregar transferências...</li>";
  document.getElementById("adjustStatus").value = "";
  document.getElementById("adjustFleetNumber").value = service.fleet_number || "";
  document.getElementById("handoverToDriverId").value = "";
  document.getElementById("handoverReasonSup").value = "";
  document.getElementById("handoverLocationSup").value = "";
  serviceDrawerBackdropEl.classList.remove("hidden");
  serviceDrawerEl.classList.remove("hidden");
  loadServiceDetails(service.id);
}

function populateSupervisorHandoverDrivers(service) {
  const selectEl = document.getElementById("handoverToDriverId");
  if (!selectEl) return;
  const currentDriverId = Number(service?.driver_id);
  const options = driversCache
    .filter(
      (d) =>
        d.is_active &&
        String(d.role || "").toLowerCase() === "driver" &&
        Number(d.id) !== currentDriverId
    )
    .sort((a, b) => String(a.name || "").localeCompare(String(b.name || ""), "pt"))
    .map((d) => `<option value="${d.id}">${d.name} (Mec ${d.mechanic_number || "-"})</option>`);

  selectEl.innerHTML = `<option value="">— Selecionar motorista —</option>${options.join("")}`;
}

function renderMiniRouteMap(points) {
  if (!points || points.length < 2) {
    serviceRouteMiniMapEl.textContent = "Sem pontos GPS.";
    serviceRouteTimesEl.textContent = "-";
    return;
  }
  const width = 340;
  const height = 170;
  const pad = 10;
  const lats = points.map((p) => Number(p.lat));
  const lngs = points.map((p) => Number(p.lng));
  const minLat = Math.min(...lats);
  const maxLat = Math.max(...lats);
  const minLng = Math.min(...lngs);
  const maxLng = Math.max(...lngs);
  const latSpan = Math.max(maxLat - minLat, 0.000001);
  const lngSpan = Math.max(maxLng - minLng, 0.000001);

  const palette = ["#2563eb", "#16a34a", "#7c3aed", "#ea580c", "#dc2626", "#0891b2"];
  const segmentColorMap = new Map();
  let segmentColorIdx = 0;
  const getSegmentColor = (segmentId) => {
    const key = String(segmentId || "0");
    if (!segmentColorMap.has(key)) {
      segmentColorMap.set(key, palette[segmentColorIdx % palette.length]);
      segmentColorIdx += 1;
    }
    return segmentColorMap.get(key);
  };

  const coords = points.map((p) => {
      const x = pad + ((Number(p.lng) - minLng) / lngSpan) * (width - pad * 2);
      const y = height - pad - ((Number(p.lat) - minLat) / latSpan) * (height - pad * 2);
      return { x, y, p };
    });

  const lines = [];
  for (let i = 1; i < coords.length; i += 1) {
    const a = coords[i - 1];
    const b = coords[i];
    const segmentId = b.p.service_segment_id || a.p.service_segment_id || 0;
    const color = getSegmentColor(segmentId);
    lines.push(
      `<line x1="${a.x.toFixed(1)}" y1="${a.y.toFixed(1)}" x2="${b.x.toFixed(1)}" y2="${b.y.toFixed(
        1
      )}" stroke="${color}" stroke-width="3" stroke-linecap="round"></line>`
    );
  }

  const first = coords[0];
  const last = coords[coords.length - 1];
  const firstTime = first.p.captured_at ? new Date(first.p.captured_at).toLocaleString() : "-";
  const lastTime = last.p.captured_at ? new Date(last.p.captured_at).toLocaleString() : "-";
  serviceRouteTimesEl.textContent = `Início: ${firstTime} | Fim: ${lastTime}`;

  const segmentLegend = Array.from(segmentColorMap.entries())
    .map(([seg, color]) => `<span style="display:inline-block;margin-right:8px;color:${color};">Trecho ${seg}</span>`)
    .join(" ");

  const startCircle = `<circle cx="${first.x.toFixed(1)}" cy="${first.y.toFixed(
    1
  )}" r="4.5" fill="#16a34a"><title>Início: ${firstTime}</title></circle>`;
  const endCircle = `<circle cx="${last.x.toFixed(1)}" cy="${last.y.toFixed(
    1
  )}" r="4.5" fill="#dc2626"><title>Fim: ${lastTime}</title></circle>`;

  serviceRouteMiniMapEl.innerHTML = `
    <svg viewBox="0 0 ${width} ${height}" width="100%" height="160" role="img" aria-label="Rota do serviço">
      <rect x="0" y="0" width="${width}" height="${height}" fill="#f9fafb"></rect>
      ${lines.join("")}
      ${startCircle}
      ${endCircle}
    </svg>
    <div style="margin-top:4px;font-size:11px;color:#374151;">${segmentLegend}</div>
  `;
}

function renderHandovers(handovers) {
  if (!handovers || !handovers.length) {
    serviceHandoversListEl.innerHTML = "<li>Sem transferências registadas.</li>";
    return;
  }
  serviceHandoversListEl.innerHTML = handovers
    .map(
      (h) =>
        `<li>${new Date(h.created_at).toLocaleString()} | ${h.from_driver_name || "-"} → ${h.to_driver_name || "-"} | Motivo: ${h.reason || "-"} | Local: ${h.handover_location_text || "-"} | Estado: ${labelEstadoTransferenciaPt(h.status)}</li>`
    )
    .join("");
}

async function loadServiceDetails(serviceId) {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/supervisor/services/${serviceId}/details`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json();
  if (!response.ok) {
    serviceDrawerInfoEl.textContent = data.message || "Erro ao carregar detalhe.";
    serviceRouteMiniMapEl.textContent = "Sem dados.";
    serviceRouteTimesEl.textContent = "-";
    serviceHandoversListEl.innerHTML = "<li>Sem dados.</li>";
    forceEndServiceBtn.classList.add("hidden");
    return;
  }

  selectedService = data.service;
  serviceDrawerInfoEl.textContent = [
    `Motorista: ${data.service.driver_name || "-"}`,
    `Linha: ${data.service.line_code || "-"}`,
    `Origem/chapa: ${data.service.plate_number || "-"}`,
    `Frota atual: ${data.service.fleet_number || "-"}`,
    `Estado: ${labelEstadoExecucaoServicoPt(data.service.status)}`,
    `Modo de fecho: ${labelCloseModePt(data.service.close_mode)}`,
    `Horario: ${data.service.service_schedule || "-"}`,
    `Início: ${data.service.started_at ? new Date(data.service.started_at).toLocaleString() : "-"}`,
    `Chegada: ${data.service.ended_at ? new Date(data.service.ended_at).toLocaleString() : "-"}`,
    `Quilómetros: ${data.service.total_km || 0}`,
  ].join("\n");
  document.getElementById("adjustFleetNumber").value = data.service.fleet_number || "";
  initServiceDetailMap();
  if (serviceDetailRouteLayer) serviceDetailRouteLayer.clearLayers();
  const executedRoute = (data.points || [])
    .map((p) => [Number(p.lat), Number(p.lng)])
    .filter((p) => Number.isFinite(p[0]) && Number.isFinite(p[1]));
  if (serviceDetailRouteLayer && executedRoute.length >= 2) {
    const doneLine = L.polyline(executedRoute, { color: "#2563eb", weight: 4 }).addTo(serviceDetailRouteLayer);
    serviceDetailRouteMap.fitBounds(doneLine.getBounds(), { padding: [20, 20] });
  } else if (serviceDetailRouteLayer) {
    const refPoints = await loadSupervisorReferenceRoute(serviceId);
    const refLatLngs = refPoints
      .map((p) => [Number(p.lat), Number(p.lng)])
      .filter((p) => Number.isFinite(p[0]) && Number.isFinite(p[1]));
    if (refLatLngs.length >= 2) {
      const refLine = L.polyline(refLatLngs, { color: "#f97316", weight: 4, dashArray: "8 8" }).addTo(serviceDetailRouteLayer);
      serviceDetailRouteMap.fitBounds(refLine.getBounds(), { padding: [20, 20] });
    }
  }
  if (executedRoute.length >= 1) {
    const firstPoint = data.points?.[0];
    const lastPoint = data.points?.[data.points.length - 1];
    const firstTime = firstPoint?.captured_at ? new Date(firstPoint.captured_at).toLocaleString() : "-";
    const lastTime = lastPoint?.captured_at ? new Date(lastPoint.captured_at).toLocaleString() : "-";
    serviceRouteTimesEl.textContent = `Início: ${firstTime} | Fim: ${lastTime}`;
  } else {
    serviceRouteTimesEl.textContent = "Sem percurso GPS registado.";
  }
  setTimeout(() => serviceDetailRouteMap?.invalidateSize(), 80);
  renderHandovers(data.handovers || []);
  populateSupervisorHandoverDrivers(data.service);
  const inProgress = data.service.status === "in_progress";
  if (inProgress) {
    forceEndServiceBtn.classList.remove("hidden");
    transferServiceBtn.classList.remove("hidden");
    cancelServiceBtn.classList.remove("hidden");
  } else {
    forceEndServiceBtn.classList.add("hidden");
    transferServiceBtn.classList.add("hidden");
    cancelServiceBtn.classList.add("hidden");
  }
}

async function saveServiceAdjust(event) {
  event.preventDefault();
  if (!supToken || !selectedService) return;

  const status = document.getElementById("adjustStatus").value;
  const fleetText = document.getElementById("adjustFleetNumber").value.trim();
  const payload = {};
  if (status) payload.status = status;
  if (fleetText !== "") payload.fleetNumber = Number(fleetText);

  if (!Object.keys(payload).length) {
    alert("Escolha pelo menos um campo para ajustar.");
    return;
  }

  const response = await fetch(`${API_BASE}/supervisor/services/${selectedService.id}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao guardar ajuste.");
    return;
  }

  const idx = currentServicesCache.findIndex((s) => s.id === data.id);
  if (idx >= 0) currentServicesCache[idx] = { ...currentServicesCache[idx], ...data };
  alert("Ajuste guardado com sucesso.");
  closeServiceDrawer();
  await loadServices();
  await loadLiveServicesMap();
}

async function forceEndService() {
  if (!supToken || !selectedService) return;
  const confirmEnd = window.confirm(`Finalizar agora o serviço n.º ${selectedService.id}?`);
  if (!confirmEnd) return;

  const response = await fetch(`${API_BASE}/supervisor/services/${selectedService.id}/force-end`, {
    method: "POST",
    headers: getAuthHeaders(),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao finalizar servico.");
    return;
  }
  alert("Servico finalizado com sucesso.");
  closeServiceDrawer();
  await loadServices();
  await loadLiveServicesMap();
}

async function cancelServiceBySupervisor() {
  if (!supToken || !selectedService) return;
  const confirmCancel = window.confirm(
    `Anular o serviço n.º ${selectedService.id}? A viagem não ficará como concluída.`
  );
  if (!confirmCancel) return;

  const response = await fetch(`${API_BASE}/supervisor/services/${selectedService.id}/cancel`, {
    method: "POST",
    headers: getAuthHeaders(),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao anular servico.");
    return;
  }
  alert("Serviço anulado com sucesso.");
  closeServiceDrawer();
  await loadServices();
  await loadLiveServicesMap();
}

async function transferServiceBySupervisor() {
  if (!supToken || !selectedService) return;
  const toDriverId = Number(document.getElementById("handoverToDriverId").value);
  const reason = document.getElementById("handoverReasonSup").value.trim();
  const handoverLocationText = document.getElementById("handoverLocationSup").value.trim();
  const toFleetNumberText = document.getElementById("adjustFleetNumber").value.trim();

  if (!Number.isFinite(toDriverId) || toDriverId <= 0) {
    alert("Selecione o motorista de destino para a transferência.");
    return;
  }
  if (reason.length < 3) {
    alert("Indique um motivo para a transferência.");
    return;
  }

  const response = await fetch(`${API_BASE}/supervisor/services/${selectedService.id}/handover`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify({
      toDriverId,
      reason,
      handoverLocationText: handoverLocationText || null,
      toFleetNumber: toFleetNumberText ? Number(toFleetNumberText) : null,
    }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao transferir serviço.");
    return;
  }
  alert("Serviço transferido com sucesso.");
  closeServiceDrawer();
  await loadServices();
  await loadLiveServicesMap();
}

async function loginSupervisor(event) {
  event.preventDefault();
  const username = document.getElementById("supUsername").value.trim();
  const password = document.getElementById("supPassword").value;

  const response = await fetch(`${API_BASE}/auth/login`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify({ username, password }),
  });
  const data = await response.json();

  if (!response.ok) {
    alert(data.message || "Erro no login.");
    return;
  }
  const role = String(data.user?.role || "").trim().toLowerCase() || readRoleFromJwt(data.token);
  if (role === "viewer" || role === "viewr") {
    window.location.href = `${window.location.origin}/frontend/viewer.html`;
    return;
  }
  if (role === "driver") {
    window.location.href = `${window.location.origin}/frontend/index.html`;
    return;
  }
  if (!["supervisor", "admin"].includes(role)) {
    alert(`Este utilizador nao tem perfil de supervisor. Perfil recebido: ${role || "desconhecido"}.`);
    return;
  }

  supToken = data.token;
  sessionStorage.setItem(AUTH_SESSION_KEY, JSON.stringify({ token: data.token, user: data.user }));
  alert(`Sessao iniciada para ${data.user.name} (${role})`);
  applySessionAndLoad(data.user);
  await loadOverview();
  await loadServices();
  await loadLiveServicesMap();
  await loadDrivers();
  await loadConflictAlerts();
  startLiveMapAutoRefresh();
  supervisorAlertBaselineReady = false;
  startSupervisorAlertsPolling();
}

function logoutSupervisor() {
  const confirmed = window.confirm("Tem a certeza que deseja terminar sessao?");
  if (!confirmed) return;

  supToken = "";
  activeQueryString = "";
  currentServicesCache = [];
  selectedService = null;
  sessionWelcomeEl.classList.add("hidden");
  logoutBtnEl.classList.add("hidden");
  loginCardEl.classList.remove("hidden");
  document.getElementById("supervisorTabs").classList.add("hidden");
  closeServiceDrawer();
  if (supLiveRefreshInterval) {
    clearInterval(supLiveRefreshInterval);
    supLiveRefreshInterval = null;
  }
  stopSupervisorAlertsPolling();
  supervisorAlertBaselineReady = false;
  lastSupervisorAlertSignature = "";
  document.getElementById("supLoginForm").reset();
  sessionStorage.removeItem(AUTH_SESSION_KEY);
}

function setSupervisorModule(moduleId, options = {}) {
  const { preserveCurrentTab = false, skipTabActivation = false } = options;
  if (!moduleId) return;
  const moduleButtons = document.querySelectorAll(".module-nav-btn");
  moduleButtons.forEach((btn) => {
    btn.classList.toggle("active", btn.getAttribute("data-module-target") === moduleId);
  });

  const tabButtons = document.querySelectorAll(".tab-btn[data-module]");
  let firstVisibleTabId = "";
  let activeVisibleTabId = "";
  tabButtons.forEach((btn) => {
    const matches = btn.getAttribute("data-module") === moduleId;
    btn.classList.toggle("hidden", !matches);
    if (matches) {
      const tabId = btn.getAttribute("data-tab-target") || "";
      if (!firstVisibleTabId) firstVisibleTabId = tabId;
      if (btn.classList.contains("active")) activeVisibleTabId = tabId;
    }
  });

  if (skipTabActivation) return;
  if (preserveCurrentTab && activeVisibleTabId) return;
  if (!firstVisibleTabId) return;
  openSupervisorTab(activeVisibleTabId || firstVisibleTabId);
}

function openSupervisorTab(tabId) {
  if (!tabId) return;
  const moduleId = TAB_MODULE_MAP[tabId];
  if (moduleId) {
    // Avoid recursive tab->module->tab activation loops.
    setSupervisorModule(moduleId, { preserveCurrentTab: true, skipTabActivation: true });
  }
  const tabButtons = document.querySelectorAll(".tab-btn");
  const tabPanels = document.querySelectorAll(".tab-panel");
  tabButtons.forEach((b) => {
    const t = b.getAttribute("data-tab-target");
    b.classList.toggle("active", t === tabId);
  });
  tabPanels.forEach((p) => {
    p.classList.toggle("active", p.id === tabId);
  });
  if (tabId === "tabEscalaDia") {
    loadRosterToday();
  }
  if (tabId === "tabTempoReal" || tabId === "tabServicos") {
    loadLiveServicesMap();
    if (supLiveMap) setTimeout(() => supLiveMap.invalidateSize(), 80);
    if (tabId === "tabTempoReal") {
      loadDeadheadMovements();
      if (deadheadMap) setTimeout(() => deadheadMap.invalidateSize(), 80);
    }
  }
  if (tabId === "tabAlertasConflito") {
    loadConflictAlerts();
  }
  if (tabId === "tabIntegracoesGps") {
    loadTrackerDevices();
  }
  if (tabId === "tabViaturas") {
    loadVehicleRegistry();
    loadOdometerReconciliationReport();
  }
  if (tabId === "tabEditorGtfs") {
    loadGtfsFeeds();
    loadGtfsEditorLines();
    loadGtfsCalendars();
  }
  if (tabId === "tabAnaliseGtfs") {
    loadGtfsFeeds();
    loadGtfsAnalyticsOverview();
    initGtfsAnalyticsMap();
  }
  if (tabId === "tabImportarMotoristas") {
    loadGtfsFeeds();
  }
  if (tabId === "tabMensagensComunicacao") {
    loadOpsMessagePresets();
    loadOpsThreads();
    if (selectedOpsDriverId) loadOpsMessages(selectedOpsDriverId);
  }
  if (tabId === "tabMensagensPresets") {
    loadSupPresetList();
  }
  if (tabId === "tabRelatoriosIa" && !reportsLoadedOnce) {
    loadOperationalReport();
  }
}

function buildOperationalReportQuery() {
  const params = new URLSearchParams();
  const fromDate = document.getElementById("reportFromDate")?.value || "";
  const toDate = document.getElementById("reportToDate")?.value || "";
  const lineCode = document.getElementById("reportLineCode")?.value?.trim() || "";
  const driverId = document.getElementById("reportDriverId")?.value?.trim() || "";
  const radiusM = document.getElementById("reportRadiusM")?.value?.trim() || "";
  const maxServices = document.getElementById("reportMaxServices")?.value?.trim() || "";
  if (fromDate) params.set("fromDate", fromDate);
  if (toDate) params.set("toDate", toDate);
  if (lineCode) params.set("lineCode", lineCode);
  if (driverId) params.set("driverId", driverId);
  if (radiusM) params.set("radiusM", radiusM);
  if (maxServices) params.set("maxServices", maxServices);
  return params.toString() ? `?${params.toString()}` : "";
}

function renderOperationalSummary(summary) {
  if (!reportSummaryCardsEl) return;
  const cards = [
    { label: "Serviços analisados", value: summary.services_analyzed ?? 0 },
    { label: "Serviços com paragens GTFS", value: summary.services_with_reference_stops ?? 0 },
    { label: "Paragens analisadas", value: summary.total_stops_analyzed ?? 0 },
    { label: "Atraso médio (min)", value: formatNumberPt(summary.avg_delay_min, 1) },
    { label: "Atraso P90 (min)", value: formatNumberPt(summary.delay_p90_min, 1) },
    { label: "Taxa paragens sem passagem", value: `${formatNumberPt(summary.missed_stop_rate_pct, 1)}%` },
  ];
  reportSummaryCardsEl.innerHTML = cards
    .map(
      (c) => `<article class="service-card-item"><strong>${c.label}</strong><div style="font-size: 20px; margin-top: 6px;">${c.value}</div></article>`
    )
    .join("");
}

function renderAiSuggestions(items) {
  if (!reportAiSuggestionsEl) return;
  const rows = Array.isArray(items) ? items : [];
  if (!rows.length) {
    reportAiSuggestionsEl.innerHTML = "<li>Sem sugestões para o período selecionado.</li>";
    return;
  }
  reportAiSuggestionsEl.innerHTML = rows.map((s) => `<li>${s}</li>`).join("");
}

function renderCriticalStops(rows) {
  if (!reportCriticalStopsBodyEl) return;
  if (!Array.isArray(rows) || !rows.length) {
    reportCriticalStopsBodyEl.innerHTML = `<tr><td colspan="6">Sem paragens críticas no período.</td></tr>`;
    return;
  }
  reportCriticalStopsBodyEl.innerHTML = rows
    .map(
      (r) => `<tr>
        <td>${r.stop_name || "-"}</td>
        <td>${r.samples ?? 0}</td>
        <td>${formatNumberPt(r.avg_delay_min, 1)}</td>
        <td>${formatNumberPt(r.delayed_rate_pct, 1)}%</td>
        <td>${formatNumberPt(r.severe_delay_rate_pct, 1)}%</td>
        <td>${formatNumberPt(r.criticality_score, 1)}</td>
      </tr>`
    )
    .join("");
}

function renderServiceRank(rows) {
  if (!reportServiceRankBodyEl) return;
  if (!Array.isArray(rows) || !rows.length) {
    reportServiceRankBodyEl.innerHTML = `<tr><td colspan="7">Sem serviços com atraso para ranking.</td></tr>`;
    return;
  }
  reportServiceRankBodyEl.innerHTML = rows
    .map(
      (r) => `<tr>
        <td>#${r.service_id}</td>
        <td>${r.line_code || "-"}</td>
        <td>${r.driver_name || "-"}</td>
        <td>${r.fleet_number || "-"}</td>
        <td>${r.stops_matched || 0}/${r.stops_total || 0}</td>
        <td>${formatNumberPt(r.avg_delay_min, 1)}</td>
        <td>${formatNumberPt(r.max_delay_min, 1)}</td>
      </tr>`
    )
    .join("");
}

async function loadOperationalReport() {
  if (!supToken) return;
  const query = buildOperationalReportQuery();
  if (reportSummaryCardsEl) {
    reportSummaryCardsEl.innerHTML = `<article class="service-card-item">A gerar relatório operacional...</article>`;
  }
  try {
    const response = await fetch(`${API_BASE}/supervisor/reports/performance${query}`, {
      headers: getAuthHeaders(),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      throw new Error(data.message || "Erro ao gerar relatório.");
    }
    reportsLoadedOnce = true;
    renderOperationalSummary(data.summary || {});
    renderAiSuggestions(data.ai_suggestions || []);
    renderCriticalStops(data.critical_stops || []);
    renderServiceRank(data.service_rank || []);
  } catch (error) {
    if (reportSummaryCardsEl) {
      reportSummaryCardsEl.innerHTML = `<article class="service-card-item">${
        error?.message || "Erro ao gerar relatório operacional."
      }</article>`;
    }
    renderAiSuggestions([]);
    renderCriticalStops([]);
    renderServiceRank([]);
  }
}

async function downloadOperationalReportExcel(queryString) {
  if (!supToken) return;
  const url = `${API_BASE}/supervisor/reports/performance.xlsx${queryString || ""}`;
  try {
    const response = await fetch(url, { headers: getAuthHeaders() });
    if (!response.ok) {
      const errText = await response.text();
      let message = "Erro ao exportar o relatório.";
      try {
        const j = JSON.parse(errText);
        message = j.message || message;
      } catch (_e) {
        // ignore
      }
      alert(message);
      return;
    }
    const blob = await response.blob();
    const cd = response.headers.get("Content-Disposition") || "";
    const match = cd.match(/filename="([^"]+)"/i);
    const fallback = `relatorio-ia${queryString || ""}.xlsx`.replace(/[?&=]/g, "_");
    const filename = match ? match[1] : fallback;
    const objectUrl = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = objectUrl;
    a.download = filename;
    document.body.appendChild(a);
    a.click();
    a.remove();
    URL.revokeObjectURL(objectUrl);
  } catch (error) {
    alert(error?.message || "Erro ao descarregar o ficheiro Excel.");
  }
}

function initTabs() {
  const tabButtons = document.querySelectorAll(".tab-btn");
  tabButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const targetId = btn.getAttribute("data-tab-target");
      openSupervisorTab(targetId);
    });
  });

  const moduleButtons = document.querySelectorAll(".module-nav-btn");
  moduleButtons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const moduleId = btn.getAttribute("data-module-target");
      setSupervisorModule(moduleId);
    });
  });
  setSupervisorModule("dashboard");
}

function renderTrackerWebhookExamples() {
  if (!trackerWebhookUrlPreviewEl || !trackerWebhookHeadersPreviewEl || !trackerWebhookPayloadPreviewEl) return;
  const webhookUrl = `${API_BASE}/integrations/teltonika/events`;
  const tokenPlaceholder = "<TELTONIKA_WEBHOOK_TOKEN>";
  const headersExample = {
    Authorization: `Bearer ${tokenPlaceholder}`,
    "Content-Type": "application/json",
  };
  const payloadExample = {
    imei: "356307042441013",
    lat: 38.7231,
    lng: -9.1384,
    speedKmh: 34.2,
    headingDeg: 115,
    accuracyM: 6.0,
    timestamp: "2026-04-22T09:00:00.000Z",
    fleetNumber: "BUS-01",
    plateNumber: "AA-00-BB",
  };
  const batchExample = {
    events: [
      { ...payloadExample, timestamp: "2026-04-22T09:00:05.000Z", lat: 38.7232, lng: -9.1383 },
      { ...payloadExample, timestamp: "2026-04-22T09:00:10.000Z", lat: 38.7233, lng: -9.1382 },
    ],
  };
  trackerWebhookUrlPreviewEl.value = webhookUrl;
  trackerWebhookHeadersPreviewEl.value = JSON.stringify(headersExample, null, 2);
  trackerWebhookPayloadPreviewEl.value = JSON.stringify(payloadExample, null, 2);
  if (trackerWebhookBatchPayloadPreviewEl) {
    trackerWebhookBatchPayloadPreviewEl.value = JSON.stringify(batchExample, null, 2);
  }
}

async function copyTrackerWebhookConfig() {
  const webhookUrl = trackerWebhookUrlPreviewEl?.value || `${API_BASE}/integrations/teltonika/events`;
  const headersText = trackerWebhookHeadersPreviewEl?.value || "";
  const payloadText = trackerWebhookPayloadPreviewEl?.value || "";
  const batchText = trackerWebhookBatchPayloadPreviewEl?.value || "";
  const text = [
    `URL:\n${webhookUrl}`,
    `\nHeaders:\n${headersText}`,
    `\nPayload — um evento:\n${payloadText}`,
    batchText ? `\nPayload — batch (events):\n${batchText}` : "",
  ].join("");
  try {
    await navigator.clipboard.writeText(text);
    alert("Configuração do webhook copiada.");
  } catch (_error) {
    alert("Não foi possível copiar automaticamente. Selecione e copie manualmente.");
  }
}

function initServiceFilterMode() {
  const originBtn = document.getElementById("filterModeOriginBtn");
  const serviceBtn = document.getElementById("filterModeServiceBtn");
  const originInput = document.getElementById("fOrigin");
  const serviceInput = document.getElementById("fServiceId");

  originBtn.addEventListener("click", () => {
    filterMode = "origin";
    originBtn.classList.add("active");
    serviceBtn.classList.remove("active");
    originInput.classList.remove("hidden");
    serviceInput.classList.add("hidden");
  });
  serviceBtn.addEventListener("click", () => {
    filterMode = "service";
    serviceBtn.classList.add("active");
    originBtn.classList.remove("active");
    serviceInput.classList.remove("hidden");
    originInput.classList.add("hidden");
  });
}

function clearServiceFilters() {
  const fieldsToClear = ["fDriverId", "fLineCode", "fStatus", "fFromDate", "fToDate", "fOrigin", "fServiceId"];
  fieldsToClear.forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });
  const onlyCancelledEl = document.getElementById("fOnlyCancelled");
  if (onlyCancelledEl) onlyCancelledEl.checked = false;

  filterMode = "origin";
  const originBtn = document.getElementById("filterModeOriginBtn");
  const serviceBtn = document.getElementById("filterModeServiceBtn");
  const originInput = document.getElementById("fOrigin");
  const serviceInput = document.getElementById("fServiceId");
  originBtn?.classList.add("active");
  serviceBtn?.classList.remove("active");
  originInput?.classList.remove("hidden");
  serviceInput?.classList.add("hidden");

  loadServices();
}

function resolveLineColorHex(rawColor, fallback = "#2563eb") {
  const c = String(rawColor || "").trim();
  if (/^[0-9a-fA-F]{6}$/.test(c)) return `#${c}`;
  if (/^#[0-9a-fA-F]{6}$/.test(c)) return c;
  return fallback;
}

function initSupervisorLiveMap() {
  if (!supLiveMapEl || supLiveMap) return;
  supLiveMap = L.map("supLiveMap").setView([38.7223, -9.1393], 12);
  const primary = L.tileLayer(`${API_BASE}/map-tiles/{z}/{x}/{y}.png`, {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  });
  const fallback = L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 20,
    attribution: "&copy; OpenStreetMap contributors &copy; CARTO",
  });
  let tileErrors = 0;
  primary.on("tileerror", () => {
    tileErrors += 1;
    if (tileErrors < 4) return;
    if (supLiveMap.hasLayer(primary)) supLiveMap.removeLayer(primary);
    if (!supLiveMap.hasLayer(fallback)) fallback.addTo(supLiveMap);
  });
  primary.addTo(supLiveMap);
  supLiveMarkersLayer = L.layerGroup().addTo(supLiveMap);
  supLiveRoutesLayer = L.layerGroup().addTo(supLiveMap);
  supLiveMap.on("zoomstart", () => {
    supLiveMapUserAdjustedView = true;
  });
  supLiveMap.on("dragstart", () => {
    supLiveMapUserAdjustedView = true;
  });
}

function initServiceDetailMap() {
  if (!serviceRouteMiniMapEl || serviceDetailRouteMap) return;
  serviceRouteMiniMapEl.style.display = "block";
  serviceRouteMiniMapEl.style.padding = "0";
  serviceRouteMiniMapEl.style.height = "220px";
  serviceDetailRouteMap = L.map(serviceRouteMiniMapEl).setView([38.7223, -9.1393], 12);
  const primary = L.tileLayer(`${API_BASE}/map-tiles/{z}/{x}/{y}.png`, {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  });
  const fallback = L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 20,
    attribution: "&copy; OpenStreetMap contributors &copy; CARTO",
  });
  let tileErrors = 0;
  primary.on("tileerror", () => {
    tileErrors += 1;
    if (tileErrors < 4) return;
    if (serviceDetailRouteMap.hasLayer(primary)) serviceDetailRouteMap.removeLayer(primary);
    if (!serviceDetailRouteMap.hasLayer(fallback)) fallback.addTo(serviceDetailRouteMap);
  });
  primary.addTo(serviceDetailRouteMap);
  serviceDetailRouteLayer = L.layerGroup().addTo(serviceDetailRouteMap);
}

async function loadSupervisorReferenceRoute(serviceId) {
  const response = await fetch(`${API_BASE}/supervisor/services/${serviceId}/reference-route`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok || !Array.isArray(data.points)) return [];
  return data.points;
}

function buildSupervisorBusIcon(fleetNumber, lineColor) {
  return L.divIcon({
    className: "sup-bus-marker",
    html: `<div class="sup-bus-label" style="background:${lineColor}">Frota ${fleetNumber || "-"}</div><div class="sup-bus-icon">🚌</div>`,
    iconSize: [60, 44],
    iconAnchor: [30, 22],
  });
}

async function loadLiveServicesMap() {
  if (!supToken || !supLiveMapEl || !supLiveServicesListEl) return;
  initSupervisorLiveMap();
  const response = await fetch(`${API_BASE}/supervisor/services/live`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => ([]));
  if (!response.ok) {
    supLiveServicesListEl.innerHTML = `<div>${data.message || "Erro ao carregar serviços em execução."}</div>`;
    return;
  }
  currentLiveServices = Array.isArray(data) ? data : [];
  renderLiveRealtimeView();
}

function computeLiveDelayMinutes(service) {
  const candidates = [
    service?.delay_minutes,
    service?.delay_min,
    service?.realtime_delay_minutes,
    service?.realtime_delay_min,
    service?.next_stop_delay_minutes,
    service?.next_stop_delay_min,
  ];
  for (const value of candidates) {
    const n = Number(value);
    if (Number.isFinite(n)) return Math.round(n);
  }
  const statusRaw = String(service?.realtime_status || service?.punctuality_status || "").trim().toLowerCase();
  if (statusRaw.includes("adiant") || statusRaw === "early") return -1;
  if (statusRaw.includes("atras") || statusRaw === "late") return 1;
  return 0;
}

function classifyLiveServiceStatus(service) {
  const delay = computeLiveDelayMinutes(service);
  if (delay < 0) return "early";
  if (delay > 0) return "late";
  return "on_time";
}

function updateLiveRealtimeCounters(services) {
  const allEl = document.getElementById("liveCountAll");
  const earlyEl = document.getElementById("liveCountEarly");
  const lateEl = document.getElementById("liveCountLate");
  if (!allEl || !earlyEl || !lateEl) return;
  const all = Array.isArray(services) ? services.length : 0;
  const early = Array.isArray(services) ? services.filter((svc) => classifyLiveServiceStatus(svc) === "early").length : 0;
  const late = Array.isArray(services) ? services.filter((svc) => classifyLiveServiceStatus(svc) === "late").length : 0;
  allEl.textContent = String(all);
  earlyEl.textContent = String(early);
  lateEl.textContent = String(late);
}

function getLiveServicesFiltered() {
  const selectedServiceId = String(liveServiceFilterSelectEl?.value || "all");
  let list =
    selectedServiceId === "all"
      ? currentLiveServices
      : currentLiveServices.filter((svc) => String(svc.id) === selectedServiceId);
  if (liveRealtimeStatusFilter === "early") {
    list = list.filter((svc) => classifyLiveServiceStatus(svc) === "early");
  } else if (liveRealtimeStatusFilter === "late") {
    list = list.filter((svc) => classifyLiveServiceStatus(svc) === "late");
  }
  return list;
}

function applyLiveServiceRouteHighlight(serviceIdRaw) {
  const targetId = serviceIdRaw == null ? null : String(serviceIdRaw);
  supLiveHighlightedServiceId = targetId;
  if (!(supLiveRouteByServiceId instanceof Map) || !supLiveRouteByServiceId.size) return;
  supLiveRouteByServiceId.forEach((polyline, serviceId) => {
    if (!polyline) return;
    const isTarget = targetId && String(serviceId) === targetId;
    const bringFront = Boolean(isTarget);
    polyline.setStyle({
      weight: isTarget ? 6 : 3,
      opacity: isTarget ? 0.95 : targetId ? 0.2 : 0.65,
    });
    if (bringFront && typeof polyline.bringToFront === "function") polyline.bringToFront();
  });
}

async function renderLiveRealtimeView() {
  if (liveServiceFilterSelectEl) {
    const currentValue = String(liveServiceFilterSelectEl.value || "all");
    const options = [
      `<option value="all">Mostrar todos</option>`,
      ...currentLiveServices.map(
        (svc) =>
          `<option value="${svc.id}">Serviço #${svc.id} | Linha ${svc.line_code || "-"} | Frota ${svc.fleet_number || "-"}</option>`
      ),
    ];
    liveServiceFilterSelectEl.innerHTML = options.join("");
    const exists = currentValue === "all" || currentLiveServices.some((svc) => String(svc.id) === currentValue);
    liveServiceFilterSelectEl.value = exists ? currentValue : "all";
  }
  updateLiveRealtimeCounters(currentLiveServices);
  const list = getLiveServicesFiltered();

  supLiveServicesListEl.innerHTML = "";
  if (!list.length) {
    supLiveServicesListEl.innerHTML = "<div>Sem serviços em execução neste momento.</div>";
  }

  supLiveMarkersLayer.clearLayers();
  if (supLiveRoutesLayer) supLiveRoutesLayer.clearLayers();
  supLiveRouteByServiceId = new Map();
  const bounds = [];
  const routeBounds = [];
  list.forEach((svc) => {
    const lat = Number(svc.lat);
    const lng = Number(svc.lng);
    const lineColor = resolveLineColorHex(svc.route_color);
    if (Number.isFinite(lat) && Number.isFinite(lng)) {
      const marker = L.marker([lat, lng], {
        icon: buildSupervisorBusIcon(svc.fleet_number, lineColor),
      });
      marker.bindPopup(
        `Serviço #${svc.id}<br/>Linha ${svc.line_code || "-"}<br/>Motorista: ${svc.driver_name || "-"}<br/>Horário: ${svc.service_schedule || "-"}`
      );
      supLiveMarkersLayer.addLayer(marker);
      bounds.push([lat, lng]);
    }
    const card = document.createElement("article");
    card.className = "service-card-item";
    const delayMinutes = computeLiveDelayMinutes(svc);
    const delayClass = getDelayClass(delayMinutes);
    card.innerHTML = `<div><strong>Serviço #${svc.id}</strong> | Linha ${svc.line_code || "-"} | Frota ${svc.fleet_number || "-"} | <span class="live-driver-name" data-live-driver-service-id="${svc.id}">${svc.driver_name || "-"}</span></div>
      <div><small>Desvio horário:</small> <span class="delay-pill ${delayClass}">${formatDelay(delayMinutes)}</span></div>
      <div class="service-card-actions">
        <button type="button" class="stop-passages-shortcut-btn" data-stop-passages-service-id="${svc.id}">Paragens</button>
      </div>`;
    supLiveServicesListEl.appendChild(card);
  });

  if (supLiveRoutesLayer) {
    for (const svc of list) {
      try {
        const points = await loadSupervisorReferenceRoute(svc.id);
        const latLngs = points
          .map((p) => [Number(p.lat), Number(p.lng)])
          .filter((p) => Number.isFinite(p[0]) && Number.isFinite(p[1]));
        if (latLngs.length < 2) continue;
        const lineColor = resolveLineColorHex(svc.route_color, "#f97316");
        const polyline = L.polyline(latLngs, { color: lineColor, weight: 3, opacity: 0.65 }).addTo(supLiveRoutesLayer);
        supLiveRouteByServiceId.set(String(svc.id), polyline);
        routeBounds.push(...latLngs);
      } catch (_e) {
        // ignore
      }
    }
  }
  applyLiveServiceRouteHighlight(supLiveHighlightedServiceId);

  const allBounds = [...bounds, ...routeBounds];
  if (allBounds.length && !supLiveMapUserAdjustedView) {
    supLiveMap.fitBounds(allBounds, { padding: [20, 20], maxZoom: 15 });
  }
}

function startLiveMapAutoRefresh() {
  if (supLiveRefreshInterval) clearInterval(supLiveRefreshInterval);
  supLiveRefreshInterval = setInterval(() => {
    if (!supToken) return;
    loadLiveServicesMap();
  }, LIVE_MAP_REFRESH_MS);
}

async function loadConflictAlerts() {
  if (!supToken || !conflictAlertsListEl) return;
  conflictAlertsListEl.innerHTML = "<div>A carregar alertas...</div>";
  const response = await fetch(`${API_BASE}/supervisor/conflict-alerts`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => ([]));
  if (!response.ok) {
    conflictAlertsListEl.innerHTML = `<div>${data.message || "Erro ao carregar alertas de conflito."}</div>`;
    return;
  }
  if (!Array.isArray(data) || !data.length) {
    conflictAlertsListEl.innerHTML = "<div>Sem alertas de conflito registados.</div>";
    return;
  }

  conflictAlertsListEl.innerHTML = "";
  data.forEach((item) => {
    const article = document.createElement("article");
    article.className = "service-card-item";
    const conflictIds = Array.isArray(item.conflict_planned_service_ids) ? item.conflict_planned_service_ids : [];
    const unassignedIds = Array.isArray(item.unassigned_planned_service_ids) ? item.unassigned_planned_service_ids : [];
    const startLoc = item.start_location || "-";
    const endLoc = item.end_location || "-";
    const alertType = String(item.alert_type || "roster_conflict");
    const alertLabel =
      alertType === "driver_swap_overlap"
        ? "Conflito após troca"
        : alertType === "unassigned_service_after_swap"
          ? "Sem motorista atribuído"
          : "Conflito";
    const affectedDriverText = item.affected_driver_name
      ? `${item.affected_driver_name} (Mec. ${item.affected_driver_mechanic_number || "-"})`
      : "-";
    const affectedServiceText = item.affected_service_code || (item.affected_planned_service_id ? `#${item.affected_planned_service_id}` : "-");
    article.innerHTML = `
      <div class="service-card-head">
        <strong>Alerta #${item.id}</strong>
        <span class="status-badge status-other">${alertLabel}</span>
      </div>
      <div class="service-card-grid">
        <div><small>Data/Hora alerta</small><div>${item.created_at ? new Date(item.created_at).toLocaleString() : "-"}</div></div>
        <div><small>Motorista</small><div>${item.driver_name || "-"} (Mec. ${item.driver_mechanic_number || "-"})</div></div>
        <div><small>Serviço</small><div>${item.service_code || "-"} (ID plan. ${item.planned_service_id || "-"})</div></div>
        <div><small>Motorista envolvido</small><div>${affectedDriverText}</div></div>
        <div><small>Serviço envolvido</small><div>${affectedServiceText}</div></div>
        <div><small>Horário</small><div>${item.service_schedule || "-"}</div></div>
        <div><small>Linha</small><div>${item.line_code || "-"}</div></div>
        <div><small>Origem / destino</small><div>${startLoc} → ${endLoc}</div></div>
        <div><small>IDs em conflito</small><div>${conflictIds.length ? conflictIds.join(", ") : "-"}</div></div>
        <div><small>Sem motorista atribuído</small><div>${unassignedIds.length ? unassignedIds.join(", ") : "-"}</div></div>
        <div><small>Notas</small><div>${item.notes || "-"}</div></div>
      </div>
    `;
    conflictAlertsListEl.appendChild(article);
  });
}

async function loadOpsMessagePresets() {
  if (!supToken || !supMessagePresetEl) return;
  try {
    const response = await fetch(`${API_BASE}/supervisor/message-presets?scope=supervisor`, {
      headers: getAuthHeaders(),
    });
    const data = await response.json().catch(() => []);
    if (!response.ok || !Array.isArray(data)) return;
    supMessagePresetEl.innerHTML = '<option value="">-- Nenhuma (escrever manualmente) --</option>';
    data.forEach((item) => {
      const option = document.createElement("option");
      option.value = item.code || "";
      option.textContent = item.label || item.code || "Preset";
      const def = item.defaultText != null && String(item.defaultText).trim() ? String(item.defaultText) : String(item.label || "");
      option.dataset.defaultText = def;
      supMessagePresetEl.appendChild(option);
    });
    supMessagePresetEl.onchange = () => {
      const value = supMessagePresetEl.value;
      if (!value) return;
      const opt = supMessagePresetEl.selectedOptions?.[0];
      const def = opt?.dataset?.defaultText;
      if (def && supMessageTextEl) {
        supMessageTextEl.value = def;
      }
    };
  } catch (_error) {
    // ignore
  }
}

async function loadSupPresetList() {
  if (!supToken || !supPresetListEl) return;
  const scope = String(supPresetListScopeEl?.value || "supervisor");
  supPresetListEl.innerHTML = "<li>A carregar…</li>";
  try {
    const response = await fetch(`${API_BASE}/supervisor/message-presets/manage?scope=${encodeURIComponent(scope)}`, {
      headers: getAuthHeaders(),
    });
    const data = await response.json().catch(() => ([]));
    if (!response.ok) {
      supPresetListEl.innerHTML = `<li>${data.message || "Erro ao listar predefinidas."}</li>`;
      return;
    }
    const rows = Array.isArray(data) ? data : [];
    supPresetListEl.innerHTML = "";
    if (!rows.length) {
      supPresetListEl.innerHTML = "<li>Sem predefinidas personalizadas.</li>";
      return;
    }
    rows.forEach((row) => {
      const li = document.createElement("li");
      const active = row.is_active;
      li.innerHTML = `<div><strong>${row.label || "-"}</strong> <code>${row.code || "-"}</code> — ${
        active ? "ativa" : "inativa"
      }</div>
        <div class="ops-message-meta">${row.default_message_text || ""}</div>
        <div>
          <button type="button" class="toggle-btn" data-preset-id="${row.id}" data-preset-next-active="${active ? 0 : 1}">
            ${active ? "Desativar" : "Reativar"}
          </button>
          <button type="button" class="danger-btn" data-preset-delete-id="${row.id}">Apagar</button>
        </div>`;
      li.querySelector("[data-preset-id]")?.addEventListener("click", async (e) => {
        const id = Number(e.currentTarget.getAttribute("data-preset-id"));
        const next = e.currentTarget.getAttribute("data-preset-next-active") === "1";
        await setSupPresetActive(id, next);
      });
      li.querySelector("[data-preset-delete-id]")?.addEventListener("click", async (e) => {
        const id = Number(e.currentTarget.getAttribute("data-preset-delete-id"));
        const ok = window.confirm("Remover definitivamente esta predefinida?");
        if (!ok) return;
        await deleteSupPreset(id);
      });
      supPresetListEl.appendChild(li);
    });
  } catch (_error) {
    supPresetListEl.innerHTML = "<li>Erro de ligação ao listar predefinidas.</li>";
  }
}

async function setSupPresetActive(presetId, isActive) {
  if (!supToken) return;
  try {
    const response = await fetch(`${API_BASE}/supervisor/message-presets/${presetId}`, {
      method: "PATCH",
      headers: getAuthHeaders(),
      body: JSON.stringify({ isActive }),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      alert(data.message || "Erro ao atualizar predefinida.");
      return;
    }
    await loadSupPresetList();
    if (String(supPresetScopeEl?.value || "") === "supervisor") {
      await loadOpsMessagePresets();
    }
  } catch (_error) {
    alert("Erro de ligação ao atualizar predefinida.");
  }
}

async function deleteSupPreset(presetId) {
  if (!supToken) return;
  try {
    const response = await fetch(`${API_BASE}/supervisor/message-presets/${presetId}`, {
      method: "DELETE",
      headers: getAuthHeaders(),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      alert(data.message || "Erro ao remover predefinida.");
      return;
    }
    await loadSupPresetList();
    if (String(supPresetScopeEl?.value || "") === "supervisor") {
      await loadOpsMessagePresets();
    }
  } catch (_error) {
    alert("Erro de ligação ao remover predefinida.");
  }
}

async function loadOpsThreads() {
  if (!supToken || !opsThreadsListEl) return;
  opsThreadsListEl.innerHTML = "<li>A carregar conversas...</li>";
  try {
    const response = await fetch(`${API_BASE}/supervisor/messages/threads`, {
      headers: getAuthHeaders(),
    });
    const data = await response.json().catch(() => []);
    if (!response.ok) {
      opsThreadsListEl.innerHTML = `<li>${data.message || "Erro ao listar conversas."}</li>`;
      return;
    }
    const list = Array.isArray(data) ? data : [];
    opsThreadsListEl.innerHTML = "";
    if (!list.length) {
      opsThreadsListEl.innerHTML = "<li>Sem conversas com motoristas.</li>";
      return;
    }
    list.forEach((item) => {
      const li = document.createElement("li");
      li.className = "ops-thread-item";
      if (Number(item.driver_id) === Number(selectedOpsDriverId)) li.classList.add("active");
      const unread = Number(item.unread_from_driver || 0);
      li.innerHTML = `<strong>${item.driver_name || "-"}</strong> (Mec. ${item.mechanic_number || "-"})<br/>
        <small>Última: ${item.last_message_at ? new Date(item.last_message_at).toLocaleString() : "-"}</small>
        ${unread > 0 ? `<br/><small>Por ler: ${unread}</small>` : ""}`;
      li.addEventListener("click", () => {
        selectedOpsDriverId = Number(item.driver_id);
        loadOpsThreads();
        loadOpsMessages(selectedOpsDriverId);
      });
      opsThreadsListEl.appendChild(li);
    });
  } catch (_error) {
    opsThreadsListEl.innerHTML = "<li>Erro de ligação ao listar conversas.</li>";
  }
}

function buildOpsMessageItem(message) {
  const li = document.createElement("li");
  li.className = "ops-message-item";
  if (message.is_traffic_alert) li.classList.add("traffic-alert");
  const meta = document.createElement("div");
  meta.className = "ops-message-meta";
  const created = message.created_at ? new Date(message.created_at).toLocaleString() : "-";
  const traffic = message.is_traffic_alert ? " | ALERTA TRANSITO" : "";
  meta.textContent = `${created}${traffic}`;
  const author = document.createElement("strong");
  author.textContent = `${message.from_name || "-"} -> ${message.to_name || "-"}`;
  const body = document.createElement("p");
  body.textContent = message.message_text || "";
  li.appendChild(meta);
  li.appendChild(author);
  li.appendChild(body);
  return li;
}

async function loadOpsMessages(driverId) {
  if (!supToken || !opsMessagesListEl) return;
  if (!Number.isFinite(Number(driverId)) || Number(driverId) <= 0) {
    opsMessagesListEl.innerHTML = "<li>Selecione um motorista.</li>";
    return;
  }
  try {
    const response = await fetch(`${API_BASE}/supervisor/messages?driverId=${encodeURIComponent(driverId)}`, {
      headers: getAuthHeaders(),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      opsMessagesListEl.innerHTML = `<li>${data.message || "Erro ao listar mensagens."}</li>`;
      return;
    }
    if (opsConversationTitleEl) {
      opsConversationTitleEl.textContent = `Conversa com ${data.driver?.name || "motorista"} (Mec. ${data.driver?.mechanic_number || "-"})`;
    }
    const list = Array.isArray(data.messages) ? data.messages : [];
    opsMessagesListEl.innerHTML = "";
    if (!list.length) {
      opsMessagesListEl.innerHTML = "<li>Sem mensagens para este motorista.</li>";
      return;
    }
    const chronological = [...list].reverse();
    for (const item of chronological) {
      opsMessagesListEl.appendChild(buildOpsMessageItem(item));
      if (!item.read_at && currentSupervisorUserId && Number(item.to_user_id) === Number(currentSupervisorUserId)) {
        fetch(`${API_BASE}/supervisor/messages/${item.id}/read`, {
          method: "PATCH",
          headers: getAuthHeaders(),
        }).catch(() => {});
      }
    }
  } catch (_error) {
    opsMessagesListEl.innerHTML = "<li>Erro de ligação ao carregar mensagens.</li>";
  }
}

async function sendSupervisorMessage(event) {
  event.preventDefault();
  if (!supToken) return;
  if (!selectedOpsDriverId) {
    alert("Selecione um motorista na lista de conversas.");
    return;
  }
  const message = String(supMessageTextEl?.value || "").trim();
  if (!message) {
    alert("Escreva a mensagem.");
    return;
  }
  const relatedRaw = Number(supMessageRelatedServiceIdEl?.value);
  const payload = {
    driverId: selectedOpsDriverId,
    message,
    presetCode: String(supMessagePresetEl?.value || "").trim() || null,
    isTrafficAlert: supMessageTrafficAlertEl?.checked === true,
    relatedServiceId: Number.isFinite(relatedRaw) && relatedRaw > 0 ? relatedRaw : null,
  };
  try {
    const response = await fetch(`${API_BASE}/supervisor/messages`, {
      method: "POST",
      headers: getAuthHeaders(),
      body: JSON.stringify(payload),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      alert(data.message || "Erro ao enviar mensagem ao motorista.");
      return;
    }
    if (supMessageTextEl) supMessageTextEl.value = "";
    if (supMessagePresetEl) supMessagePresetEl.value = "";
    if (supMessageTrafficAlertEl) supMessageTrafficAlertEl.checked = false;
    await loadOpsMessages(selectedOpsDriverId);
    await loadOpsThreads();
    alert("Mensagem enviada ao motorista.");
  } catch (_error) {
    alert("Erro de ligação ao enviar mensagem.");
  }
}

async function loadOverview() {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/supervisor/overview`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao carregar resumo.");
    return;
  }

  const n = (v, decimals = 3) => Number(v ?? 0).toFixed(decimals);
  overviewEl.textContent = [
    `Data do resumo: ${data.report_date || "—"}`,
    `Previstos na escala (hoje): ${data.planned_roster_count ?? 0}`,
    `Realizados na escala (conclusão da viagem neste dia): ${data.realized_roster_slots ?? 0}`,
    `Não realizados (diferença em quantidade): ${data.not_realized_count ?? 0}`,
    "— Serviços na aplicação (com linha na escala deste dia; actividade neste dia civil) —",
    `Serviços registados (início ou conclusão neste dia): ${data.total_services}`,
    `Concluídos (data de fim da viagem neste dia): ${data.completed_services}`,
    `Em curso ou em transferência (início neste dia): ${data.in_progress_services}`,
    `Quilómetros em carga (concluídos): ${n(data.total_km)}`,
    `Quilómetros em vazio: ${n(data.deadhead_km)}`,
    `Quilómetros totais (carga + vazio): ${n(data.total_km_with_deadhead)}`,
    `Média de quilómetros (concluídos hoje): ${n(data.avg_km)}`,
    `Quilómetros previstos estimados (previstos × média de concluídos nos últimos 12 meses): ${n(data.estimated_planned_km_today)}`,
    `Quilómetros não realizados (estimativa: previsto estimado − realizados): ${n(data.km_not_realized_estimate)}`,
  ].join("\n");
}

async function loadServices(event) {
  if (event) event.preventDefault();
  if (!supToken) return;

  activeQueryString = buildQueryString();
  const response = await fetch(`${API_BASE}/supervisor/services${activeQueryString}`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao listar serviços.");
    return;
  }

  let filteredData = data;
  if (filterMode === "origin") {
    const originFilter = document.getElementById("fOrigin").value.trim().toLowerCase();
    if (originFilter) {
      filteredData = filteredData.filter((s) =>
        String(s.plate_number || "").toLowerCase().includes(originFilter)
      );
    }
  } else {
    const serviceIdFilter = document.getElementById("fServiceId").value.trim();
    if (serviceIdFilter) {
      filteredData = filteredData.filter((s) => String(s.id) === serviceIdFilter);
    }
  }
  currentServicesCache = filteredData;

  serviceCardsEl.innerHTML = "";
  filteredData.forEach((s) => {
    const delayMinutes = computeDelayMinutes(s);
    const delayClass = getDelayClass(delayMinutes);
    const expectedStart = s.started_at ? formatDate(s.started_at) : "-";
    const executionDate = s.started_at ? formatDateOnly(s.started_at) : "-";
    const chegada = s.ended_at ? formatDate(s.ended_at) : "-";
    const card = document.createElement("article");
    card.className = "service-card-item";
    card.innerHTML = `
      <div class="service-card-head">
        <strong>Serviço #${s.id}</strong>
        <span class="delay-pill ${delayClass}">${formatDelay(delayMinutes)}</span>
      </div>
      <div class="service-card-grid">
        <div><small>Motorista</small><div>${s.driver_name || "-"}</div></div>
        <div><small>Linha</small><div>${s.line_code || "-"}</div></div>
        <div><small>Data de execução</small><div>${executionDate}</div></div>
        <div><small>Chapa</small><div>${s.plate_number || "-"}</div></div>
        <div><small>Frota</small><div>${s.fleet_number || "-"}</div></div>
        <div><small>Início</small><div>${expectedStart}</div></div>
        <div><small>Chegada</small><div>${chegada}</div></div>
        <div><small>Quilómetros</small><div>${s.total_km || 0}</div></div>
        <div><small>Estado</small><div>${labelEstadoExecucaoServicoPt(s.status)}</div></div>
        <div><small>Fecho</small><div><span class="status-badge ${closeModeBadgeClass(s.close_mode)}">${labelCloseModePt(
      s.close_mode
    )}</span></div></div>
      </div>
      <div class="service-card-actions">
        <button type="button" class="adjust-btn" data-service-id="${s.id}">Abrir detalhe</button>
        <button type="button" class="stop-passages-shortcut-btn" data-stop-passages-service-id="${s.id}">Paragens</button>
      </div>
    `;
    serviceCardsEl.appendChild(card);
  });

  serviceCardsEl.querySelectorAll(".adjust-btn").forEach((btn) => {
    btn.addEventListener("click", () => {
      const serviceId = btn.getAttribute("data-service-id");
      const service = filteredData.find((s) => String(s.id) === String(serviceId));
      if (!service) return;
      openServiceDrawer(service);
    });
  });

  const counts = {
    completed: filteredData.filter((s) => s.status === "completed").length,
    inProgress: filteredData.filter((s) => s.status === "in_progress").length,
    waiting: filteredData.filter((s) => s.status === "awaiting_handover").length,
    cancelled: filteredData.filter((s) => s.status === "cancelled").length,
  };

  servicesPieEl.style.background = buildPieBackground([
    { value: counts.completed, color: "#16a34a" },
    { value: counts.inProgress, color: "#ea580c" },
    { value: counts.waiting, color: "#7c3aed" },
    { value: counts.cancelled, color: "#dc2626" },
  ]);
  servicesStatsTextEl.textContent =
    `Concluídos: ${counts.completed} | Em curso: ${counts.inProgress} | Aguardam transferência: ${counts.waiting} | Anulados: ${counts.cancelled}`;

  // Second chart kept as operational blocks-like summary.
  const total = filteredData.length;
  const executed = counts.completed;
  const remaining = Math.max(total - executed, 0);
  blocksPieEl.style.background = buildPieBackground([
    { value: executed, color: "#2563eb" },
    { value: remaining, color: "#f59e0b" },
  ]);
  blocksStatsTextEl.textContent = `Total: ${total} | Realizados: ${executed} | Por realizar: ${remaining}`;
  renderServicesDailySummary(filteredData);
}

async function exportCsv() {
  if (!supToken) {
    alert("Inicie sessão como supervisor ou administrador.");
    return;
  }

  if (!activeQueryString) {
    activeQueryString = buildQueryString();
  }

  const response = await fetch(
    `${API_BASE}/supervisor/services/export.csv${activeQueryString}`,
    { headers: { Authorization: `Bearer ${supToken}` } }
  );

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    alert(error.message || "Erro ao exportar CSV.");
    return;
  }

  const csvText = await response.text();
  const blob = new Blob([csvText], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "servicos.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function exportExcelServices() {
  if (!supToken) {
    alert("Inicie sessão como supervisor ou administrador.");
    return;
  }

  if (!activeQueryString) {
    activeQueryString = buildQueryString();
  }

  const response = await fetch(
    `${API_BASE}/supervisor/services/export.xlsx${activeQueryString}`,
    { headers: { Authorization: `Bearer ${supToken}` } }
  );

  if (!response.ok) {
    const error = await response.json().catch(() => ({}));
    alert(error.message || "Erro ao exportar Excel.");
    return;
  }

  const buffer = await response.arrayBuffer();
  const blob = new Blob([buffer], {
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "servicos.xlsx";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function createDriver(event) {
  event.preventDefault();
  if (!supToken) return;
  const payload = {
    name: document.getElementById("dName").value.trim(),
    username: document.getElementById("dUsername").value.trim(),
    email: document.getElementById("dEmail").value.trim(),
    mechanicNumber: document.getElementById("dMechanic").value.trim(),
    password: document.getElementById("dPassword").value.trim(),
    companyName: document.getElementById("dCompany").value.trim() || null,
    isActive: document.getElementById("dIsActive").value === "true",
  };

  const response = await fetch(`${API_BASE}/supervisor/drivers`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao criar motorista.");
    return;
  }
  alert(`Motorista ${data.name} criado com sucesso.`);
  await loadDrivers();
}

async function createAccessUser(event) {
  event.preventDefault();
  if (!supToken) return;
  const payload = {
    name: document.getElementById("aName").value.trim(),
    username: document.getElementById("aUsername").value.trim(),
    email: document.getElementById("aEmail").value.trim(),
    password: document.getElementById("aPassword").value.trim(),
    role: document.getElementById("aRole").value,
    isActive: document.getElementById("aIsActive").value === "true",
  };

  const response = await fetch(`${API_BASE}/supervisor/users`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao criar utilizador de acesso.");
    return;
  }
  alert(`Utilizador ${data.username} (${labelPerfilAcessoPt(data.role)}) criado com sucesso.`);
}

async function resetUserPassword({ username, newPassword, allowedRoles, activateUser }) {
  if (!supToken) return { ok: false, message: "Sessao expirada." };
  const response = await fetch(`${API_BASE}/supervisor/users/password-reset`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({ username, newPassword, allowedRoles, activateUser }),
  });
  const data = await response.json().catch(() => ({}));
  return {
    ok: response.ok,
    message: data.message || (response.ok ? "Password atualizada." : "Erro ao atualizar password."),
    user: data.user || null,
  };
}

async function resetDriverPassword(event) {
  event.preventDefault();
  const username = document.getElementById("rpDriverUsername").value.trim();
  const newPassword = document.getElementById("rpDriverPassword").value.trim();
  const activateUser = document.getElementById("rpDriverActivate").checked;
  if (!username || !newPassword) {
    alert("Preencha username e nova palavra-passe.");
    return;
  }
  const result = await resetUserPassword({
    username,
    newPassword,
    allowedRoles: ["driver"],
    activateUser,
  });
  if (!result.ok) {
    alert(result.message);
    return;
  }
  alert(`Password do motorista ${result.user?.username || username} atualizada com sucesso.`);
  document.getElementById("driverPasswordResetForm").reset();
  await loadDrivers();
}

async function resetAccessPassword(event) {
  event.preventDefault();
  const username = document.getElementById("rpAccessUsername").value.trim();
  const newPassword = document.getElementById("rpAccessPassword").value.trim();
  const activateUser = document.getElementById("rpAccessActivate").checked;
  if (!username || !newPassword) {
    alert("Preencha username e nova palavra-passe.");
    return;
  }
  const result = await resetUserPassword({
    username,
    newPassword,
    allowedRoles: ["viewer", "supervisor", "admin"],
    activateUser,
  });
  if (!result.ok) {
    alert(result.message);
    return;
  }
  alert(`Password do utilizador ${result.user?.username || username} atualizada com sucesso.`);
  document.getElementById("accessPasswordResetForm").reset();
}

async function importDrivers() {
  if (!supToken) return;
  const csvText = document.getElementById("driversCsvText").value;
  const defaultCompany = document.getElementById("driversDefaultCompany").value.trim();
  const fileInput = document.getElementById("driversFileInput");
  const file = fileInput.files && fileInput.files[0];

  let payload = { csvText, defaultCompany };
  if (file) {
    const fileBuffer = await file.arrayBuffer();
    const bytes = new Uint8Array(fileBuffer);
    let binary = "";
    bytes.forEach((b) => {
      binary += String.fromCharCode(b);
    });
    payload = {
      defaultCompany,
      fileType: file.name.toLowerCase().endsWith(".xlsx") ? "xlsx" : "csv",
      fileBase64: btoa(binary),
    };
  }

  const response = await fetch(`${API_BASE}/supervisor/drivers/import`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao importar motoristas.");
    importDriversReportEl.textContent = data.message || "Erro na importacao.";
    return;
  }

  const lines = [
    `Importacao concluida: inseridos=${data.inserted}, atualizados=${data.updated}, falhas=${data.failed || 0}`,
  ];
  (data.rowReports || []).forEach((r) => {
    lines.push(
      `Linha ${r.line} | ${r.key} | ${r.action} | ${r.status} | ${r.message}`
    );
  });
  importDriversReportEl.textContent = lines.join("\n");
  alert(`Importacao concluida. Inseridos: ${data.inserted}, Atualizados: ${data.updated}, Falhas: ${data.failed || 0}`);
  await loadDrivers();
}

async function importRosterFile(dryRun = false) {
  if (!supToken) return;
  const fileInput = document.getElementById("rosterFileInput");
  const serviceDate = document.getElementById("rosterServiceDate").value;
  const file = fileInput.files && fileInput.files[0];
  if (!file) {
    alert("Seleciona um ficheiro CSV ou Excel da escala.");
    return;
  }

  const fileBuffer = await file.arrayBuffer();
  const bytes = new Uint8Array(fileBuffer);
  let binary = "";
  bytes.forEach((b) => {
    binary += String.fromCharCode(b);
  });

  const response = await fetch(`${API_BASE}/supervisor/roster/import`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify({
      fileBase64: btoa(binary),
      fileType: file.name.toLowerCase().endsWith(".xlsx") ? "xlsx" : "csv",
      serviceDate,
      dryRun,
    }),
  });
  const rawText = await response.text();
  let data = {};
  try {
    data = rawText ? JSON.parse(rawText) : {};
  } catch (_error) {
    data = { message: rawText || "Resposta invalida do servidor." };
  }
  if (!response.ok) {
    alert(data.message || `Erro ao importar ficheiro da escala (HTTP ${response.status}).`);
    importRosterReportEl.textContent = data.message || "Erro na importacao de escala.";
    return;
  }

  const lines = [
    `Escala ${dryRun ? "validada" : "importada"} para ${data.serviceDate} | lidas=${data.parsedLines} | inseridas=${data.inserted} | atualizadas=${data.updated} | ignoradas=${data.ignored || 0} | falhas=${data.failed}`,
  ];
  (data.rowReports || []).forEach((r) => {
    const extras = [
      r.plateNumber ? `Chapa ${r.plateNumber}` : null,
      r.fleetNumber ? `Frota ${r.fleetNumber}` : null,
      r.serviceSchedule ? `Horario ${r.serviceSchedule}` : null,
      r.kmsCarga != null && r.kmsCarga !== "-" ? `KmsCarga ${r.kmsCarga}` : null,
    ]
      .filter(Boolean)
      .join(" | ");
    lines.push(
      `Linha ${r.line} | Mec ${r.mechanicNumber} | Nome ${r.driverName || "-"} | Servico ${r.serviceCode} | ${r.action} | ${r.status} | ${r.message}${extras ? ` | ${extras}` : ""}`
    );
  });
  importRosterReportEl.textContent = lines.join("\n");
  alert(
    `${dryRun ? "Validacao" : "Importacao"} concluida. Inseridas: ${data.inserted}, Atualizadas: ${data.updated}, Falhas: ${data.failed}`
  );
}

async function importGtfsZip() {
  if (!supToken) return;
  const fileInput = document.getElementById("gtfsZipInput");
  const file = fileInput.files && fileInput.files[0];
  if (!file) {
    alert("Seleciona um ficheiro GTFS .zip.");
    return;
  }

  const fileBuffer = await file.arrayBuffer();
  const bytes = new Uint8Array(fileBuffer);
  let binary = "";
  bytes.forEach((b) => {
    binary += String.fromCharCode(b);
  });

  const feedKey = String(gtfsFeedKeyEl?.value || "")
    .trim()
    .toLowerCase()
    .replace(/[^a-z0-9_-]+/g, "-")
    .replace(/-+/g, "-")
    .replace(/^-|-$/g, "");
  const feedName = String(gtfsFeedNameEl?.value || "").trim();
  if (!feedKey) {
    alert("Indique a chave do feed GTFS (ex.: urbano_lisboa).");
    return;
  }

  const response = await fetch(`${API_BASE}/gtfs/import`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify({
      fileBase64: btoa(binary),
      feedKey,
      feedName: feedName || feedKey,
      fileName: file.name,
      replaceFeed: gtfsReplaceFeedEl?.checked !== false,
    }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao importar GTFS.");
    importGtfsReportEl.textContent = data.message || "Erro na importacao GTFS.";
    return;
  }

  importGtfsReportEl.textContent = [
    data.message || "GTFS importado.",
    `feed: ${data.feed?.feedKey || feedKey} (${data.feed?.feedName || feedName || feedKey})`,
    `routes: ${data.counts?.routes || 0}`,
    `trips: ${data.counts?.trips || 0}`,
    `shapes: ${data.counts?.shapes || 0}`,
    `stops: ${data.counts?.stops || 0}`,
    `stop_times: ${data.counts?.stopTimes || 0}`,
  ].join("\n");
  alert("GTFS importado com sucesso.");
  await loadGtfsFeeds();
  await loadGtfsEditorLines();
}

async function loadGtfsFeeds() {
  if (!supToken || !gtfsFeedsListEl) return;
  gtfsFeedsListEl.innerHTML = "<div>A carregar feeds GTFS...</div>";
  const response = await fetch(`${API_BASE}/gtfs/feeds`, { headers: getAuthHeaders() });
  const data = await response.json().catch(() => ([]));
  if (!response.ok) {
    gtfsFeedsListEl.innerHTML = `<div>${data.message || "Erro ao carregar feeds GTFS."}</div>`;
    return;
  }
  const rows = Array.isArray(data) ? data : [];
  if (!rows.length) {
    gtfsFeedsListEl.innerHTML = "<div>Sem feeds GTFS carregados.</div>";
    return;
  }
  if (!selectedGtfsFeedKey) {
    const firstActive = rows.find((r) => r.is_active === true);
    selectedGtfsFeedKey = String(firstActive?.feed_key || rows[0]?.feed_key || "");
  }
  if (!selectedGtfsAnalyticsFeedKey) {
    selectedGtfsAnalyticsFeedKey = selectedGtfsFeedKey;
  }
  if (gtfsEditorFeedSelectEl) {
    gtfsEditorFeedSelectEl.innerHTML = '<option value="">-- Feed ativo (automático) --</option>';
    rows.forEach((feed) => {
      const option = document.createElement("option");
      option.value = feed.feed_key;
      option.textContent = `${feed.feed_name || feed.feed_key} (${feed.is_active ? "ativo" : "inativo"})`;
      if (feed.feed_key === selectedGtfsFeedKey) option.selected = true;
      gtfsEditorFeedSelectEl.appendChild(option);
    });
  }
  if (gtfsAnalyticsFeedSelectEl) {
    gtfsAnalyticsFeedSelectEl.innerHTML = '<option value="">-- Feed ativo (automático) --</option>';
    rows.forEach((feed) => {
      const option = document.createElement("option");
      option.value = feed.feed_key;
      option.textContent = `${feed.feed_name || feed.feed_key} (${feed.is_active ? "ativo" : "inativo"})`;
      if (feed.feed_key === selectedGtfsAnalyticsFeedKey) option.selected = true;
      gtfsAnalyticsFeedSelectEl.appendChild(option);
    });
  }
  const selectedFeed = rows.find((r) => String(r.feed_key) === String(selectedGtfsFeedKey));
  if (gtfsEffectiveFromEl) gtfsEffectiveFromEl.value = selectedFeed?.gtfs_effective_from ? String(selectedFeed.gtfs_effective_from).slice(0, 10) : "";
  if (calendarEffectiveFromEl) {
    calendarEffectiveFromEl.value = selectedFeed?.calendar_effective_from
      ? String(selectedFeed.calendar_effective_from).slice(0, 10)
      : "";
  }
  gtfsFeedsListEl.innerHTML = "";
  rows.forEach((feed) => {
    const item = document.createElement("article");
    item.className = "service-card-item";
    const active = feed.is_active === true;
    item.innerHTML = `
      <div class="service-card-head">
        <strong>${feed.feed_name || feed.feed_key}</strong>
        <span class="status-badge ${active ? "status-completed" : "status-cancelled"}">${active ? "Ativo" : "Inativo"}</span>
      </div>
      <div class="service-card-grid">
        <div><small>Chave</small><div>${feed.feed_key || "-"}</div></div>
        <div><small>Origem</small><div>${feed.source_filename || "-"}</div></div>
        <div><small>Linhas</small><div>${feed.routes_count ?? 0}</div></div>
        <div><small>Trips</small><div>${feed.trips_count ?? 0}</div></div>
        <div><small>Atualizado</small><div>${feed.updated_at ? formatDate(feed.updated_at) : "-"}</div></div>
      </div>
      <div class="service-card-actions">
        <button type="button" class="${active ? "danger-btn" : "adjust-btn"}" data-gtfs-feed-toggle="${feed.feed_key}" data-gtfs-feed-active="${active ? "1" : "0"}">
          ${active ? "Desativar" : "Ativar"}
        </button>
      </div>
    `;
    gtfsFeedsListEl.appendChild(item);
  });
}

function initGtfsAnalyticsMap() {
  if (!gtfsAnalyticsMapEl || gtfsAnalyticsMap) return;
  gtfsAnalyticsMap = L.map(gtfsAnalyticsMapEl).setView([38.7223, -9.1393], 12);
  const primary = L.tileLayer(`${API_BASE}/map-tiles/{z}/{x}/{y}.png`, {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  });
  const fallback = L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
    maxZoom: 20,
    attribution: "&copy; OpenStreetMap contributors &copy; CARTO",
  });
  let tileErrors = 0;
  primary.on("tileerror", () => {
    tileErrors += 1;
    if (tileErrors < 4) return;
    if (gtfsAnalyticsMap.hasLayer(primary)) gtfsAnalyticsMap.removeLayer(primary);
    if (!gtfsAnalyticsMap.hasLayer(fallback)) fallback.addTo(gtfsAnalyticsMap);
  });
  primary.addTo(gtfsAnalyticsMap);
  gtfsAnalyticsRouteLayer = L.layerGroup().addTo(gtfsAnalyticsMap);
}

function renderGtfsAnalyticsSummary(text) {
  if (gtfsAnalyticsSummaryEl) gtfsAnalyticsSummaryEl.textContent = text || "";
}

function renderGtfsLineDetailSummary(text) {
  if (gtfsLineDetailSummaryEl) gtfsLineDetailSummaryEl.textContent = text || "";
}

function fillGtfsAnalyticsLineSelect(rows) {
  if (!gtfsAnalyticsLineSelectEl) return;
  gtfsAnalyticsLineSelectEl.innerHTML = '<option value="">-- Escolher linha da tabela --</option>';
  rows.forEach((row) => {
    const option = document.createElement("option");
    option.value = row.route_id;
    option.textContent = `${row.route_label || row.route_id} | trips ${row.trips_count || 0} | km/mês ${formatNumberPt(row.estimated_month_km, 1)}`;
    gtfsAnalyticsLineSelectEl.appendChild(option);
  });
}

function renderGtfsAnalyticsRows(rows) {
  if (!gtfsAnalyticsTableBodyEl) return;
  if (!Array.isArray(rows) || !rows.length) {
    gtfsAnalyticsTableBodyEl.innerHTML = '<tr><td colspan="11">Sem linhas para o feed selecionado.</td></tr>';
    fillGtfsAnalyticsLineSelect([]);
    return;
  }
  gtfsAnalyticsTableBodyEl.innerHTML = rows
    .map(
      (row) => `<tr>
        <td>${row.route_label || row.route_id}</td>
        <td>${row.trips_count || 0}</td>
        <td>${formatNumberPt(row.avg_trip_km, 2)}</td>
        <td>${row.weekday_trips || 0}</td>
        <td>${row.saturday_trips || 0}</td>
        <td>${row.sunday_trips || 0}</td>
        <td>${row.holiday_trips || 0}</td>
        <td>${formatNumberPt(row.estimated_month_km, 1)}</td>
        <td>${formatNumberPt(row.estimated_semester_km, 1)}</td>
        <td>${formatNumberPt(row.estimated_year_km, 1)}</td>
        <td><button type="button" class="adjust-btn" data-gtfs-analytics-route="${row.route_id}">Ver mapa</button></td>
      </tr>`
    )
    .join("");
  fillGtfsAnalyticsLineSelect(rows);
}

function renderGtfsTripOptions(trips) {
  if (!gtfsAnalyticsTripSelectEl) return;
  gtfsAnalyticsTripSelectEl.innerHTML = '<option value="">-- Escolher trip --</option>';
  trips.forEach((trip) => {
    const option = document.createElement("option");
    option.value = trip.trip_id;
    option.textContent = `${trip.trip_id} | headsign ${trip.trip_headsign || "-"} | paragens ${trip.stops_count || 0}`;
    gtfsAnalyticsTripSelectEl.appendChild(option);
  });
}

function drawGtfsAnalyticsTrip(trip) {
  initGtfsAnalyticsMap();
  if (!gtfsAnalyticsRouteLayer) return;
  gtfsAnalyticsRouteLayer.clearLayers();
  const points = Array.isArray(trip?.shape_points) ? trip.shape_points : [];
  if (!points.length) {
    renderGtfsLineDetailSummary("Trip sem shape desenhavel.");
    return;
  }
  const latLngs = points
    .map((p) => [Number(p.lat), Number(p.lng)])
    .filter((p) => Number.isFinite(p[0]) && Number.isFinite(p[1]));
  if (!latLngs.length) {
    renderGtfsLineDetailSummary("Trip sem coordenadas validas.");
    return;
  }
  const line = L.polyline(latLngs, { color: "#2563eb", weight: 5 }).addTo(gtfsAnalyticsRouteLayer);
  L.circleMarker(latLngs[0], { radius: 6, color: "#15803d", fillColor: "#22c55e", fillOpacity: 0.9 }).addTo(gtfsAnalyticsRouteLayer);
  L.circleMarker(latLngs[latLngs.length - 1], { radius: 6, color: "#991b1b", fillColor: "#ef4444", fillOpacity: 0.9 }).addTo(
    gtfsAnalyticsRouteLayer
  );
  if (gtfsAnalyticsMap) {
    gtfsAnalyticsMap.fitBounds(line.getBounds(), { padding: [20, 20] });
  }
  renderGtfsLineDetailSummary(
    [
      `Trip: ${trip.trip_id}`,
      `Headsign: ${trip.trip_headsign || "-"}`,
      `Direction: ${trip.direction_id ?? "-"}`,
      `Service ID: ${trip.service_id || "-"}`,
      `Paragens: ${trip.stops_count || 0}`,
      `Pontos shape: ${latLngs.length}`,
    ].join("\n")
  );
}

async function loadGtfsAnalyticsOverview() {
  if (!supToken || !gtfsAnalyticsTableBodyEl) return;
  const feedKey = String(gtfsAnalyticsFeedSelectEl?.value || selectedGtfsAnalyticsFeedKey || "").trim();
  selectedGtfsAnalyticsFeedKey = feedKey;
  renderGtfsAnalyticsSummary("A carregar análise GTFS...");
  gtfsAnalyticsTableBodyEl.innerHTML = '<tr><td colspan="11">A carregar...</td></tr>';
  const query = feedKey ? `?feedKey=${encodeURIComponent(feedKey)}` : "";
  const response = await fetch(`${API_BASE}/gtfs/analytics/overview${query}`, { headers: getAuthHeaders() });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    gtfsAnalyticsRowsCache = [];
    renderGtfsAnalyticsRows([]);
    renderGtfsAnalyticsSummary(data.message || "Erro ao carregar análise GTFS.");
    return;
  }
  const rows = Array.isArray(data.lines) ? data.lines : [];
  gtfsAnalyticsRowsCache = rows;
  renderGtfsAnalyticsRows(rows);
  if (!rows.length) {
    gtfsAnalyticsSelectedRouteId = "";
  }
  renderGtfsAnalyticsSummary(
    [
      `Linhas analisadas: ${rows.length}`,
      `Pressupostos: dias úteis/mês ${data.assumptions?.monthWeekdays ?? 22}, sábados ${data.assumptions?.monthSaturdays ?? 4}, domingos ${data.assumptions?.monthSundays ?? 4}, feriados ${data.assumptions?.monthHolidays ?? 1}`,
    ].join("\n")
  );
}

async function loadGtfsLineDetail(routeIdRaw) {
  if (!supToken) return;
  const routeId = String(routeIdRaw || gtfsAnalyticsLineSelectEl?.value || "").trim();
  if (!routeId) {
    alert("Escolha uma linha para ver o mapa.");
    return;
  }
  renderGtfsLineDetailSummary(`A carregar detalhe da linha ${routeId}...`);
  const response = await fetch(`${API_BASE}/gtfs/analytics/line-detail?routeId=${encodeURIComponent(routeId)}`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    gtfsLineTripsCache = [];
    renderGtfsTripOptions([]);
    renderGtfsLineDetailSummary(data.message || "Erro ao carregar detalhe da linha.");
    return;
  }
  const trips = Array.isArray(data.trips) ? data.trips : [];
  gtfsAnalyticsSelectedRouteId = routeId;
  gtfsLineTripsCache = trips;
  renderGtfsTripOptions(trips);
  if (gtfsAnalyticsLineSelectEl) gtfsAnalyticsLineSelectEl.value = routeId;
  if (!trips.length) {
    renderGtfsLineDetailSummary("Linha sem trips desenhaveis.");
    return;
  }
  if (gtfsAnalyticsTripSelectEl) gtfsAnalyticsTripSelectEl.value = trips[0].trip_id;
  drawGtfsAnalyticsTrip(trips[0]);
}

async function exportGtfsAnalyticsExcel() {
  if (!supToken) return;
  const feedKey = String(gtfsAnalyticsFeedSelectEl?.value || selectedGtfsAnalyticsFeedKey || "").trim();
  const routeId = String(gtfsAnalyticsLineSelectEl?.value || gtfsAnalyticsSelectedRouteId || "").trim();
  const params = new URLSearchParams();
  if (feedKey) params.set("feedKey", feedKey);
  if (routeId) params.set("routeId", routeId);
  const query = params.toString();
  const response = await fetch(`${API_BASE}/gtfs/analytics/export.xlsx${query ? `?${query}` : ""}`, {
    headers: getAuthHeaders(),
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    alert(data.message || "Erro ao exportar Excel detalhado de GTFS.");
    return;
  }
  const buffer = await response.arrayBuffer();
  const blob = new Blob([buffer], {
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = routeId ? `gtfs_analise_${routeId}.xlsx` : "gtfs_analise_detalhada.xlsx";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function toggleGtfsFeed(feedKey, currentlyActive) {
  if (!supToken || !feedKey) return;
  const response = await fetch(`${API_BASE}/gtfs/feeds/${encodeURIComponent(feedKey)}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({ isActive: !currentlyActive }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao atualizar feed GTFS.");
    return;
  }
  await loadGtfsFeeds();
  await loadGtfsCalendars();
  await loadGtfsEditorLines();
}

async function saveGtfsEffectiveDates() {
  if (!supToken || !selectedGtfsFeedKey) {
    alert("Selecione um feed GTFS.");
    return;
  }
  const response = await fetch(`${API_BASE}/gtfs/feeds/${encodeURIComponent(selectedGtfsFeedKey)}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({
      gtfsEffectiveFrom: gtfsEffectiveFromEl?.value || null,
      calendarEffectiveFrom: calendarEffectiveFromEl?.value || null,
    }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao guardar datas de entrada em vigor.");
    return;
  }
  alert("Datas de entrada em vigor guardadas.");
  await loadGtfsFeeds();
}

async function exportGtfsModified() {
  if (!supToken || !selectedGtfsFeedKey) {
    alert("Selecione um feed GTFS.");
    return;
  }
  const response = await fetch(`${API_BASE}/gtfs/feeds/${encodeURIComponent(selectedGtfsFeedKey)}/export.zip`, {
    headers: getAuthHeaders(),
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    alert(data.message || "Erro ao exportar GTFS modificado.");
    return;
  }
  const buffer = await response.arrayBuffer();
  const blob = new Blob([buffer], { type: "application/zip" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = `gtfs_${selectedGtfsFeedKey}_modified.zip`;
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

function renderGtfsCalendars(rows) {
  if (!gtfsCalendarsListEl) return;
  gtfsCalendarsListEl.innerHTML = "";
  if (!Array.isArray(rows) || !rows.length) {
    gtfsCalendarsListEl.innerHTML = "<div>Sem calendários para este feed.</div>";
    return;
  }
  rows.forEach((row) => {
    const item = document.createElement("article");
    item.className = "service-card-item";
    item.innerHTML = `
      <div class="service-card-head">
        <strong>${row.service_id || "-"}</strong>
        <span class="status-badge ${row.is_active ? "status-completed" : "status-cancelled"}">${row.is_active ? "Ativo" : "Inativo"}</span>
      </div>
      <div class="service-card-grid">
        <div><small>Início</small><div><input type="date" data-cal-start value="${row.start_date ? String(row.start_date).slice(0, 10) : ""}" /></div></div>
        <div><small>Fim</small><div><input type="date" data-cal-end value="${row.end_date ? String(row.end_date).slice(0, 10) : ""}" /></div></div>
        <div><small>Dias (0/1)</small><div>${row.monday}${row.tuesday}${row.wednesday}${row.thursday}${row.friday}${row.saturday}${row.sunday}</div></div>
      </div>
      <div class="service-card-actions">
        <button type="button" class="${row.is_active ? "danger-btn" : "adjust-btn"}" data-cal-toggle="${row.service_id}" data-cal-active="${row.is_active ? "1" : "0"}">
          ${row.is_active ? "Desativar calendário" : "Ativar calendário"}
        </button>
        <button type="button" class="adjust-btn" data-cal-save="${row.service_id}">Guardar datas</button>
      </div>
    `;
    gtfsCalendarsListEl.appendChild(item);
  });
}

async function loadGtfsCalendars() {
  if (!supToken || !gtfsCalendarsListEl || !selectedGtfsFeedKey) return;
  gtfsCalendarsListEl.innerHTML = "<div>A carregar calendários...</div>";
  const response = await fetch(`${API_BASE}/gtfs/feeds/${encodeURIComponent(selectedGtfsFeedKey)}/calendars`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => ([]));
  if (!response.ok) {
    gtfsCalendarsListEl.innerHTML = `<div>${data.message || "Erro ao carregar calendários."}</div>`;
    return;
  }
  renderGtfsCalendars(Array.isArray(data) ? data : []);
}

async function updateGtfsCalendar(serviceId, payload) {
  if (!supToken || !selectedGtfsFeedKey || !serviceId) return;
  const response = await fetch(
    `${API_BASE}/gtfs/feeds/${encodeURIComponent(selectedGtfsFeedKey)}/calendars/${encodeURIComponent(serviceId)}`,
    {
      method: "PATCH",
      headers: getAuthHeaders(),
      body: JSON.stringify(payload),
    }
  );
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao atualizar calendário.");
    return;
  }
  await loadGtfsCalendars();
}

function renderGtfsEditorSummary(text) {
  if (gtfsEditorSummaryEl) gtfsEditorSummaryEl.textContent = text || "";
}

async function loadGtfsEditorLines() {
  if (!supToken || !gtfsEditorRouteSelectEl) return;
  renderGtfsEditorSummary("A carregar linhas GTFS...");
  const query = selectedGtfsFeedKey ? `?feedKey=${encodeURIComponent(selectedGtfsFeedKey)}` : "";
  const response = await fetch(`${API_BASE}/gtfs/editor/lines${query}`, { headers: getAuthHeaders() });
  const data = await response.json().catch(() => ([]));
  if (!response.ok) {
    renderGtfsEditorSummary(data.message || "Erro ao carregar linhas GTFS.");
    return;
  }
  const rows = Array.isArray(data) ? data : [];
  gtfsEditorRouteSelectEl.innerHTML = '<option value="">-- Selecionar linha --</option>';
  rows.forEach((r) => {
    const option = document.createElement("option");
    option.value = r.route_id;
    const short = String(r.route_short_name || "").trim();
    const long = String(r.route_long_name || "").trim();
    option.textContent = `[${r.feed_key || "default"}] ${short || r.route_id}${long ? ` - ${long}` : ""} | trips ${r.trips_count || 0} | paragens ${r.stops_count || 0}`;
    gtfsEditorRouteSelectEl.appendChild(option);
  });
  if (gtfsEditorTripSelectEl) {
    gtfsEditorTripSelectEl.innerHTML = '<option value="">-- Selecionar trip --</option>';
  }
  if (gtfsEditorStopsListEl) gtfsEditorStopsListEl.innerHTML = "";
  renderGtfsEditorSummary(`Linhas GTFS carregadas: ${rows.length}`);
}

async function loadGtfsEditorTripsByRoute() {
  if (!supToken || !gtfsEditorRouteSelectEl || !gtfsEditorTripSelectEl) return;
  const routeId = String(gtfsEditorRouteSelectEl.value || "").trim();
  gtfsEditorTripSelectEl.innerHTML = '<option value="">-- Selecionar trip --</option>';
  if (!routeId) {
    renderGtfsEditorSummary("Selecione uma linha GTFS para listar trips.");
    return;
  }
  renderGtfsEditorSummary(`A carregar trips da linha ${routeId}...`);
  const response = await fetch(`${API_BASE}/gtfs/editor/trips?routeId=${encodeURIComponent(routeId)}`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => ([]));
  if (!response.ok) {
    renderGtfsEditorSummary(data.message || "Erro ao carregar trips GTFS.");
    return;
  }
  const rows = Array.isArray(data) ? data : [];
  rows.forEach((t) => {
    const option = document.createElement("option");
    option.value = t.trip_id;
    option.textContent = `${t.trip_id} | headsign ${t.trip_headsign || "-"} | dir ${t.direction_id ?? "-"} | paragens ${t.stops_count || 0}`;
    gtfsEditorTripSelectEl.appendChild(option);
  });
  if (gtfsEditorStopsListEl) gtfsEditorStopsListEl.innerHTML = "";
  renderGtfsEditorSummary(`Trips da linha ${routeId}: ${rows.length}`);
}

function renderGtfsEditorStops(rows) {
  if (!gtfsEditorStopsListEl) return;
  gtfsEditorStopsListEl.innerHTML = "";
  if (!Array.isArray(rows) || !rows.length) {
    gtfsEditorStopsListEl.innerHTML = "<div>Sem paragens para esta trip.</div>";
    return;
  }
  rows.forEach((stop) => {
    const article = document.createElement("article");
    article.className = "service-card-item";
    article.innerHTML = `
      <div class="service-card-head">
        <strong>#${stop.stop_sequence} ${stop.stop_name || stop.stop_id || "-"}</strong>
      </div>
      <div class="service-card-grid">
        <div><small>stop_id</small><div>${stop.stop_id || "-"}</div></div>
        <div><small>Hora chegada</small><div>${stop.arrival_time || "-"}</div></div>
        <div><small>Hora partida</small><div>${stop.departure_time || "-"}</div></div>
        <div><small>Coordenadas</small><div>${stop.stop_lat ?? "-"}, ${stop.stop_lon ?? "-"}</div></div>
      </div>
      <div class="service-card-actions">
        <button type="button" class="danger-btn" data-gtfs-remove-seq="${stop.stop_sequence}">Remover</button>
      </div>
    `;
    gtfsEditorStopsListEl.appendChild(article);
  });
}

async function loadGtfsEditorTripStops() {
  if (!supToken || !gtfsEditorTripSelectEl) return;
  const tripId = String(gtfsEditorTripSelectEl.value || "").trim();
  if (!tripId) {
    renderGtfsEditorSummary("Selecione uma trip GTFS.");
    return;
  }
  renderGtfsEditorSummary(`A carregar paragens da trip ${tripId}...`);
  const response = await fetch(`${API_BASE}/gtfs/editor/trip-stops?tripId=${encodeURIComponent(tripId)}`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    renderGtfsEditorSummary(data.message || "Erro ao carregar paragens da trip.");
    if (gtfsEditorStopsListEl) gtfsEditorStopsListEl.innerHTML = "";
    return;
  }
  renderGtfsEditorStops(data.stops || []);
  renderGtfsEditorSummary(
    `Trip ${data.trip?.trip_id || tripId} | route ${data.trip?.route_id || "-"} | headsign ${data.trip?.trip_headsign || "-"} | paragens ${Array.isArray(data.stops) ? data.stops.length : 0}`
  );
}

async function addGtfsEditorStop(event) {
  event.preventDefault();
  if (!supToken || !gtfsEditorTripSelectEl) return;
  const tripId = String(gtfsEditorTripSelectEl.value || "").trim();
  if (!tripId) {
    alert("Selecione uma trip GTFS antes de adicionar paragem.");
    return;
  }
  const stopId = String(document.getElementById("gtfsEditorAddStopId")?.value || "").trim();
  const stopName = String(document.getElementById("gtfsEditorAddStopName")?.value || "").trim();
  const stopLat = String(document.getElementById("gtfsEditorAddStopLat")?.value || "").trim();
  const stopLon = String(document.getElementById("gtfsEditorAddStopLon")?.value || "").trim();
  const stopSequence = String(document.getElementById("gtfsEditorAddStopSequence")?.value || "").trim();
  const arrivalTime = String(document.getElementById("gtfsEditorAddArrivalTime")?.value || "").trim();
  const departureTime = String(document.getElementById("gtfsEditorAddDepartureTime")?.value || "").trim();

  const payload = {
    tripId,
    applyScope: String(gtfsEditorApplyScopeEl?.value || "trip").trim().toLowerCase(),
    stopId: stopId || null,
    stopName: stopName || null,
    stopLat: stopLat || null,
    stopLon: stopLon || null,
    stopSequence: stopSequence || null,
    arrivalTime: arrivalTime || null,
    departureTime: departureTime || null,
  };
  const response = await fetch(`${API_BASE}/gtfs/editor/trip-stops`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao adicionar paragem GTFS.");
    return;
  }
  alert(data.message || "Paragem adicionada.");
  event.target.reset();
  await loadGtfsEditorTripStops();
}

async function removeGtfsEditorStop(stopSequence) {
  if (!supToken || !gtfsEditorTripSelectEl) return;
  const tripId = String(gtfsEditorTripSelectEl.value || "").trim();
  if (!tripId) return;
  const applyScope = String(gtfsEditorApplyScopeEl?.value || "trip").trim().toLowerCase();
  const targetLabel =
    applyScope === "route"
      ? "de todas as trips da carreira (linha da trip selecionada)"
      : `da trip ${tripId}`;
  const confirmed = window.confirm(`Remover paragem da sequência ${stopSequence} ${targetLabel}?`);
  if (!confirmed) return;
  const response = await fetch(
    `${API_BASE}/gtfs/editor/trip-stops?tripId=${encodeURIComponent(tripId)}&stopSequence=${encodeURIComponent(stopSequence)}&applyScope=${encodeURIComponent(applyScope)}`,
    {
      method: "DELETE",
      headers: getAuthHeaders(),
    }
  );
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao remover paragem GTFS.");
    return;
  }
  alert(data.message || "Paragem removida.");
  await loadGtfsEditorTripStops();
}

async function downloadDriversTemplateCsv() {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/supervisor/drivers/import-template.csv`, {
    headers: { Authorization: `Bearer ${supToken}` },
  });
  if (!response.ok) {
    alert("Erro ao baixar template.");
    return;
  }
  const csvText = await response.text();
  const blob = new Blob([csvText], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "template_importacao_motoristas.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function downloadDriversTemplateXlsx() {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/supervisor/drivers/import-template.xlsx`, {
    headers: { Authorization: `Bearer ${supToken}` },
  });
  if (!response.ok) {
    alert("Erro ao baixar template Excel.");
    return;
  }
  const buffer = await response.arrayBuffer();
  const blob = new Blob([buffer], {
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "template_importacao_motoristas.xlsx";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function exportDriversCsv() {
  if (!supToken) return;
  const company = document.getElementById("driversExportCompany").value.trim();
  const query = company ? `?company=${encodeURIComponent(company)}` : "";
  const response = await fetch(`${API_BASE}/supervisor/drivers/export.csv${query}`, {
    headers: { Authorization: `Bearer ${supToken}` },
  });
  if (!response.ok) {
    alert("Erro ao exportar motoristas.");
    return;
  }
  const csvText = await response.text();
  const blob = new Blob([csvText], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "motoristas.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function exportDriversXlsx() {
  if (!supToken) return;
  const company = document.getElementById("driversExportCompany").value.trim();
  const query = company ? `?company=${encodeURIComponent(company)}` : "";
  const response = await fetch(`${API_BASE}/supervisor/drivers/export.xlsx${query}`, {
    headers: { Authorization: `Bearer ${supToken}` },
  });
  if (!response.ok) {
    alert("Erro ao exportar motoristas em Excel.");
    return;
  }
  const buffer = await response.arrayBuffer();
  const blob = new Blob([buffer], {
    type: "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
  });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "motoristas.xlsx";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

async function toggleDriverActive(driverId, currentActive) {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/supervisor/drivers/${driverId}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({ isActive: !currentActive }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao atualizar estado do motorista.");
    return;
  }
  await loadDrivers();
}

async function toggleAccessUserActive(userId, currentActive) {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/supervisor/users/${userId}`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({ isActive: !currentActive }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao atualizar estado do utilizador.");
    return;
  }
  await loadDrivers();
}

let inlineResetUserId = null;

function renderUsersList() {
  driversListEl.innerHTML = "";
  usersCache.forEach((d) => {
    const role = String(d.role || "").toLowerCase();
    const li = document.createElement("li");
    li.className = "user-row-item";

    const summary = document.createElement("div");
    summary.className = "user-row-summary";
    summary.textContent = `${d.name} | Utilizador ${d.username || "-"} | Perfil ${labelPerfilAcessoPt(role)} | Mec. ${
      d.mechanic_number || "-"
    } | ${d.email || "-"} | Empresa ${d.company_name || "-"} | ${d.is_active ? "Ativo" : "Nao ativo"}`;
    li.appendChild(summary);

    const actions = document.createElement("div");
    actions.className = "user-row-actions";

    const resetBtn = document.createElement("button");
    resetBtn.type = "button";
    resetBtn.textContent = inlineResetUserId === d.id ? "Cancelar reset" : "Reset password";
    resetBtn.addEventListener("click", () => {
      inlineResetUserId = inlineResetUserId === d.id ? null : d.id;
      renderUsersList();
    });
    actions.appendChild(resetBtn);

    if (role === "driver") {
      const toggleBtn = document.createElement("button");
      toggleBtn.type = "button";
      toggleBtn.textContent = d.is_active ? "Desativar" : "Ativar";
      toggleBtn.addEventListener("click", () => toggleDriverActive(d.id, d.is_active));
      actions.appendChild(toggleBtn);
    } else if (["viewer", "viewr", "supervisor", "admin"].includes(role)) {
      const toggleBtn = document.createElement("button");
      toggleBtn.type = "button";
      toggleBtn.textContent = d.is_active ? "Desativar" : "Ativar";
      toggleBtn.addEventListener("click", () => toggleAccessUserActive(d.id, d.is_active));
      actions.appendChild(toggleBtn);
    }
    li.appendChild(actions);

    if (inlineResetUserId === d.id) {
      const form = document.createElement("form");
      form.className = "inline-reset-form";
      form.innerHTML = `
        <input type="password" name="newPassword" placeholder="Nova palavra-passe" required />
        <label><input type="checkbox" name="activateUser" /> Ativar conta ao resetar</label>
        <button type="submit">Guardar nova password</button>
      `;
      form.addEventListener("submit", async (event) => {
        event.preventDefault();
        const fd = new FormData(form);
        const newPassword = String(fd.get("newPassword") || "").trim();
        const activateUser = fd.get("activateUser") === "on";
        const username = String(d.username || "").trim();
        if (!username || !newPassword) {
          alert("Preencha a nova palavra-passe.");
          return;
        }
        const allowedRoles = role === "driver" ? ["driver"] : ["viewer", "supervisor", "admin"];
        const result = await resetUserPassword({
          username,
          newPassword,
          allowedRoles,
          activateUser,
        });
        if (!result.ok) {
          alert(result.message);
          return;
        }
        alert(`Password de ${username} atualizada com sucesso.`);
        inlineResetUserId = null;
        await loadDrivers();
      });
      li.appendChild(form);
    }

    driversListEl.appendChild(li);
  });
}

async function loadStopPassages() {
  if (!supToken) {
    alert("Inicie sessão como supervisor ou administrador.");
    return;
  }
  const idInput = document.getElementById("stopPassageServiceId");
  const radiusInput = document.getElementById("stopPassageRadiusM");
  const summaryEl = document.getElementById("stopPassageSummary");
  const wrapEl = document.getElementById("stopPassageTableWrap");
  const tbody = document.getElementById("stopPassageTableBody");
  const sid = Number(idInput?.value);
  if (!Number.isFinite(sid) || sid <= 0) {
    alert("Indique o n.º do serviço (ID).");
    return;
  }
  let radius = Number(radiusInput?.value);
  if (!Number.isFinite(radius)) radius = 85;
  radius = Math.min(200, Math.max(40, radius));

  summaryEl.textContent = "A carregar...";
  wrapEl.classList.add("hidden");
  tbody.innerHTML = "";

  const response = await fetch(
    `${API_BASE}/supervisor/services/${sid}/stop-passages?radiusM=${encodeURIComponent(radius)}`,
    { headers: getAuthHeaders() }
  );
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    summaryEl.textContent = data.message || "Erro ao carregar a análise.";
    return;
  }

  const svc = data.service || {};
  summaryEl.textContent = [
    `Serviço #${svc.id} | ${svc.driver_name || "-"} | Linha ${svc.line_code || "-"} | ${svc.service_schedule || "-"}`,
    `Trip GTFS: ${data.trip_id || "-"} | Tolerância: ${data.threshold_meters ?? radius} m`,
    `Pontos GPS: ${data.gps_points_count ?? 0}`,
    `Paragens com passagem detetada: ${data.summary?.stops_matched ?? 0} / ${data.summary?.stops_total ?? 0} (${data.summary?.pct ?? 0}%)`,
    data.note ? String(data.note) : "",
  ]
    .filter((line) => line)
    .join("\n");

  (data.stops || []).forEach((row) => {
    const tr = document.createElement("tr");
    const cells = [
      row.stop_sequence ?? "—",
      row.stop_name || row.stop_id || "—",
      row.scheduled_departure || row.scheduled_arrival || "—",
      formatDateTimePt(row.passed_at),
      row.distance_from_stop_m != null ? Number(row.distance_from_stop_m).toFixed(1) : "—",
      row.passed_near_stop ? "Sim" : "Não",
    ];
    cells.forEach((text) => {
      const td = document.createElement("td");
      td.textContent = text;
      tr.appendChild(td);
    });
    tbody.appendChild(tr);
  });
  wrapEl.classList.remove("hidden");
}

async function loadDrivers() {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/supervisor/drivers`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json();
  if (!response.ok) return;

  usersCache = data;
  driversCache = data.filter((d) => String(d.role || "").toLowerCase() === "driver");
  if (inlineResetUserId != null && !usersCache.some((u) => Number(u.id) === Number(inlineResetUserId))) {
    inlineResetUserId = null;
  }
  renderUsersList();
}

function normalizeImeiDigits(value) {
  const digits = String(value || "").replace(/\D/g, "");
  return digits || "";
}

async function loadTrackerDevices() {
  if (!supToken || !trackerDevicesListEl) return;
  trackerDevicesListEl.innerHTML = "<li>A carregar dispositivos...</li>";
  const response = await fetch(`${API_BASE}/integrations/teltonika/devices`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => []);
  if (!response.ok) {
    trackerDevicesListEl.innerHTML = `<li>${data.message || "Erro ao carregar dispositivos Teltonika."}</li>`;
    return;
  }
  trackerDevicesCache = Array.isArray(data) ? data : [];
  trackerDevicesListEl.innerHTML = "";
  if (!trackerDevicesCache.length) {
    trackerDevicesListEl.innerHTML = "<li>Sem dispositivos configurados.</li>";
    return;
  }
  trackerDevicesCache.forEach((item) => {
    const li = document.createElement("li");
    li.className = "user-row-item";
    const isActive = asPgBool(item.is_active);
    li.innerHTML = `
      <div class="user-row-summary">
        IMEI ${item.imei} | Frota ${item.fleet_number || "-"} | Chapa ${item.plate_number || "-"} | ${
          isActive ? "Ativo" : "Inativo"
        }
      </div>
    `;
    const actions = document.createElement("div");
    actions.className = "user-row-actions";
    const toggleBtn = document.createElement("button");
    toggleBtn.type = "button";
    toggleBtn.textContent = isActive ? "Desativar" : "Ativar";
    toggleBtn.addEventListener("click", () => upsertTrackerDevice(item, !isActive));
    actions.appendChild(toggleBtn);
    li.appendChild(actions);
    trackerDevicesListEl.appendChild(li);
  });
}

function ensureDeadheadMap() {
  if (!deadheadMapEl || deadheadMap) return;
  deadheadMap = L.map(deadheadMapEl).setView([38.7223, -9.1393], 11);
  L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
    maxZoom: 19,
    attribution: "&copy; OpenStreetMap contributors",
  }).addTo(deadheadMap);
  deadheadRouteLayer = L.layerGroup().addTo(deadheadMap);
}

function formatDateTimePt(value) {
  if (!value) return "-";
  const dt = new Date(value);
  if (!Number.isFinite(dt.getTime())) return "-";
  return dt.toLocaleString("pt-PT");
}

async function loadDeadheadMovements() {
  if (!supToken || !deadheadMovementsListEl) return;
  deadheadMovementsListEl.innerHTML = '<article class="service-card-item">A carregar vazios...</article>';
  try {
    const params = new URLSearchParams();
    const from = String(deadheadFromDateEl?.value || "").trim();
    const to = String(deadheadToDateEl?.value || "").trim();
    const fleetNumber = String(deadheadFleetFilterEl?.value || "").trim();
    if (from) params.set("from", from);
    if (to) params.set("to", to);
    if (fleetNumber) params.set("fleetNumber", fleetNumber);
    const query = params.toString() ? `?${params.toString()}` : "";
    const response = await fetch(`${API_BASE}/integrations/deadhead-movements${query}`, { headers: getAuthHeaders() });
    const data = await response.json().catch(() => []);
    if (!response.ok) {
      deadheadMovementsListEl.innerHTML = `<article class="service-card-item">${data.message || "Erro ao carregar vazios."}</article>`;
      return;
    }
    const rows = Array.isArray(data) ? data : [];
    if (!rows.length) {
      deadheadMovementsListEl.innerHTML = '<article class="service-card-item">Sem movimentos em vazio registados.</article>';
      return;
    }
    deadheadMovementsListEl.innerHTML = rows
      .slice(0, 30)
      .map(
        (row) => `<article class="service-card-item">
          <strong>Vazio #${row.id} | Frota ${row.fleet_number || "-"} | Chapa ${row.plate_number || "-"}</strong>
          <div>Início: ${formatDateTimePt(row.started_at)}</div>
          <div>Fim: ${formatDateTimePt(row.ended_at)}</div>
          <div>Km: ${Number(row.total_km || 0).toFixed(3)}</div>
          <button type="button" data-deadhead-id="${row.id}">Ver percurso no mapa</button>
        </article>`
      )
      .join("");
  } catch (_error) {
    deadheadMovementsListEl.innerHTML = '<article class="service-card-item">Erro de rede ao carregar vazios.</article>';
  }
}

function buildDeadheadQueryString() {
  const params = new URLSearchParams();
  const from = String(deadheadFromDateEl?.value || "").trim();
  const to = String(deadheadToDateEl?.value || "").trim();
  const fleetNumber = String(deadheadFleetFilterEl?.value || "").trim();
  if (from) params.set("from", from);
  if (to) params.set("to", to);
  if (fleetNumber) params.set("fleetNumber", fleetNumber);
  return params.toString() ? `?${params.toString()}` : "";
}

async function exportDeadheadCsv() {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/integrations/deadhead-movements/export.csv${buildDeadheadQueryString()}`, {
    headers: getAuthHeaders(),
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    alert(data.message || "Erro ao exportar vazios CSV.");
    return;
  }
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "movimentos_vazio.csv";
  a.click();
  URL.revokeObjectURL(url);
}

async function exportDeadheadXlsx() {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/integrations/deadhead-movements/export.xlsx${buildDeadheadQueryString()}`, {
    headers: getAuthHeaders(),
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    alert(data.message || "Erro ao exportar vazios Excel.");
    return;
  }
  const blob = await response.blob();
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "movimentos_vazio.xlsx";
  a.click();
  URL.revokeObjectURL(url);
}

async function loadOdometerReconciliationReport() {
  if (!supToken || !odometerReconciliationListEl) return;
  const dateInputEl = document.getElementById("odometerReportDate");
  const reportDate = String(dateInputEl?.value || "").trim();
  if (!reportDate) {
    alert("Selecione a data do relatório.");
    return;
  }
  odometerReconciliationListEl.innerHTML = '<article class="service-card-item">A carregar conciliação...</article>';
  const response = await fetch(
    `${API_BASE}/integrations/odometer-reconciliation/daily?date=${encodeURIComponent(reportDate)}`,
    { headers: getAuthHeaders() }
  );
  const data = await response.json().catch(() => []);
  if (!response.ok) {
    odometerReconciliationListEl.innerHTML = `<article class="service-card-item">${data.message || "Erro ao carregar relatório."}</article>`;
    return;
  }
  const rows = Array.isArray(data) ? data : [];
  if (!rows.length) {
    odometerReconciliationListEl.innerHTML = '<article class="service-card-item">Sem dados para a data selecionada.</article>';
    return;
  }
  odometerReconciliationListEl.innerHTML = rows
    .map(
      (row) => `<article class="service-card-item">
        <strong>Frota ${row.fleet_number || "-"}</strong>
        <div>Matrícula: ${row.plate_number || "-"}</div>
        <div>Km app carga: ${Number(row.app_km_load || 0).toFixed(3)}</div>
        <div>Km app vazio: ${Number(row.app_km_deadhead || 0).toFixed(3)}</div>
        <div>Km app total: ${Number(row.app_km_total || 0).toFixed(3)}</div>
        <div>Km odómetro (dia): ${
          row.odometer_km_day == null && row.odometer_km_from_services == null
            ? "-"
            : Number(row.odometer_km_day ?? row.odometer_km_from_services).toFixed(3)
        }</div>
        <div>Diferença odómetro - app: ${
          row.odometer_vs_app_diff_km == null ? "-" : Number(row.odometer_vs_app_diff_km).toFixed(3)
        }</div>
      </article>`
    )
    .join("");
}

async function loadDeadheadMovementDetail(movementId) {
  if (!supToken || !Number.isFinite(Number(movementId))) return;
  ensureDeadheadMap();
  if (!deadheadMap || !deadheadRouteLayer) return;
  const response = await fetch(`${API_BASE}/integrations/deadhead-movements/${movementId}`, {
    headers: getAuthHeaders(),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao carregar percurso do vazio.");
    return;
  }
  const coords = data?.route_geojson?.geometry?.coordinates || [];
  const latLngs = coords.map((c) => [Number(c[1]), Number(c[0])]).filter((p) => Number.isFinite(p[0]) && Number.isFinite(p[1]));
  deadheadRouteLayer.clearLayers();
  if (!latLngs.length) {
    alert("Este vazio não tem percurso disponível.");
    return;
  }
  const line = L.polyline(latLngs, { color: "#f97316", weight: 4 }).addTo(deadheadRouteLayer);
  L.circleMarker(latLngs[0], { radius: 6, color: "#16a34a", weight: 2 }).addTo(deadheadRouteLayer);
  L.circleMarker(latLngs[latLngs.length - 1], { radius: 6, color: "#dc2626", weight: 2 }).addTo(deadheadRouteLayer);
  deadheadMap.fitBounds(line.getBounds(), { padding: [20, 20] });
}

async function loadGtfsRtPreview(kind) {
  if (!supToken || !gtfsRtPreviewReportEl) return;
  const map = {
    vehicles: "/gtfs-rt/vehicle-positions.json",
    tripUpdates: "/gtfs-rt/trip-updates.json",
    alerts: "/gtfs-rt/service-alerts.json",
  };
  const url = map[kind];
  if (!url) return;
  gtfsRtPreviewReportEl.textContent = `A carregar ${kind}...`;
  try {
    const response = await fetch(`${API_BASE}${url}`, {
      headers: getAuthHeaders(),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      gtfsRtPreviewReportEl.textContent = data.message || `Erro ao carregar preview GTFS-RT (${kind}).`;
      return;
    }
    const entities = Array.isArray(data?.entity) ? data.entity : [];
    gtfsRtPreviewReportEl.textContent = [
      `Feed: ${kind}`,
      `timestamp: ${data?.header?.timestamp || "-"}`,
      `entidades: ${entities.length}`,
      "",
      JSON.stringify(data, null, 2),
    ].join("\n");
  } catch (_error) {
    gtfsRtPreviewReportEl.textContent = `Erro de rede ao carregar ${kind}.`;
  }
}

async function upsertTrackerDevice(device, nextIsActive) {
  if (!supToken) return;
  const payload = {
    imei: normalizeImeiDigits(device.imei),
    fleetNumber: String(device.fleet_number || "").trim(),
    plateNumber: String(device.plate_number || "").trim(),
    isActive: Boolean(nextIsActive),
  };
  const response = await fetch(`${API_BASE}/integrations/teltonika/devices`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao atualizar dispositivo Teltonika.");
    return;
  }
  await loadTrackerDevices();
}

async function saveTrackerDevice(event) {
  event.preventDefault();
  if (!supToken) return;
  const imei = normalizeImeiDigits(document.getElementById("trackerImei").value);
  const fleetNumber = document.getElementById("trackerFleetNumber").value.trim();
  const plateNumber = document.getElementById("trackerPlateNumber").value.trim();
  const isActive = document.getElementById("trackerIsActive").value === "true";
  if (!imei) {
    alert("Indique um IMEI válido.");
    return;
  }
  if (!fleetNumber) {
    alert("Indique a frota da viatura.");
    return;
  }
  const response = await fetch(`${API_BASE}/integrations/teltonika/devices`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify({ imei, fleetNumber, plateNumber, isActive }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao guardar dispositivo Teltonika.");
    return;
  }
  trackerDeviceFormEl.reset();
  document.getElementById("trackerIsActive").value = "true";
  await loadTrackerDevices();
  alert(`Dispositivo ${data.imei} guardado com sucesso.`);
}

function formatVehicleKm(value) {
  const num = Number(value);
  if (!Number.isFinite(num)) return "-";
  return `${num.toFixed(1)} km`;
}

async function loadVehicleRegistry() {
  if (!supToken || !vehicleRegistryListEl) return;
  vehicleRegistryListEl.innerHTML = '<article class="service-card-item">A carregar viaturas...</article>';
  try {
    const response = await fetch(`${API_BASE}/integrations/teltonika/devices`, { headers: getAuthHeaders() });
    const data = await response.json().catch(() => []);
    if (!response.ok) {
      vehicleRegistryListEl.innerHTML = `<article class="service-card-item">${data.message || "Erro ao carregar viaturas."}</article>`;
      return;
    }
    const rows = Array.isArray(data) ? data : [];
    if (!rows.length) {
      vehicleRegistryListEl.innerHTML = '<article class="service-card-item">Sem viaturas registadas.</article>';
      return;
    }
    vehicleRegistryListEl.innerHTML = rows
      .map((item) => {
        const imei = String(item.imei || "");
        return `
          <article class="service-card-item">
            <strong>Frota ${item.fleet_number || "-"}</strong>
            <div>IMEI: ${imei || "-"}</div>
            <div>Matrícula: ${item.plate_number || "-"}</div>
            <div>Km instalação: ${formatVehicleKm(item.install_odometer_km)}</div>
            <div>Km atual: ${formatVehicleKm(item.current_odometer_km)}</div>
            <div>Estado: ${asPgBool(item.is_active) ? "Ativa" : "Inativa"}</div>
            <div class="stack-actions">
              <input type="number" step="0.1" min="0" id="vehicle-current-${imei}" placeholder="Novo km atual" />
              <button type="button" data-vehicle-imei="${imei}" class="vehicle-km-save-btn">Guardar km atual</button>
            </div>
          </article>
        `;
      })
      .join("");
  } catch (_error) {
    vehicleRegistryListEl.innerHTML = '<article class="service-card-item">Erro de rede ao carregar viaturas.</article>';
  }
}

async function saveVehicleRegistry(event) {
  event.preventDefault();
  if (!supToken || !vehicleRegistryFormEl) return;
  const imei = normalizeImeiDigits(document.getElementById("vehicleImei")?.value);
  const fleetNumber = document.getElementById("vehicleFleetNumber")?.value?.trim() || "";
  const plateNumber = document.getElementById("vehiclePlateNumber")?.value?.trim() || "";
  const installOdometerKmRaw = document.getElementById("vehicleInstallOdometerKm")?.value;
  const currentOdometerKmRaw = document.getElementById("vehicleCurrentOdometerKm")?.value;
  const isActive = document.getElementById("vehicleIsActive")?.value === "true";
  if (!imei || !fleetNumber) {
    alert("IMEI e nº de frota são obrigatórios.");
    return;
  }
  const payload = {
    imei,
    fleetNumber,
    plateNumber,
    isActive,
    installOdometerKm: installOdometerKmRaw === "" ? null : Number(installOdometerKmRaw),
    currentOdometerKm: currentOdometerKmRaw === "" ? null : Number(currentOdometerKmRaw),
  };
  const response = await fetch(`${API_BASE}/integrations/teltonika/devices`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify(payload),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao guardar viatura.");
    return;
  }
  vehicleRegistryFormEl.reset();
  document.getElementById("vehicleIsActive").value = "true";
  await loadVehicleRegistry();
}

async function updateVehicleCurrentKm(imei, currentOdometerKm) {
  if (!supToken) return;
  const response = await fetch(`${API_BASE}/integrations/teltonika/devices/${encodeURIComponent(imei)}/odometer`, {
    method: "PATCH",
    headers: getAuthHeaders(),
    body: JSON.stringify({ currentOdometerKm }),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok) {
    alert(data.message || "Erro ao atualizar km da viatura.");
    return;
  }
  await loadVehicleRegistry();
}

function renderRosterDayServiceRows(rows) {
  const container = document.getElementById("rosterDayCards");
  if (!container) return;
  container.innerHTML = "";
  if (!rows.length) {
    container.innerHTML = "<div>Este motorista não tem linhas de escala nesta data.</div>";
    return;
  }

  rows.forEach((row) => {
    const canReassign = rosterRowCanReassign(row);
    const blockReason = rosterRowBlockReason(row);
    const startLoc = row.start_location || "-";
    const endLoc = row.end_location || "-";
    const rs = String(row.roster_status || "").toLowerCase();
    const rosterBadgeClass =
      rs === "completed" ? "status-completed" : rs === "in_progress" ? "status-in_progress" : "status-other";
    const driverOptions = driversCache
      .filter(
        (d) =>
          d.is_active &&
          String(d.role || "").toLowerCase() === "driver" &&
          Number(d.id) !== Number(row.driver_id)
      )
      .map(
        (d) =>
          `<option value="${d.id}">${d.name} (Mec ${d.mechanic_number || "-"})</option>`
      )
      .join("");
    const noOtherActiveDrivers = canReassign && !driverOptions.trim();

    const card = document.createElement("article");
    card.className = "service-card-item";
    card.innerHTML = `
      <div class="service-card-head">
        <strong>${row.service_code || "Serviço"}</strong>
        <span class="status-badge ${rosterBadgeClass}">${labelEstadoEscalaPt(row.roster_status)}</span>
      </div>
      <div class="service-card-grid">
        <div><small>Data</small><div>${row.service_date ? formatDateOnly(row.service_date) : "-"}</div></div>
        <div><small>Horário</small><div>${row.service_schedule || "-"}</div></div>
        <div><small>Linha</small><div>${row.line_code || "-"}</div></div>
        <div><small>Frota</small><div>${row.fleet_number || "-"}</div></div>
        <div><small>Km carga</small><div>${row.kms_carga == null ? "-" : Number(row.kms_carga).toFixed(3)}</div></div>
        <div><small>Chapa</small><div>${row.plate_number || "-"}</div></div>
        <div><small>Motorista</small><div>${row.driver_name || "-"} (Mec. ${row.driver_mechanic_number || "-"})</div></div>
        <div><small>Origem / destino</small><div>${startLoc} → ${endLoc}</div></div>
      </div>
      ${
        noOtherActiveDrivers
          ? `<p class="field-help">Não há outros motoristas <strong>activos</strong> no sistema para escolher na reatribuição. Vá ao separador de motoristas, confirme que existem utilizadores com perfil motorista e que estão activos, e carregue em «Atualizar escala».</p>`
          : ""
      }
      ${
        canReassign && !noOtherActiveDrivers
          ? `<form class="roster-reassign-form" data-roster-id="${row.roster_id}">
        <label for="newDriver_${row.roster_id}">Novo motorista</label>
        <select id="newDriver_${row.roster_id}" name="newDriverId" required>
          <option value="">— Selecionar —</option>
          ${driverOptions}
        </select>
        <label for="reason_${row.roster_id}">Motivo da alteração</label>
        <textarea id="reason_${row.roster_id}" name="reason" rows="2" required minlength="5" placeholder="Obrigatório para registar a alteração na escala"></textarea>
        <button type="submit">Alterar escalamento</button>
      </form>`
          : !noOtherActiveDrivers && !canReassign
            ? blockReason === "execucao_app"
              ? `<p class="field-help">Não é possível alterar o motorista: já existe neste dia, na aplicação, um serviço iniciado, em transferência ou concluído (com data de fim) para esta planificação e motorista.</p>`
              : blockReason === "estado_escala"
                ? `<p class="field-help">Não é possível alterar: o estado na escala já não permite reatribuição (viagem em curso ou concluída na escala).</p>`
                : ""
            : ""
      }
    `;
    container.appendChild(card);
  });
}

function selectRosterDayDriver(driverId) {
  rosterDaySelectedDriverId = driverId;
  document.querySelectorAll(".roster-day-driver-btn").forEach((btn) => {
    btn.classList.toggle("active", Number(btn.dataset.driverId) === Number(driverId));
  });
  const hintEl = document.getElementById("rosterDayPickDriverHint");
  if (hintEl) {
    hintEl.textContent = "Serviços escalados apenas para o motorista selecionado.";
  }
  const rows = rosterDayCache.filter((r) => Number(r.driver_id) === Number(driverId));
  renderRosterDayServiceRows(rows);
}

/**
 * @param {{ selectDriverId?: number | null }} [options] — após reatribuir, seleccionar o motorista de destino para mostrar a linha movida.
 */
async function loadRosterToday(options = {}) {
  if (!supToken) return;
  const selectDriverIdOpt =
    options.selectDriverId != null && options.selectDriverId !== ""
      ? Number(options.selectDriverId)
      : null;
  const dateInput = document.getElementById("rosterDayDate");
  const container = document.getElementById("rosterDayCards");
  const driverListEl = document.getElementById("rosterDayDriverList");
  const hintEl = document.getElementById("rosterDayPickDriverHint");
  if (!dateInput || !container || !driverListEl) return;
  if (!dateInput.value) {
    dateInput.value = todayISOInLisbon();
  }
  if (!driversCache.length) {
    await loadDrivers();
  }

  const prevDriverId = rosterDaySelectedDriverId;
  driverListEl.innerHTML = "<div>A carregar motoristas...</div>";
  container.innerHTML = "<div>A carregar serviços...</div>";
  if (hintEl) {
    hintEl.textContent = "A carregar…";
  }

  const response = await fetch(
    `${API_BASE}/supervisor/roster/today?date=${encodeURIComponent(dateInput.value)}`,
    { headers: getAuthHeaders() }
  );
  const data = await response.json();
  if (!response.ok) {
    driverListEl.innerHTML = "";
    container.innerHTML = `<div>${data.message || "Erro ao carregar escala."}</div>`;
    if (hintEl) hintEl.textContent = "Erro ao carregar a escala.";
    rosterDayCache = [];
    rosterDaySelectedDriverId = null;
    return;
  }

  rosterDayCache = data;
  driverListEl.innerHTML = "";
  container.innerHTML = "";

  if (!data.length) {
    driverListEl.innerHTML = "<div>Sem motoristas com escala nesta data.</div>";
    if (hintEl) {
      hintEl.textContent = "Não há linhas de escala para esta data.";
    }
    rosterDaySelectedDriverId = null;
    return;
  }

  const byDriver = new Map();
  data.forEach((row) => {
    const id = Number(row.driver_id);
    if (!byDriver.has(id)) {
      byDriver.set(id, {
        driver_id: id,
        driver_name: row.driver_name,
        mechanic: row.driver_mechanic_number,
        count: 0,
      });
    }
    byDriver.get(id).count += 1;
  });

  const sorted = [...byDriver.values()].sort((a, b) =>
    String(a.driver_name || "").localeCompare(String(b.driver_name || ""), "pt")
  );

  sorted.forEach((d) => {
    const btn = document.createElement("button");
    btn.type = "button";
    btn.className = "roster-day-driver-btn";
    btn.dataset.driverId = String(d.driver_id);
    const nameSpan = document.createElement("span");
    nameSpan.className = "roster-day-driver-name";
    nameSpan.textContent = d.driver_name || "(sem nome)";
    const metaSpan = document.createElement("span");
    metaSpan.className = "roster-day-driver-meta";
    metaSpan.textContent = `Mec. ${d.mechanic || "-"} · ${d.count} serviço(s)`;
    btn.appendChild(nameSpan);
    btn.appendChild(metaSpan);
    btn.addEventListener("click", () => selectRosterDayDriver(d.driver_id));
    driverListEl.appendChild(btn);
  });

  const preferNew =
    selectDriverIdOpt != null &&
    Number.isFinite(selectDriverIdOpt) &&
    sorted.some((d) => Number(d.driver_id) === selectDriverIdOpt);
  const keepOld =
    !preferNew &&
    prevDriverId != null &&
    sorted.some((d) => Number(d.driver_id) === Number(prevDriverId));

  if (preferNew) {
    selectRosterDayDriver(selectDriverIdOpt);
  } else if (keepOld) {
    selectRosterDayDriver(Number(prevDriverId));
  } else {
    rosterDaySelectedDriverId = null;
    if (hintEl) {
      hintEl.textContent = "Escolha um motorista acima para ver os serviços e alterar o escalamento.";
    }
    container.innerHTML = "";
  }
}

function bindById(id, eventName, handler) {
  const el = document.getElementById(id);
  if (!el) return;
  el.addEventListener(eventName, handler);
}

bindById("supLoginForm", "submit", loginSupervisor);
bindById("filtersForm", "submit", loadServices);
const clearFiltersBtn = document.getElementById("clearFiltersBtn");
if (clearFiltersBtn) {
  clearFiltersBtn.addEventListener("click", clearServiceFilters);
}
const onlyCancelledEl = document.getElementById("fOnlyCancelled");
if (onlyCancelledEl) {
  onlyCancelledEl.addEventListener("change", () => {
    const statusInput = document.getElementById("fStatus");
    if (onlyCancelledEl.checked && statusInput) {
      statusInput.value = "";
    }
    loadServices();
  });
}
bindById("exportBtn", "click", exportCsv);
bindById("exportExcelBtn", "click", exportExcelServices);
bindById("driverCreateForm", "submit", createDriver);
bindById("accessUserCreateForm", "submit", createAccessUser);
bindById("driverPasswordResetForm", "submit", resetDriverPassword);
bindById("accessPasswordResetForm", "submit", resetAccessPassword);
bindById("importDriversBtn", "click", importDrivers);
bindById("downloadDriversTemplateCsvBtn", "click", downloadDriversTemplateCsv);
bindById("downloadDriversTemplateXlsxBtn", "click", downloadDriversTemplateXlsx);
bindById("exportDriversCsvBtn", "click", exportDriversCsv);
bindById("exportDriversXlsxBtn", "click", exportDriversXlsx);
bindById("refreshDriversBtn", "click", loadDrivers);
if (trackerDeviceFormEl) {
  trackerDeviceFormEl.addEventListener("submit", saveTrackerDevice);
}
if (vehicleRegistryFormEl) {
  vehicleRegistryFormEl.addEventListener("submit", saveVehicleRegistry);
}
const refreshVehicleRegistryBtn = document.getElementById("refreshVehicleRegistryBtn");
if (refreshVehicleRegistryBtn) {
  refreshVehicleRegistryBtn.addEventListener("click", loadVehicleRegistry);
}
if (vehicleRegistryListEl && !vehicleRegistryListEl.dataset.vehicleKmDelegation) {
  vehicleRegistryListEl.dataset.vehicleKmDelegation = "1";
  vehicleRegistryListEl.addEventListener("click", (event) => {
    const btn = event.target.closest(".vehicle-km-save-btn");
    if (!btn) return;
    const imei = String(btn.getAttribute("data-vehicle-imei") || "");
    if (!imei) return;
    const input = document.getElementById(`vehicle-current-${imei}`);
    const value = Number(input?.value);
    if (!Number.isFinite(value) || value < 0) {
      alert("Introduza um valor de km válido.");
      return;
    }
    updateVehicleCurrentKm(imei, value);
  });
}
const copyTrackerWebhookConfigBtn = document.getElementById("copyTrackerWebhookConfigBtn");
if (copyTrackerWebhookConfigBtn) {
  copyTrackerWebhookConfigBtn.addEventListener("click", copyTrackerWebhookConfig);
}
const refreshTrackerDevicesBtn = document.getElementById("refreshTrackerDevicesBtn");
if (refreshTrackerDevicesBtn) {
  refreshTrackerDevicesBtn.addEventListener("click", loadTrackerDevices);
}
const refreshDeadheadMovementsBtn = document.getElementById("refreshDeadheadMovementsBtn");
if (refreshDeadheadMovementsBtn) {
  refreshDeadheadMovementsBtn.addEventListener("click", loadDeadheadMovements);
}
const exportDeadheadCsvBtn = document.getElementById("exportDeadheadCsvBtn");
if (exportDeadheadCsvBtn) {
  exportDeadheadCsvBtn.addEventListener("click", exportDeadheadCsv);
}
const exportDeadheadXlsxBtn = document.getElementById("exportDeadheadXlsxBtn");
if (exportDeadheadXlsxBtn) {
  exportDeadheadXlsxBtn.addEventListener("click", exportDeadheadXlsx);
}
if (deadheadFromDateEl && !deadheadFromDateEl.value) {
  deadheadFromDateEl.value = todayISOInLisbon();
}
if (deadheadToDateEl && !deadheadToDateEl.value) {
  deadheadToDateEl.value = todayISOInLisbon();
}
if (gtfsFeedKeyEl && !gtfsFeedKeyEl.value) {
  gtfsFeedKeyEl.value = "default";
}
if (deadheadFromDateEl) deadheadFromDateEl.addEventListener("change", loadDeadheadMovements);
if (deadheadToDateEl) deadheadToDateEl.addEventListener("change", loadDeadheadMovements);
if (deadheadFleetFilterEl) {
  deadheadFleetFilterEl.addEventListener("keydown", (event) => {
    if (event.key === "Enter") {
      event.preventDefault();
      loadDeadheadMovements();
    }
  });
}
const loadOdometerReportBtn = document.getElementById("loadOdometerReportBtn");
if (loadOdometerReportBtn) {
  loadOdometerReportBtn.addEventListener("click", loadOdometerReconciliationReport);
}
const odometerReportDateEl = document.getElementById("odometerReportDate");
if (odometerReportDateEl && !odometerReportDateEl.value) {
  odometerReportDateEl.value = todayISOInLisbon();
}
if (deadheadMovementsListEl && !deadheadMovementsListEl.dataset.deadheadDelegation) {
  deadheadMovementsListEl.dataset.deadheadDelegation = "1";
  deadheadMovementsListEl.addEventListener("click", (event) => {
    const btn = event.target.closest("[data-deadhead-id]");
    if (!btn) return;
    const id = Number(btn.getAttribute("data-deadhead-id"));
    if (!Number.isFinite(id)) return;
    loadDeadheadMovementDetail(id);
  });
}
const previewGtfsRtVehiclesBtn = document.getElementById("previewGtfsRtVehiclesBtn");
if (previewGtfsRtVehiclesBtn) {
  previewGtfsRtVehiclesBtn.addEventListener("click", () => loadGtfsRtPreview("vehicles"));
}
const previewGtfsRtTripUpdatesBtn = document.getElementById("previewGtfsRtTripUpdatesBtn");
if (previewGtfsRtTripUpdatesBtn) {
  previewGtfsRtTripUpdatesBtn.addEventListener("click", () => loadGtfsRtPreview("tripUpdates"));
}
const previewGtfsRtAlertsBtn = document.getElementById("previewGtfsRtAlertsBtn");
if (previewGtfsRtAlertsBtn) {
  previewGtfsRtAlertsBtn.addEventListener("click", () => loadGtfsRtPreview("alerts"));
}
const loadStopPassagesBtn = document.getElementById("loadStopPassagesBtn");
if (loadStopPassagesBtn) {
  loadStopPassagesBtn.addEventListener("click", loadStopPassages);
}
bindById("importRosterPreviewBtn", "click", () => importRosterFile(true));
bindById("importRosterBtn", "click", () => importRosterFile(false));
bindById("importGtfsBtn", "click", importGtfsZip);
bindById("refreshGtfsFeedsBtn", "click", loadGtfsFeeds);
bindById("refreshGtfsCalendarsBtn", "click", loadGtfsCalendars);
bindById("saveGtfsEffectiveDatesBtn", "click", saveGtfsEffectiveDates);
bindById("exportGtfsModifiedBtn", "click", exportGtfsModified);
bindById("loadGtfsAnalyticsBtn", "click", loadGtfsAnalyticsOverview);
bindById("loadGtfsLineDetailBtn", "click", () => loadGtfsLineDetail());
bindById("exportGtfsAnalyticsExcelBtn", "click", exportGtfsAnalyticsExcel);
const refreshGtfsEditorLinesBtn = document.getElementById("refreshGtfsEditorLinesBtn");
if (refreshGtfsEditorLinesBtn) {
  refreshGtfsEditorLinesBtn.addEventListener("click", loadGtfsEditorLines);
}
if (gtfsEditorFeedSelectEl) {
  gtfsEditorFeedSelectEl.addEventListener("change", () => {
    selectedGtfsFeedKey = String(gtfsEditorFeedSelectEl.value || "").trim();
    loadGtfsFeeds();
    loadGtfsEditorLines();
    loadGtfsCalendars();
  });
}
if (gtfsAnalyticsFeedSelectEl) {
  gtfsAnalyticsFeedSelectEl.addEventListener("change", () => {
    selectedGtfsAnalyticsFeedKey = String(gtfsAnalyticsFeedSelectEl.value || "").trim();
    loadGtfsAnalyticsOverview();
  });
}
if (gtfsAnalyticsLineSelectEl) {
  gtfsAnalyticsLineSelectEl.addEventListener("change", () => loadGtfsLineDetail());
}
if (gtfsAnalyticsTripSelectEl) {
  gtfsAnalyticsTripSelectEl.addEventListener("change", () => {
    const tripId = String(gtfsAnalyticsTripSelectEl.value || "").trim();
    const trip = gtfsLineTripsCache.find((item) => String(item.trip_id) === tripId);
    if (!trip) return;
    drawGtfsAnalyticsTrip(trip);
  });
}
if (gtfsEditorRouteSelectEl) {
  gtfsEditorRouteSelectEl.addEventListener("change", loadGtfsEditorTripsByRoute);
}
const loadGtfsEditorTripStopsBtn = document.getElementById("loadGtfsEditorTripStopsBtn");
if (loadGtfsEditorTripStopsBtn) {
  loadGtfsEditorTripStopsBtn.addEventListener("click", loadGtfsEditorTripStops);
}
const gtfsEditorAddStopForm = document.getElementById("gtfsEditorAddStopForm");
if (gtfsEditorAddStopForm) {
  gtfsEditorAddStopForm.addEventListener("submit", addGtfsEditorStop);
}
if (gtfsEditorStopsListEl && !gtfsEditorStopsListEl.dataset.gtfsEditorDelegation) {
  gtfsEditorStopsListEl.dataset.gtfsEditorDelegation = "1";
  gtfsEditorStopsListEl.addEventListener("click", (e) => {
    const btn = e.target.closest("[data-gtfs-remove-seq]");
    if (!btn) return;
    const seq = Number(btn.getAttribute("data-gtfs-remove-seq"));
    if (!Number.isFinite(seq)) return;
    removeGtfsEditorStop(seq);
  });
}
if (gtfsFeedsListEl && !gtfsFeedsListEl.dataset.gtfsFeedsDelegation) {
  gtfsFeedsListEl.dataset.gtfsFeedsDelegation = "1";
  gtfsFeedsListEl.addEventListener("click", (event) => {
    const btn = event.target.closest("button[data-gtfs-feed-toggle]");
    if (!btn) return;
    const feedKey = String(btn.getAttribute("data-gtfs-feed-toggle") || "");
    const active = btn.getAttribute("data-gtfs-feed-active") === "1";
    toggleGtfsFeed(feedKey, active);
  });
}
if (gtfsAnalyticsTableBodyEl && !gtfsAnalyticsTableBodyEl.dataset.gtfsAnalyticsDelegation) {
  gtfsAnalyticsTableBodyEl.dataset.gtfsAnalyticsDelegation = "1";
  gtfsAnalyticsTableBodyEl.addEventListener("click", (event) => {
    const btn = event.target.closest("button[data-gtfs-analytics-route]");
    if (!btn) return;
    const routeId = String(btn.getAttribute("data-gtfs-analytics-route") || "").trim();
    loadGtfsLineDetail(routeId);
  });
}
if (gtfsCalendarsListEl && !gtfsCalendarsListEl.dataset.gtfsCalendarsDelegation) {
  gtfsCalendarsListEl.dataset.gtfsCalendarsDelegation = "1";
  gtfsCalendarsListEl.addEventListener("click", (event) => {
    const toggleBtn = event.target.closest("button[data-cal-toggle]");
    if (toggleBtn) {
      const serviceId = String(toggleBtn.getAttribute("data-cal-toggle") || "");
      const active = toggleBtn.getAttribute("data-cal-active") === "1";
      updateGtfsCalendar(serviceId, { isActive: !active });
      return;
    }
    const saveBtn = event.target.closest("button[data-cal-save]");
    if (!saveBtn) return;
    const serviceId = String(saveBtn.getAttribute("data-cal-save") || "");
    const item = saveBtn.closest(".service-card-item");
    const startInput = item?.querySelector("input[data-cal-start]");
    const endInput = item?.querySelector("input[data-cal-end]");
    updateGtfsCalendar(serviceId, {
      startDate: startInput?.value || null,
      endDate: endInput?.value || null,
    });
  });
}
bindById("closeServiceDrawerBtn", "click", closeServiceDrawer);
bindById("serviceDrawerBackdrop", "click", closeServiceDrawer);
bindById("serviceAdjustForm", "submit", saveServiceAdjust);
bindById("forceEndServiceBtn", "click", forceEndService);
bindById("transferServiceBtn", "click", transferServiceBySupervisor);
bindById("cancelServiceBtn", "click", cancelServiceBySupervisor);
bindById("logoutBtn", "click", logoutSupervisor);
const openRosterFromServicesBtn = document.getElementById("openRosterFromServicesBtn");
if (openRosterFromServicesBtn) {
  openRosterFromServicesBtn.addEventListener("click", () => openSupervisorTab("tabEscalaDia"));
}
const rosterDayRefreshBtn = document.getElementById("rosterDayRefreshBtn");
if (rosterDayRefreshBtn) {
  rosterDayRefreshBtn.addEventListener("click", () => loadRosterToday());
}
const rosterDayDateInput = document.getElementById("rosterDayDate");
if (rosterDayDateInput) {
  rosterDayDateInput.addEventListener("change", () => {
    rosterDaySelectedDriverId = null;
    loadRosterToday();
  });
}
const refreshConflictAlertsBtn = document.getElementById("refreshConflictAlertsBtn");
if (refreshConflictAlertsBtn) {
  refreshConflictAlertsBtn.addEventListener("click", loadConflictAlerts);
}
const refreshLiveServicesBtn = document.getElementById("refreshLiveServicesBtn");
if (refreshLiveServicesBtn) {
  refreshLiveServicesBtn.addEventListener("click", loadLiveServicesMap);
}
if (liveServiceFilterSelectEl) {
  liveServiceFilterSelectEl.addEventListener("change", () => {
    if (!currentLiveServices.length) {
      loadLiveServicesMap();
      return;
    }
    renderLiveRealtimeView();
  });
}
document.querySelectorAll(".live-status-chip").forEach((btn) => {
  btn.addEventListener("click", () => {
    liveRealtimeStatusFilter = btn.getAttribute("data-live-status") || "all";
    document.querySelectorAll(".live-status-chip").forEach((chip) => {
      chip.classList.toggle("active", chip === btn);
    });
    if (!currentLiveServices.length) {
      loadLiveServicesMap();
      return;
    }
    renderLiveRealtimeView();
  });
});
const rosterDayCardsEl = document.getElementById("rosterDayCards");
if (rosterDayCardsEl) {
  rosterDayCardsEl.addEventListener("submit", async (event) => {
    const form = event.target.closest(".roster-reassign-form");
    if (!form) return;
    event.preventDefault();
    if (!supToken) {
      alert("Sessão expirada ou sem token. Inicie sessão novamente.");
      return;
    }
    const rosterId = form.getAttribute("data-roster-id");
    const fd = new FormData(form);
    const newDriverId = fd.get("newDriverId");
    const reason = String(fd.get("reason") || "").trim();
    if (!newDriverId) {
      alert("Selecione o motorista de destino.");
      return;
    }
    if (reason.length < 5) {
      alert("Indique um motivo com pelo menos 5 caracteres.");
      return;
    }
    let data;
    try {
      const response = await fetch(`${API_BASE}/supervisor/roster/${rosterId}/reassign`, {
        method: "PATCH",
        headers: getAuthHeaders(),
        body: JSON.stringify({ newDriverId: Number(newDriverId), reason }),
      });
      data = await response.json().catch(() => ({}));
      const apiRev = data.apiRevision || "";
      const revSuffix = apiRev ? ` [API ${apiRev}]` : "";
      if (!response.ok) {
        alert((data.message || "Erro ao alterar escalamento.") + revSuffix);
        return;
      }
    } catch (_err) {
      alert("Não foi possível contactar o servidor. Verifique a API e a rede.");
      return;
    }
    const apiRevOk = data.apiRevision || "";
    const revOk = apiRevOk ? ` [API ${apiRevOk}]` : "";
    alert((data.message || "Escalamento atualizado.") + revOk);
    await loadRosterToday({ selectDriverId: data.new_driver_id });
  });
}
initTabs();
initServiceFilterMode();
renderTrackerWebhookExamples();

(() => {
  const fromEl = document.getElementById("reportFromDate");
  const toEl = document.getElementById("reportToDate");
  if (fromEl && !fromEl.value) fromEl.value = todayISOInLisbon();
  if (toEl && !toEl.value) toEl.value = todayISOInLisbon();
})();

const reportsFormEl = document.getElementById("reportsForm");
if (reportsFormEl) {
  reportsFormEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    await loadOperationalReport();
  });
}
const exportReportExcelBtn = document.getElementById("exportReportExcelBtn");
if (exportReportExcelBtn) {
  exportReportExcelBtn.addEventListener("click", () => {
    if (!supToken) {
      alert("Sessão inválida.");
      return;
    }
    const query = buildOperationalReportQuery();
    void downloadOperationalReportExcel(query);
  });
}
const exportReportExcelTodayBtn = document.getElementById("exportReportExcelTodayBtn");
if (exportReportExcelTodayBtn) {
  exportReportExcelTodayBtn.addEventListener("click", () => {
    if (!supToken) {
      alert("Sessão inválida.");
      return;
    }
    const fromDateEl = document.getElementById("reportFromDate");
    const toDateEl = document.getElementById("reportToDate");
    const today = todayISOInLisbon();
    if (fromDateEl) fromDateEl.value = today;
    if (toDateEl) toDateEl.value = today;
    const query = buildOperationalReportQuery();
    void downloadOperationalReportExcel(query);
  });
}
if (supMessageFormEl) {
  supMessageFormEl.addEventListener("submit", sendSupervisorMessage);
}
if (supPresetFormEl) {
  supPresetFormEl.addEventListener("submit", async (event) => {
    event.preventDefault();
    if (!supToken) return;
    const scope = String(supPresetScopeEl?.value || "supervisor");
    const code = String(supPresetCodeEl?.value || "").trim() || null;
    const label = String(supPresetLabelEl?.value || "").trim();
    const defaultText = String(supPresetDefaultTextEl?.value || "").trim() || label;
    if (!label) {
      alert("Indique o rótulo da predefinida.");
      return;
    }
    try {
      const response = await fetch(`${API_BASE}/supervisor/message-presets`, {
        method: "POST",
        headers: getAuthHeaders(),
        body: JSON.stringify({ scope, code, label, defaultText, isActive: supPresetIsActiveEl?.checked !== false }),
      });
      const data = await response.json().catch(() => ({}));
      if (!response.ok) {
        alert(data.message || "Erro ao guardar predefinida.");
        return;
      }
      supPresetFormEl.reset();
      if (supPresetIsActiveEl) supPresetIsActiveEl.checked = true;
      await loadSupPresetList();
      if (scope === "supervisor") {
        await loadOpsMessagePresets();
      }
      alert("Predefinida guardada.");
    } catch (_error) {
      alert("Erro de ligação ao guardar predefinida.");
    }
  });
}
if (supPresetListScopeEl) {
  supPresetListScopeEl.addEventListener("change", loadSupPresetList);
}
if (refreshSupPresetListBtnEl) {
  refreshSupPresetListBtnEl.addEventListener("click", loadSupPresetList);
}
if (refreshOpsThreadsBtnEl) {
  refreshOpsThreadsBtnEl.addEventListener("click", loadOpsThreads);
}
if (refreshOpsMessagesBtnEl) {
  refreshOpsMessagesBtnEl.addEventListener("click", () => {
    if (!selectedOpsDriverId) {
      alert("Selecione um motorista na lista.");
      return;
    }
    loadOpsMessages(selectedOpsDriverId);
  });
}
if (supAlertSoundTypeEl) {
  supAlertSoundTypeEl.addEventListener("change", () => saveSupAlertSoundSettings(getSupAlertSoundSettings()));
}
if (supAlertSoundVolumeEl) {
  supAlertSoundVolumeEl.addEventListener("input", () => saveSupAlertSoundSettings(getSupAlertSoundSettings()));
}
if (testSupAlertSoundBtnEl) {
  testSupAlertSoundBtnEl.addEventListener("click", playSupervisorAlertSound);
}
applySupAlertSoundSettingsToUi();

if (serviceCardsEl && !serviceCardsEl.dataset.stopPassagesDelegation) {
  serviceCardsEl.dataset.stopPassagesDelegation = "1";
  serviceCardsEl.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-stop-passages-service-id]");
    if (!btn) return;
    e.preventDefault();
    openStopPassagesTabForService(btn.getAttribute("data-stop-passages-service-id"));
  });
}
if (supLiveServicesListEl && !supLiveServicesListEl.dataset.stopPassagesDelegation) {
  supLiveServicesListEl.dataset.stopPassagesDelegation = "1";
  supLiveServicesListEl.addEventListener("click", (e) => {
    const btn = e.target.closest("button[data-stop-passages-service-id]");
    if (!btn) return;
    e.preventDefault();
    openStopPassagesTabForService(btn.getAttribute("data-stop-passages-service-id"));
  });
  supLiveServicesListEl.addEventListener("mouseover", (e) => {
    const target = e.target.closest("[data-live-driver-service-id]");
    if (!target) return;
    applyLiveServiceRouteHighlight(target.getAttribute("data-live-driver-service-id"));
  });
  supLiveServicesListEl.addEventListener("mouseout", (e) => {
    const target = e.target.closest("[data-live-driver-service-id]");
    if (!target) return;
    applyLiveServiceRouteHighlight(null);
  });
}

(() => {
  const raw = sessionStorage.getItem(AUTH_SESSION_KEY);
  if (!raw) return;
  try {
    const session = JSON.parse(raw);
    if (!session?.token || !session?.user) return;
    const role = String(session.user.role || "").trim().toLowerCase() || readRoleFromJwt(session.token);
    if (role === "viewer" || role === "viewr") {
      window.location.href = `${window.location.origin}/frontend/viewer.html`;
      return;
    }
    if (role === "driver") {
      window.location.href = `${window.location.origin}/frontend/index.html`;
      return;
    }
    if (!["supervisor", "admin"].includes(role)) return;
    supToken = session.token;
    applySessionAndLoad(session.user);
    loadOverview();
    loadServices();
    loadLiveServicesMap();
    loadDrivers();
    loadConflictAlerts();
    startLiveMapAutoRefresh();
    supervisorAlertBaselineReady = false;
    startSupervisorAlertsPolling();
  } catch (_error) {
    // ignore invalid persisted session
  }
})();
