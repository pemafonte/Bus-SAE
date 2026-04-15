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
const serviceRouteTimesEl = document.getElementById("serviceRouteTimes");
const conflictAlertsListEl = document.getElementById("conflictAlertsList");
const supLiveMapEl = document.getElementById("supLiveMap");
const supLiveServicesListEl = document.getElementById("supLiveServicesList");
const liveServiceFilterSelectEl = document.getElementById("liveServiceFilterSelect");

let driversCache = [];
let usersCache = [];
let rosterDayCache = [];
let rosterDaySelectedDriverId = null;
let supLiveMap = null;
let supLiveMarkersLayer = null;
let supLiveRefreshInterval = null;
let currentLiveServices = [];

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

function applySessionAndLoad(user) {
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
  const fromDate = document.getElementById("fFromDate").value;
  const toDate = document.getElementById("fToDate").value;

  if (driverId) params.set("driverId", driverId);
  if (lineCode) params.set("lineCode", lineCode);
  if (status) params.set("status", status);
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

function closeServiceDrawer() {
  serviceDrawerEl.classList.add("hidden");
  serviceDrawerBackdropEl.classList.add("hidden");
  selectedService = null;
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
  serviceDrawerBackdropEl.classList.remove("hidden");
  serviceDrawerEl.classList.remove("hidden");
  loadServiceDetails(service.id);
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
    `Horario: ${data.service.service_schedule || "-"}`,
    `Início: ${data.service.started_at ? new Date(data.service.started_at).toLocaleString() : "-"}`,
    `Chegada: ${data.service.ended_at ? new Date(data.service.ended_at).toLocaleString() : "-"}`,
    `Quilómetros: ${data.service.total_km || 0}`,
  ].join("\n");
  document.getElementById("adjustFleetNumber").value = data.service.fleet_number || "";
  renderMiniRouteMap(data.points || []);
  renderHandovers(data.handovers || []);
  if (data.service.status === "in_progress") forceEndServiceBtn.classList.remove("hidden");
  else forceEndServiceBtn.classList.add("hidden");
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
  document.getElementById("supLoginForm").reset();
  sessionStorage.removeItem(AUTH_SESSION_KEY);
}

function openSupervisorTab(tabId) {
  if (!tabId) return;
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
  if (tabId === "tabServicos") {
    loadLiveServicesMap();
    if (supLiveMap) setTimeout(() => supLiveMap.invalidateSize(), 80);
  }
  if (tabId === "tabAlertasConflito") {
    loadConflictAlerts();
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
  const selectedServiceId = String(liveServiceFilterSelectEl?.value || "all");
  const list =
    selectedServiceId === "all"
      ? currentLiveServices
      : currentLiveServices.filter((svc) => String(svc.id) === selectedServiceId);

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

  supLiveServicesListEl.innerHTML = "";
  if (!list.length) {
    supLiveServicesListEl.innerHTML = "<div>Sem serviços em execução neste momento.</div>";
  }

  supLiveMarkersLayer.clearLayers();
  const bounds = [];
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
    card.innerHTML = `<div><strong>Serviço #${svc.id}</strong> | Linha ${svc.line_code || "-"} | Frota ${svc.fleet_number || "-"} | ${svc.driver_name || "-"}</div>`;
    supLiveServicesListEl.appendChild(card);
  });

  if (bounds.length) {
    supLiveMap.fitBounds(bounds, { padding: [20, 20], maxZoom: 15 });
  }
}

function startLiveMapAutoRefresh() {
  if (supLiveRefreshInterval) clearInterval(supLiveRefreshInterval);
  supLiveRefreshInterval = setInterval(() => {
    if (!supToken) return;
    loadLiveServicesMap();
  }, 15000);
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
    const startLoc = item.start_location || "-";
    const endLoc = item.end_location || "-";
    article.innerHTML = `
      <div class="service-card-head">
        <strong>Alerta #${item.id}</strong>
        <span class="status-badge status-other">Conflito</span>
      </div>
      <div class="service-card-grid">
        <div><small>Data/Hora alerta</small><div>${item.created_at ? new Date(item.created_at).toLocaleString() : "-"}</div></div>
        <div><small>Motorista</small><div>${item.driver_name || "-"} (Mec. ${item.driver_mechanic_number || "-"})</div></div>
        <div><small>Serviço</small><div>${item.service_code || "-"} (ID plan. ${item.planned_service_id || "-"})</div></div>
        <div><small>Horário</small><div>${item.service_schedule || "-"}</div></div>
        <div><small>Linha</small><div>${item.line_code || "-"}</div></div>
        <div><small>Origem / destino</small><div>${startLoc} → ${endLoc}</div></div>
        <div><small>IDs em conflito</small><div>${conflictIds.length ? conflictIds.join(", ") : "-"}</div></div>
        <div><small>Notas</small><div>${item.notes || "-"}</div></div>
      </div>
    `;
    conflictAlertsListEl.appendChild(article);
  });
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
    `Quilómetros realizados (concluídos): ${n(data.total_km)}`,
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
      </div>
      <div class="service-card-actions">
        <button type="button" class="adjust-btn" data-service-id="${s.id}">Abrir detalhe</button>
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
  };

  servicesPieEl.style.background = buildPieBackground([
    { value: counts.completed, color: "#16a34a" },
    { value: counts.inProgress, color: "#ea580c" },
    { value: counts.waiting, color: "#7c3aed" },
  ]);
  servicesStatsTextEl.textContent =
    `Concluídos: ${counts.completed} | Em curso: ${counts.inProgress} | Aguardam transferência: ${counts.waiting}`;

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

  const response = await fetch(`${API_BASE}/gtfs/import`, {
    method: "POST",
    headers: getAuthHeaders(),
    body: JSON.stringify({ fileBase64: btoa(binary) }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao importar GTFS.");
    importGtfsReportEl.textContent = data.message || "Erro na importacao GTFS.";
    return;
  }

  importGtfsReportEl.textContent = [
    data.message || "GTFS importado.",
    `routes: ${data.counts?.routes || 0}`,
    `trips: ${data.counts?.trips || 0}`,
    `shapes: ${data.counts?.shapes || 0}`,
    `stops: ${data.counts?.stops || 0}`,
    `stop_times: ${data.counts?.stopTimes || 0}`,
  ].join("\n");
  alert("GTFS importado com sucesso.");
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

document.getElementById("supLoginForm").addEventListener("submit", loginSupervisor);
document.getElementById("filtersForm").addEventListener("submit", loadServices);
document.getElementById("exportBtn").addEventListener("click", exportCsv);
document.getElementById("exportExcelBtn").addEventListener("click", exportExcelServices);
document.getElementById("driverCreateForm").addEventListener("submit", createDriver);
document.getElementById("accessUserCreateForm").addEventListener("submit", createAccessUser);
document.getElementById("driverPasswordResetForm").addEventListener("submit", resetDriverPassword);
document.getElementById("accessPasswordResetForm").addEventListener("submit", resetAccessPassword);
document.getElementById("importDriversBtn").addEventListener("click", importDrivers);
document.getElementById("downloadDriversTemplateCsvBtn").addEventListener("click", downloadDriversTemplateCsv);
document.getElementById("downloadDriversTemplateXlsxBtn").addEventListener("click", downloadDriversTemplateXlsx);
document.getElementById("exportDriversCsvBtn").addEventListener("click", exportDriversCsv);
document.getElementById("exportDriversXlsxBtn").addEventListener("click", exportDriversXlsx);
document.getElementById("refreshDriversBtn").addEventListener("click", loadDrivers);
document.getElementById("importRosterPreviewBtn").addEventListener("click", () => importRosterFile(true));
document.getElementById("importRosterBtn").addEventListener("click", () => importRosterFile(false));
document.getElementById("importGtfsBtn").addEventListener("click", importGtfsZip);
document.getElementById("closeServiceDrawerBtn").addEventListener("click", closeServiceDrawer);
document.getElementById("serviceDrawerBackdrop").addEventListener("click", closeServiceDrawer);
document.getElementById("serviceAdjustForm").addEventListener("submit", saveServiceAdjust);
document.getElementById("forceEndServiceBtn").addEventListener("click", forceEndService);
document.getElementById("logoutBtn").addEventListener("click", logoutSupervisor);
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
    const selectedServiceId = String(liveServiceFilterSelectEl.value || "all");
    const filtered =
      selectedServiceId === "all"
        ? currentLiveServices
        : currentLiveServices.filter((svc) => String(svc.id) === selectedServiceId);

    supLiveMarkersLayer.clearLayers();
    supLiveServicesListEl.innerHTML = "";
    const bounds = [];
    filtered.forEach((svc) => {
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
      card.innerHTML = `<div><strong>Serviço #${svc.id}</strong> | Linha ${svc.line_code || "-"} | Frota ${svc.fleet_number || "-"} | ${svc.driver_name || "-"}</div>`;
      supLiveServicesListEl.appendChild(card);
    });
    if (!filtered.length) {
      supLiveServicesListEl.innerHTML = "<div>Sem serviços em execução neste momento.</div>";
    } else if (bounds.length) {
      supLiveMap.fitBounds(bounds, { padding: [20, 20], maxZoom: 15 });
    }
  });
}
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
  } catch (_error) {
    // ignore invalid persisted session
  }
})();
