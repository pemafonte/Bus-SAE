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

function parseServiceScheduleRangeMinutes(serviceSchedule) {
  const text = String(serviceSchedule || "").trim();
  if (!text) return null;
  const rangeMatch = text.match(/(\d{1,2})\s*:\s*(\d{2})\s*-\s*(\d{1,2})\s*:\s*(\d{2})/);
  if (rangeMatch) {
    let start = Number(rangeMatch[1]) * 60 + Number(rangeMatch[2]);
    let end = Number(rangeMatch[3]) * 60 + Number(rangeMatch[4]);
    if (Number.isNaN(start) || Number.isNaN(end)) return null;
    if (end <= start) end += 24 * 60;
    return { start, end };
  }
  const singleMatch = text.match(/(\d{1,2})\s*:\s*(\d{2})/);
  if (!singleMatch) return null;
  const start = Number(singleMatch[1]) * 60 + Number(singleMatch[2]);
  if (Number.isNaN(start)) return null;
  return { start, end: start + 4 * 60 };
}

function scheduleRangesOverlap(a, b) {
  if (!a || !b) return false;
  return a.start < b.end && b.start < a.end;
}

let token = "";
let activeServiceId = null;
let watchId = null;
let selectedPlannedServiceId = null;
let selectedPlannedFleetNumber = "";
let authenticatedUser = null;
let gpsPointQueue = [];
let gpsFlushInProgress = false;
let gpsSaveFailureWarned = false;
const AUTH_SESSION_KEY = "auth_session";
const conflictNotifiedPlannedServiceIds = new Set();

const loginScreenEl = document.getElementById("loginScreen");
const appScreenEl = document.getElementById("appScreen");
const sessionWelcomeEl = document.getElementById("sessionWelcome");
const logoutBtnEl = document.getElementById("logoutBtn");
const summaryEl = document.getElementById("serviceSummary");
const endTripBtn = document.getElementById("endTripBtn");
const cancelTripBtn = document.getElementById("cancelTripBtn");
const handoverBtn = document.getElementById("handoverBtn");
const historyList = document.getElementById("historyList");
const todayServicesList = document.getElementById("todayServicesList");
const pendingHandoversList = document.getElementById("pendingHandoversList");
const fleetNumberInput = document.getElementById("fleetNumber");
const fleetWarningEl = document.getElementById("fleetWarning");
const driverTabsEl = document.getElementById("driverTabs");
const driverNotificationsBarEl = document.getElementById("driverNotificationsBar");

const map = L.map("map").setView([38.7223, -9.1393], 12);

function attachBaseTileLayer(targetMap) {
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
    if (targetMap.hasLayer(primary)) targetMap.removeLayer(primary);
    if (!targetMap.hasLayer(fallback)) fallback.addTo(targetMap);
  });
  primary.addTo(targetMap);
}

attachBaseTileLayer(map);

const routePolyline = L.polyline([], { color: "#2563eb", weight: 5 }).addTo(map);
const referenceRoutePolyline = L.polyline([], { color: "#f97316", weight: 4, dashArray: "8 8" }).addTo(map);
const gtfsStopsLayer = L.layerGroup().addTo(map);
let activeBusMarker = null;
handoverBtn.disabled = true;
cancelTripBtn.disabled = true;

function clearActiveServiceState() {
  activeServiceId = null;
  gpsPointQueue = [];
  gpsSaveFailureWarned = false;
  endTripBtn.disabled = true;
  handoverBtn.disabled = true;
  cancelTripBtn.disabled = true;
  if (watchId) {
    navigator.geolocation.clearWatch(watchId);
    watchId = null;
  }
  if (activeBusMarker) {
    map.removeLayer(activeBusMarker);
    activeBusMarker = null;
  }
}

async function flushGpsPointQueue() {
  if (gpsFlushInProgress || !gpsPointQueue.length || !activeServiceId || !token) return;
  gpsFlushInProgress = true;
  try {
    while (gpsPointQueue.length && activeServiceId && token) {
      const nextPoint = gpsPointQueue[0];
      const pointResponse = await fetch(`${API_BASE}/services/${activeServiceId}/points`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify(nextPoint),
      });
      const pointData = await pointResponse.json().catch(() => ({}));
      if (!pointResponse.ok) {
        if (!gpsSaveFailureWarned) {
          gpsSaveFailureWarned = true;
          alert("A ligação GPS está instável. Vamos continuar a tentar guardar os pontos do percurso.");
        }
        break;
      }
      gpsPointQueue.shift();
      gpsSaveFailureWarned = false;
      if (pointData.routeCheck) {
        const offRouteText = pointData.routeCheck.isOffRoute ? "Fora da rota" : "Dentro da rota";
        const deviationText =
          pointData.routeCheck.deviationMeters == null ? "-" : `${pointData.routeCheck.deviationMeters} m`;
        const currentSummary = summaryEl.textContent.split("\n").filter((line) => !line.startsWith("Rota:"));
        currentSummary.push(`Rota: ${offRouteText} | Desvio: ${deviationText}`);
        setSummary(currentSummary.join("\n"));
      }
    }
  } finally {
    gpsFlushInProgress = false;
  }
}

function buildBusIcon(fleetNumber) {
  const fleet = String(fleetNumber || "-");
  return L.divIcon({
    className: "driver-bus-marker",
    html: `<div class="driver-bus-label">Frota ${fleet}</div><div class="driver-bus-icon">🚌</div>`,
    iconSize: [56, 44],
    iconAnchor: [28, 22],
  });
}

function updateActiveBusMarker(lat, lng, fleetNumber) {
  if (!Number.isFinite(lat) || !Number.isFinite(lng)) return;
  const icon = buildBusIcon(fleetNumber);
  if (!activeBusMarker) {
    activeBusMarker = L.marker([lat, lng], { icon }).addTo(map);
    return;
  }
  activeBusMarker.setLatLng([lat, lng]);
  activeBusMarker.setIcon(icon);
}

function showDriverTab(tabId) {
  const buttons = driverTabsEl?.querySelectorAll(".tab-btn") || [];
  const panels = document.querySelectorAll("#appScreen .tab-panel");
  buttons.forEach((btn) => {
    const target = btn.getAttribute("data-driver-tab-target");
    btn.classList.toggle("active", target === tabId);
  });
  panels.forEach((panel) => {
    panel.classList.toggle("active", panel.id === tabId);
  });
  if (tabId === "driverStepMap") {
    setTimeout(() => map.invalidateSize(), 80);
  }
  if (tabId === "driverStepSelect") {
    refreshDriverNotifications();
  }
}

function initDriverTabs() {
  const buttons = driverTabsEl?.querySelectorAll(".tab-btn") || [];
  buttons.forEach((btn) => {
    btn.addEventListener("click", () => {
      const target = btn.getAttribute("data-driver-tab-target");
      if (target) showDriverTab(target);
    });
  });
}

function authHeaders() {
  return {
    "Content-Type": "application/json",
    Authorization: `Bearer ${token}`,
  };
}

function renderDriverNotificationsBar(list) {
  const el = driverNotificationsBarEl;
  if (!el) return;
  el.innerHTML = "";
  const unread = (list || []).filter((n) => !n.read_at);
  if (!unread.length) {
    el.classList.add("hidden");
    return;
  }
  el.classList.remove("hidden");
  unread.forEach((n) => {
    const wrap = document.createElement("div");
    wrap.className = "driver-notification-item";
    const title = document.createElement("strong");
    title.textContent = n.title || "Aviso";
    const msg = document.createElement("p");
    msg.textContent = n.message || "";
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = "Marcar como lido";
    btn.addEventListener("click", async () => {
      try {
        await fetch(`${API_BASE}/services/notifications/${n.id}/read`, {
          method: "PATCH",
          headers: authHeaders(),
        });
      } catch (_e) {
        // ignore
      }
      await refreshDriverNotifications();
    });
    wrap.appendChild(title);
    wrap.appendChild(msg);
    wrap.appendChild(btn);
    el.appendChild(wrap);
  });
}

async function refreshDriverNotifications() {
  if (!token || !driverNotificationsBarEl) return;
  try {
    const response = await fetch(`${API_BASE}/services/notifications`, {
      headers: authHeaders(),
    });
    const list = await response.json();
    if (!response.ok) return;
    renderDriverNotificationsBar(list);
  } catch (_e) {
    // ignore offline errors
  }
}

function setSummary(text) {
  summaryEl.textContent = text;
}

function updateFleetWarning() {
  const currentValue = fleetNumberInput.value.trim();
  const hasPlanned = selectedPlannedServiceId && selectedPlannedFleetNumber;
  const isDifferent =
    hasPlanned &&
    currentValue &&
    currentValue.toLowerCase() !== selectedPlannedFleetNumber.toLowerCase();

  fleetNumberInput.classList.toggle("warning", Boolean(isDifferent));
  fleetWarningEl.classList.toggle("hidden", !isDifferent);
}

async function login(event) {
  event.preventDefault();
  const username = document.getElementById("loginUsername").value.trim();
  const password = document.getElementById("loginPassword").value;

  if (!username) {
    alert("Preencha o nome de utilizador.");
    return;
  }

  let response;
  let data;
  try {
    response = await fetch(`${API_BASE}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
    data = await response.json();
  } catch (_error) {
    alert("Não foi possível ligar ao servidor. Verifique se a API está ativa.");
    return;
  }

  if (!response.ok) {
    alert(data?.message || "Erro no início de sessão.");
    return;
  }

  token = data.token;
  authenticatedUser = data.user;
  sessionStorage.setItem(AUTH_SESSION_KEY, JSON.stringify({ token: data.token, user: data.user }));
  alert(`Bem-vindo ${data.user.name}`);

  const role = String(data.user.role || "").trim().toLowerCase();
  if (role === "supervisor" || role === "admin") {
    window.location.href = `${window.location.origin}/frontend/supervisor.html`;
    return;
  }
  if (role === "viewer" || role === "viewr") {
    window.location.href = `${window.location.origin}/frontend/viewer.html`;
    return;
  }

  sessionWelcomeEl.textContent = `Bem-vindo ${data.user.username || username}`;
  sessionWelcomeEl.classList.remove("hidden");
  logoutBtnEl.classList.remove("hidden");
  loginScreenEl.classList.add("hidden");
  appScreenEl.classList.remove("hidden");
  showDriverTab("driverStepSelect");
  await restoreActiveService();
  await loadTodayServices();
  await loadPendingHandovers();
  await refreshHistory();
  await refreshDriverNotifications();
  showDriverTab("driverStepSelect");
}

function logoutDriver() {
  const confirmed = window.confirm("Tem a certeza de que deseja terminar a sessão?");
  if (!confirmed) return;

  token = "";
  authenticatedUser = null;
  gpsPointQueue = [];
  gpsSaveFailureWarned = false;
  sessionStorage.removeItem(AUTH_SESSION_KEY);
  activeServiceId = null;
  selectedPlannedServiceId = null;
  selectedPlannedFleetNumber = "";
  if (watchId) {
    navigator.geolocation.clearWatch(watchId);
    watchId = null;
  }
  routePolyline.setLatLngs([]);
  referenceRoutePolyline.setLatLngs([]);
  gtfsStopsLayer.clearLayers();
  if (activeBusMarker) {
    map.removeLayer(activeBusMarker);
    activeBusMarker = null;
  }
  endTripBtn.disabled = true;
  handoverBtn.disabled = true;
  cancelTripBtn.disabled = true;
  sessionWelcomeEl.classList.add("hidden");
  logoutBtnEl.classList.add("hidden");
  appScreenEl.classList.add("hidden");
  loginScreenEl.classList.remove("hidden");
  document.getElementById("loginForm").reset();
  setSummary("Sem viagem em curso.");
  todayServicesList.innerHTML = "";
  pendingHandoversList.innerHTML = "";
  historyList.innerHTML = "";
  if (driverNotificationsBarEl) {
    driverNotificationsBarEl.innerHTML = "";
    driverNotificationsBarEl.classList.add("hidden");
  }
}

async function restoreActiveService() {
  if (!token) return;
  const response = await fetch(`${API_BASE}/services/active`, {
    headers: authHeaders(),
  });
  const data = await response.json().catch(() => ({}));
  if (!response.ok || !data.activeService) return;

  const active = data.activeService;
  activeServiceId = active.id;
  endTripBtn.disabled = false;
  handoverBtn.disabled = false;
  cancelTripBtn.disabled = false;
  document.getElementById("plateNumber").value = active.plate_number || "";
  document.getElementById("serviceSchedule").value = active.service_schedule || "";
  document.getElementById("lineCode").value = active.line_code || "";
  document.getElementById("fleetNumber").value = active.fleet_number || "";

  setSummary(
    [
      `Serviço ativo: ${active.id}`,
      `Chapa: ${active.plate_number}`,
      `Horário: ${active.service_schedule}`,
      `Linha: ${active.line_code}`,
      `Frota: ${active.fleet_number}`,
      `Estado: ${labelEstadoExecucaoServicoPt(active.status)}`,
    ].join("\n")
  );
  if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(
      (position) => {
        updateActiveBusMarker(position.coords.latitude, position.coords.longitude, active.fleet_number);
      },
      () => {}
    );
  }
  await loadReferenceRoute(active.id);
  startLocationTracking();
  showDriverTab("driverStepMap");
}

async function startTrip(event) {
  event.preventDefault();
  if (!token) {
    alert("Inicie sessão primeiro.");
    return;
  }

  const payload = {
    plateNumber: document.getElementById("plateNumber").value,
    serviceSchedule: document.getElementById("serviceSchedule").value,
    lineCode: document.getElementById("lineCode").value,
    fleetNumber: document.getElementById("fleetNumber").value,
    plannedServiceId: selectedPlannedServiceId,
  };

  const response = await fetch(`${API_BASE}/services/start`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify(payload),
  });

  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao iniciar a viagem.");
    return;
  }

  activeServiceId = data.id;
  gpsPointQueue = [];
  gpsSaveFailureWarned = false;
  endTripBtn.disabled = false;
  handoverBtn.disabled = false;
  cancelTripBtn.disabled = false;
  document.getElementById("handoverToFleetNumber").value = payload.fleetNumber;
  routePolyline.setLatLngs([]);
  if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(
      (position) => {
        updateActiveBusMarker(position.coords.latitude, position.coords.longitude, data.fleet_number);
      },
      () => {}
    );
  }
  setSummary(
    [
      `Serviço: ${data.id}`,
      `Chapa: ${data.plate_number}`,
      `Horário: ${data.service_schedule}`,
      `Linha: ${data.line_code}`,
      `Frota: ${data.fleet_number}`,
      `Estado: ${labelEstadoExecucaoServicoPt(data.status)}`,
    ].join("\n")
  );

  await loadReferenceRoute(data.id);
  startLocationTracking();
  showDriverTab("driverStepMap");
}

async function loadReferenceRoute(serviceId) {
  if (!serviceId || !token) return;
  const response = await fetch(`${API_BASE}/services/${serviceId}/reference-route`, {
    headers: authHeaders(),
  });
  const data = await response.json();
  if (!response.ok || !data.points?.length) {
    referenceRoutePolyline.setLatLngs([]);
    gtfsStopsLayer.clearLayers();
    return;
  }

  const latLngs = data.points.map((p) => [p.lat, p.lng]);
  referenceRoutePolyline.setLatLngs(latLngs);
  if (routePolyline.getLatLngs().length === 0) {
    map.fitBounds(referenceRoutePolyline.getBounds(), { padding: [20, 20] });
  }

  gtfsStopsLayer.clearLayers();
  if (Array.isArray(data.stops) && data.stops.length) {
    data.stops.forEach((stop) => {
      if (typeof stop.lat !== "number" || typeof stop.lng !== "number") return;
      const marker = L.circleMarker([stop.lat, stop.lng], {
        radius: 5,
        color: "#1f2937",
        weight: 1,
        fillColor: "#facc15",
        fillOpacity: 0.95,
      });
      const timeText = stop.departureTime || stop.arrivalTime || "-";
      marker.bindPopup(
        `<strong>Paragem ${stop.sequence ?? "-"}</strong><br/>${stop.stopName || "-"}<br/>Horário: ${timeText}`
      );
      gtfsStopsLayer.addLayer(marker);
    });
  }
}

async function loadReferenceRoutePreview(lineCode, serviceSchedule) {
  const lc = String(lineCode || "").trim();
  const ss = String(serviceSchedule || "").trim();
  if (!lc || !ss || !token) return;
  const response = await fetch(
    `${API_BASE}/services/reference-route-preview/by-header?lineCode=${encodeURIComponent(lc)}&serviceSchedule=${encodeURIComponent(ss)}`,
    {
      headers: authHeaders(),
    }
  );
  const data = await response.json().catch(() => ({}));
  if (!response.ok || !data.points?.length) return;
  const latLngs = data.points.map((p) => [p.lat, p.lng]);
  referenceRoutePolyline.setLatLngs(latLngs);
  if (routePolyline.getLatLngs().length === 0 && latLngs.length > 1) {
    map.fitBounds(referenceRoutePolyline.getBounds(), { padding: [20, 20] });
  }
}

function startLocationTracking() {
  if (!("geolocation" in navigator)) {
    alert("Geolocalização indisponível neste navegador.");
    return;
  }

  if (watchId) navigator.geolocation.clearWatch(watchId);

  watchId = navigator.geolocation.watchPosition(
    async (position) => {
      const lat = position.coords.latitude;
      const lng = position.coords.longitude;

      routePolyline.addLatLng([lat, lng]);
      updateActiveBusMarker(lat, lng, fleetNumberInput.value.trim() || selectedPlannedFleetNumber);
      map.setView([lat, lng], 15);

      if (!activeServiceId || !token) return;
      gpsPointQueue.push({ lat, lng });
      await flushGpsPointQueue();
    },
    (error) => {
      console.error(error);
      alert("Não foi possível obter a localização.");
    },
    {
      enableHighAccuracy: true,
      maximumAge: 2000,
      timeout: 10000,
    }
  );
}

async function endTrip() {
  if (!activeServiceId || !token) return;

  await flushGpsPointQueue();
  if (gpsPointQueue.length) {
    alert(
      "Ainda existem pontos GPS por guardar. Verifique a ligação de dados/GPS e aguarde alguns segundos antes de finalizar."
    );
    return;
  }

  const response = await fetch(`${API_BASE}/services/${activeServiceId}/end`, {
    method: "POST",
    headers: authHeaders(),
  });
  const data = await response.json();

  if (!response.ok) {
    alert(data.message || "Erro ao finalizar a viagem.");
    return;
  }

  clearActiveServiceState();

  setSummary(
    [
      `Serviço finalizado: ${data.id}`,
      `Horário: ${data.service_schedule}`,
      `Linha: ${data.line_code}`,
      `Frota: ${data.fleet_number}`,
      `Quilómetros: ${data.total_km}`,
      `Início: ${new Date(data.started_at).toLocaleString()}`,
      `Fim: ${new Date(data.ended_at).toLocaleString()}`,
    ].join("\n")
  );

  if (data.route_geojson?.geometry?.coordinates?.length) {
    const latLngs = data.route_geojson.geometry.coordinates.map((c) => [c[1], c[0]]);
    routePolyline.setLatLngs(latLngs);
    map.fitBounds(routePolyline.getBounds(), { padding: [20, 20] });
  }
  referenceRoutePolyline.setLatLngs([]);
  gtfsStopsLayer.clearLayers();

  selectedPlannedServiceId = null;
  selectedPlannedFleetNumber = "";
  updateFleetWarning();
  showDriverTab("driverStepSelect");
  window.scrollTo({ top: 0, behavior: "smooth" });
  await loadTodayServices();
  await loadPendingHandovers();
  await refreshHistory();
}

async function cancelTrip() {
  if (!activeServiceId || !token) return;

  const confirmed = window.confirm(
    "Tem a certeza de que quer anular esta viagem? A viagem NÃO ficará concluída e poderá escolher outro serviço."
  );
  if (!confirmed) return;

  const response = await fetch(`${API_BASE}/services/${activeServiceId}/cancel`, {
    method: "POST",
    headers: authHeaders(),
  });
  const data = await response.json();

  if (!response.ok) {
    alert(data.message || "Erro ao anular a viagem.");
    return;
  }

  clearActiveServiceState();
  referenceRoutePolyline.setLatLngs([]);
  gtfsStopsLayer.clearLayers();
  routePolyline.setLatLngs([]);
  selectedPlannedServiceId = null;
  selectedPlannedFleetNumber = "";
  updateFleetWarning();
  setSummary("Viagem anulada. Pode selecionar o serviço correto.");
  showDriverTab("driverStepSelect");
  window.scrollTo({ top: 0, behavior: "smooth" });
  await loadTodayServices();
  await loadPendingHandovers();
  await refreshHistory();
}

async function loadTodayServices() {
  if (!token) return;

  const response = await fetch(`${API_BASE}/services/today-planned`, {
    headers: authHeaders(),
  });
  const data = await response.json();

  if (!response.ok) {
    alert(data.message || "Erro ao carregar os serviços de hoje.");
    return;
  }

  todayServicesList.innerHTML = "";
  if (!data.length) {
    const li = document.createElement("li");
    li.textContent = "Sem serviços previstos para hoje.";
    todayServicesList.appendChild(li);
    return;
  }

  const changedServices = data.filter((item) => item.is_roster_changed);
  const conflictByPlannedServiceId = new Set();
  const conflictsMap = new Map();
  if (changedServices.length) {
    data.forEach((item) => {
      const itemRange = parseServiceScheduleRangeMinutes(item.service_schedule);
      if (!itemRange) return;
      changedServices.forEach((changed) => {
        if (changed.planned_service_id === item.planned_service_id) return;
        const changedRange = parseServiceScheduleRangeMinutes(changed.service_schedule);
        if (!changedRange) return;
        if (scheduleRangesOverlap(itemRange, changedRange)) {
          conflictByPlannedServiceId.add(item.planned_service_id);
          conflictByPlannedServiceId.add(changed.planned_service_id);
          if (!conflictsMap.has(item.planned_service_id)) conflictsMap.set(item.planned_service_id, new Set());
          if (!conflictsMap.has(changed.planned_service_id)) conflictsMap.set(changed.planned_service_id, new Set());
          conflictsMap.get(item.planned_service_id).add(changed.planned_service_id);
          conflictsMap.get(changed.planned_service_id).add(item.planned_service_id);
        }
      });
    });
  }

  data.forEach((item) => {
    const li = document.createElement("li");
    const btn = document.createElement("button");
    let startLocation = String(item.start_location || "").trim();
    let endLocation = String(item.end_location || "").trim();
    if ((!startLocation || startLocation === "-") && (!endLocation || endLocation === "-")) {
      const [fromLineCode, toLineCode] = String(item.line_code || "").split("->").map((part) => part.trim());
      if (fromLineCode && toLineCode) {
        startLocation = fromLineCode;
        endLocation = toLineCode;
      }
    }
    startLocation = startLocation || "-";
    endLocation = endLocation || "-";
    btn.type = "button";
    const isConflict = conflictByPlannedServiceId.has(item.planned_service_id);
    btn.className = `planned-service-btn${item.is_roster_changed ? " planned-service-btn--roster-change" : ""}${isConflict ? " planned-service-btn--conflict" : ""}`;
    const kmsCargaText = item.kms_carga == null ? "-" : Number(item.kms_carga).toFixed(3);
    btn.innerHTML = `
      <span class="planned-service-line1">Linha ${item.line_code} | ${item.service_schedule} | Frota ${item.fleet_number} | Chapa ${item.plate_number} | Km carga ${kmsCargaText}</span>
      <span class="planned-service-line2">${startLocation} → ${endLocation} | ${labelEstadoEscalaPt(item.roster_status)}${isConflict ? " | Conflito de horário" : ""}</span>
    `;
    if (isConflict && item.is_roster_changed) {
      const notifyBtn = document.createElement("button");
      const alreadyNotified = conflictNotifiedPlannedServiceIds.has(item.planned_service_id);
      notifyBtn.type = "button";
      notifyBtn.className = "notify-supervisor-btn";
      notifyBtn.textContent = alreadyNotified ? "Supervisor notificado" : "Notificar supervisor";
      notifyBtn.disabled = alreadyNotified;
      notifyBtn.addEventListener("click", async (event) => {
        event.stopPropagation();
        if (conflictNotifiedPlannedServiceIds.has(item.planned_service_id)) return;
        try {
          const conflictIds = Array.from(conflictsMap.get(item.planned_service_id) || []);
          const response = await fetch(`${API_BASE}/services/conflicts/notify-supervisor`, {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify({
              rosterId: item.roster_id,
              plannedServiceId: item.planned_service_id,
              serviceSchedule: item.service_schedule,
              lineCode: item.line_code,
              conflictPlannedServiceIds: conflictIds,
              notes: "Conflito de horario detetado pelo motorista na escala do dia.",
            }),
          });
          const payload = await response.json().catch(() => ({}));
          if (!response.ok) {
            alert(payload.message || "Não foi possível notificar o supervisor.");
            return;
          }
          conflictNotifiedPlannedServiceIds.add(item.planned_service_id);
          notifyBtn.textContent = "Supervisor notificado";
          notifyBtn.disabled = true;
          alert("Supervisor notificado sobre o conflito de horário.");
        } catch (_error) {
          alert("Erro de ligação ao notificar o supervisor.");
        }
      });
      li.appendChild(notifyBtn);
    }
    btn.addEventListener("click", () => {
      selectedPlannedServiceId = item.planned_service_id;
      selectedPlannedFleetNumber = item.fleet_number;
      document.getElementById("plateNumber").value = item.plate_number;
      document.getElementById("serviceSchedule").value = item.service_schedule;
      document.getElementById("lineCode").value = item.line_code;
      document.getElementById("fleetNumber").value = item.fleet_number;
      updateFleetWarning();
      loadReferenceRoutePreview(item.line_code, item.service_schedule);
      alert(`Serviço ${item.service_code} selecionado.`);
      showDriverTab("driverStepStart");
    });
    li.appendChild(btn);
    todayServicesList.appendChild(li);
  });
}

async function loadPendingHandovers() {
  if (!token) return;

  const response = await fetch(`${API_BASE}/services/pending-handover`, {
    headers: authHeaders(),
  });
  const data = await response.json();
  if (!response.ok) return;

  pendingHandoversList.innerHTML = "";
  if (!data.length) {
    const li = document.createElement("li");
    li.textContent = "Sem transferências pendentes para este motorista.";
    pendingHandoversList.appendChild(li);
    return;
  }

  data.forEach((item) => {
    const li = document.createElement("li");
    const info = document.createElement("span");
    info.textContent =
      `Serviço n.º ${item.service_id} | Linha ${item.line_code} | ${item.service_schedule} | Motivo: ${item.reason} | De: ${item.from_driver_name}`;
    const btn = document.createElement("button");
    btn.type = "button";
    btn.textContent = "Assumir serviço";
    btn.addEventListener("click", async () => {
      const resumeResponse = await fetch(`${API_BASE}/services/${item.service_id}/resume`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ handoverId: item.handover_id }),
      });
      const resumeData = await resumeResponse.json();
      if (!resumeResponse.ok) {
        alert(resumeData.message || "Erro ao assumir o serviço.");
        return;
      }
      activeServiceId = item.service_id;
      endTripBtn.disabled = false;
      handoverBtn.disabled = false;
      cancelTripBtn.disabled = false;
      document.getElementById("fleetNumber").value = resumeData.service.fleet_number || "";
      setSummary(
        [
          `Serviço retomado: ${resumeData.service.id}`,
          `Linha: ${resumeData.service.line_code}`,
          `Frota: ${resumeData.service.fleet_number}`,
          `Estado: ${labelEstadoExecucaoServicoPt(resumeData.service.status)}`,
        ].join("\n")
      );
      await loadReferenceRoute(item.service_id);
      startLocationTracking();
      await loadPendingHandovers();
      await refreshHistory();
      alert("Serviço assumido com sucesso.");
      showDriverTab("driverStepMap");
    });
    li.appendChild(info);
    li.appendChild(document.createTextNode(" "));
    li.appendChild(btn);
    pendingHandoversList.appendChild(li);
  });
}

async function transferService(event) {
  event.preventDefault();
  if (!activeServiceId || !token) {
    alert("Não existe um serviço em curso para transferir.");
    return;
  }

  const toMechanicNumber = document.getElementById("handoverToMechanicNumber").value.trim();
  const toFleetNumber =
    document.getElementById("handoverToFleetNumber").value.trim() || fleetNumberInput.value.trim();
  const reason = document.getElementById("handoverReason").value.trim();
  const notes = document.getElementById("handoverNotes").value.trim();
  const handoverLocationText = document.getElementById("handoverLocationText").value.trim();

  if (!toMechanicNumber || !reason) {
    alert("Indique o número mecanográfico do motorista de destino e o motivo.");
    return;
  }

  try {
    const checkRes = await fetch(
      `${API_BASE}/services/${activeServiceId}/handover-roster-overlap-check?mechanicNumber=${encodeURIComponent(
        toMechanicNumber
      )}`,
      { headers: authHeaders() }
    );
    if (checkRes.ok) {
      const checkData = await checkRes.json();
      if (Array.isArray(checkData.conflicts) && checkData.conflicts.length) {
        const lines = checkData.conflicts
          .map(
            (c) =>
              `· Linha ${c.line_code || "-"} | ${c.service_schedule || "-"} | ${c.service_code || "-"} (escala: ${labelEstadoEscalaPt(
                c.roster_status
              )})`
          )
          .join("\n");
        const proceed = window.confirm(
          `O motorista de destino tem serviços escalados neste dia com horário sobreposto ao da viagem em curso:\n\n${lines}\n\nDeseja continuar com a transferência? Pode depois ajustar a sobreposição na escala (supervisor).`
        );
        if (!proceed) return;
      }
    }
  } catch (_e) {
    // Se a verificação falhar, segue para o pedido de transferência.
  }

  const response = await fetch(`${API_BASE}/services/${activeServiceId}/handover`, {
    method: "POST",
    headers: authHeaders(),
    body: JSON.stringify({ toMechanicNumber, toFleetNumber, reason, notes, handoverLocationText }),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao transferir o serviço.");
    return;
  }

  clearActiveServiceState();
  setSummary("Serviço transferido com sucesso. À espera do motorista de substituição.");
  showDriverTab("driverStepSelect");
  window.scrollTo({ top: 0, behavior: "smooth" });
  alert("Serviço transferido.");
}

async function refreshHistory() {
  if (!token) return;

  const response = await fetch(`${API_BASE}/services/history-detailed`, {
    headers: authHeaders(),
  });
  const data = await response.json();

  if (!response.ok) return;

  historyList.innerHTML = "";
  data.forEach((item) => {
    const li = document.createElement("li");
    const serviceStatus = String(item.status || "").toLowerCase();

    const initiated = item.initiated_by
      ? `Iniciado por ${item.initiated_by.driverName} (Mec. ${item.initiated_by.mechanicNumber || "-"}) | Frota inicial ${item.initiated_by.fleetNumber}`
      : "Iniciador não identificado";

    const segmentDetails =
      item.segments?.length
        ? item.segments
            .map(
              (seg, index) =>
                `Segmento ${index + 1}: ${seg.driver_name} (Mec. ${seg.mechanic_number || "-"}) | Frota ${seg.fleet_number} | ${seg.km_segment} km | ${labelEstadoExecucaoServicoPt(seg.status)}`
            )
            .join("\n")
        : "Sem segmentos.";

    const handoverDetails =
      item.handovers?.length
        ? item.handovers
            .map((h, index) => {
              const local =
                h.handover_location_text ||
                (h.handover_lat && h.handover_lng
                  ? `GPS ${Number(h.handover_lat).toFixed(5)}, ${Number(h.handover_lng).toFixed(5)}`
                  : "Local não registado");
              return `Transferência ${index + 1}: ${h.from_driver_name} → ${h.to_driver_name || "pendente"} | Frota ${h.from_fleet_number} → ${h.to_fleet_number} | Motivo: ${h.reason} | Local: ${local} | Estado: ${labelEstadoTransferenciaPt(h.status)}`;
            })
            .join("\n")
        : "Sem transferências.";

    const statusBadgeText =
      serviceStatus === "cancelled"
        ? "ANULADO (erro corrigido)"
        : serviceStatus === "completed"
          ? "CONCLUÍDO"
          : labelEstadoExecucaoServicoPt(item.status);
    const statusBadgeClass = serviceStatus === "cancelled" ? "history-badge history-badge--cancelled" : "history-badge";

    li.innerHTML = `
      <div><strong>Serviço n.º ${item.id}</strong> <span class="${statusBadgeClass}">${statusBadgeText}</span></div>
      <div>Horário: ${item.service_schedule}</div>
      <div>Linha: ${item.line_code}</div>
      <div>Frota final: ${item.fleet_number}</div>
      <div>Quilómetros realizados: ${item.total_km} km</div>
      <pre>${initiated}
${handoverDetails}
${segmentDetails}</pre>
    `;
    historyList.appendChild(li);
  });
}

document.getElementById("loginForm").addEventListener("submit", login);
document.getElementById("serviceForm").addEventListener("submit", startTrip);
document.getElementById("refreshHistoryBtn").addEventListener("click", refreshHistory);
document.getElementById("refreshTodayServicesBtn").addEventListener("click", async () => {
  await loadTodayServices();
  await loadPendingHandovers();
  await refreshDriverNotifications();
});
document.getElementById("refreshPendingHandoversBtn").addEventListener("click", loadPendingHandovers);
document.getElementById("handoverForm").addEventListener("submit", transferService);
fleetNumberInput.addEventListener("input", updateFleetWarning);
document.getElementById("lineCode").addEventListener("change", () => {
  loadReferenceRoutePreview(document.getElementById("lineCode").value, document.getElementById("serviceSchedule").value);
});
document.getElementById("serviceSchedule").addEventListener("change", () => {
  loadReferenceRoutePreview(document.getElementById("lineCode").value, document.getElementById("serviceSchedule").value);
});
endTripBtn.addEventListener("click", endTrip);
cancelTripBtn.addEventListener("click", cancelTrip);
document.getElementById("logoutBtn").addEventListener("click", logoutDriver);
initDriverTabs();

(() => {
  const raw = sessionStorage.getItem(AUTH_SESSION_KEY);
  if (!raw) return;
  try {
    const session = JSON.parse(raw);
    if (!session?.token || !session?.user) return;
    const role = String(session.user.role || "").trim().toLowerCase();
    if (role === "supervisor" || role === "admin") {
      window.location.href = `${window.location.origin}/frontend/supervisor.html`;
      return;
    }
    if (role === "viewer" || role === "viewr") {
      window.location.href = `${window.location.origin}/frontend/viewer.html`;
    }
  } catch (_error) {
    // ignore invalid persisted session
  }
})();
