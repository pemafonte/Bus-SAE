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

let token = "";
let activeServiceId = null;
let watchId = null;
let selectedPlannedServiceId = null;
let selectedPlannedFleetNumber = "";
let authenticatedUser = null;
const AUTH_SESSION_KEY = "auth_session";

const loginScreenEl = document.getElementById("loginScreen");
const appScreenEl = document.getElementById("appScreen");
const sessionWelcomeEl = document.getElementById("sessionWelcome");
const logoutBtnEl = document.getElementById("logoutBtn");
const summaryEl = document.getElementById("serviceSummary");
const endTripBtn = document.getElementById("endTripBtn");
const handoverBtn = document.getElementById("handoverBtn");
const historyList = document.getElementById("historyList");
const todayServicesList = document.getElementById("todayServicesList");
const pendingHandoversList = document.getElementById("pendingHandoversList");
const fleetNumberInput = document.getElementById("fleetNumber");
const fleetWarningEl = document.getElementById("fleetWarning");
const driverTabsEl = document.getElementById("driverTabs");
const driverNotificationsBarEl = document.getElementById("driverNotificationsBar");

const map = L.map("map").setView([38.7223, -9.1393], 12);
L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
  maxZoom: 19,
  attribution: "&copy; OpenStreetMap contributors",
}).addTo(map);

const routePolyline = L.polyline([], { color: "#2563eb", weight: 5 }).addTo(map);
const referenceRoutePolyline = L.polyline([], { color: "#f97316", weight: 4, dashArray: "8 8" }).addTo(map);
const gtfsStopsLayer = L.layerGroup().addTo(map);
handoverBtn.disabled = true;

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
  endTripBtn.disabled = true;
  handoverBtn.disabled = true;
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
  endTripBtn.disabled = false;
  handoverBtn.disabled = false;
  document.getElementById("handoverToFleetNumber").value = payload.fleetNumber;
  routePolyline.setLatLngs([]);
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
      map.setView([lat, lng], 15);

      if (!activeServiceId || !token) return;

      const pointResponse = await fetch(`${API_BASE}/services/${activeServiceId}/points`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ lat, lng }),
      });
      const pointData = await pointResponse.json().catch(() => ({}));
      if (pointResponse.ok && pointData.routeCheck) {
        const offRouteText = pointData.routeCheck.isOffRoute ? "Fora da rota" : "Dentro da rota";
        const deviationText =
          pointData.routeCheck.deviationMeters == null ? "-" : `${pointData.routeCheck.deviationMeters} m`;
        const currentSummary = summaryEl.textContent.split("\n").filter((line) => !line.startsWith("Rota:"));
        currentSummary.push(`Rota: ${offRouteText} | Desvio: ${deviationText}`);
        setSummary(currentSummary.join("\n"));
      }
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

  const response = await fetch(`${API_BASE}/services/${activeServiceId}/end`, {
    method: "POST",
    headers: authHeaders(),
  });
  const data = await response.json();

  if (!response.ok) {
    alert(data.message || "Erro ao finalizar a viagem.");
    return;
  }

  endTripBtn.disabled = true;
  handoverBtn.disabled = true;
  activeServiceId = null;
  if (watchId) {
    navigator.geolocation.clearWatch(watchId);
    watchId = null;
  }

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

  data.forEach((item) => {
    const li = document.createElement("li");
    const btn = document.createElement("button");
    const startLocation = item.start_location || "-";
    const endLocation = item.end_location || "-";
    btn.type = "button";
    btn.className = "planned-service-btn";
    btn.innerHTML = `
      <span class="planned-service-line1">Linha ${item.line_code} | ${item.service_schedule} | Frota ${item.fleet_number} | Chapa ${item.plate_number}</span>
      <span class="planned-service-line2">${startLocation} → ${endLocation} | ${labelEstadoEscalaPt(item.roster_status)}</span>
    `;
    btn.addEventListener("click", () => {
      selectedPlannedServiceId = item.planned_service_id;
      selectedPlannedFleetNumber = item.fleet_number;
      document.getElementById("plateNumber").value = item.plate_number;
      document.getElementById("serviceSchedule").value = item.service_schedule;
      document.getElementById("lineCode").value = item.line_code;
      document.getElementById("fleetNumber").value = item.fleet_number;
      updateFleetWarning();
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
      document.getElementById("fleetNumber").value = resumeData.service.fleet_number || "";
      setSummary(
        [
          `Serviço retomado: ${resumeData.service.id}`,
          `Linha: ${resumeData.service.line_code}`,
          `Frota: ${resumeData.service.fleet_number}`,
          `Estado: ${labelEstadoExecucaoServicoPt(resumeData.service.status)}`,
        ].join("\n")
      );
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

  if (watchId) {
    navigator.geolocation.clearWatch(watchId);
    watchId = null;
  }
  activeServiceId = null;
  endTripBtn.disabled = true;
  handoverBtn.disabled = true;
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

    li.textContent = [
      `Serviço n.º ${item.id}`,
      `Horário: ${item.service_schedule}`,
      `Linha: ${item.line_code}`,
      `Frota final: ${item.fleet_number}`,
      `Quilómetros realizados: ${item.total_km} km`,
      initiated,
      handoverDetails,
      segmentDetails,
    ].join("\n");
    historyList.appendChild(li);
  });
}

document.getElementById("loginForm").addEventListener("submit", login);
document.getElementById("serviceForm").addEventListener("submit", startTrip);
document.getElementById("refreshHistoryBtn").addEventListener("click", refreshHistory);
document.getElementById("refreshTodayServicesBtn").addEventListener("click", async () => {
  await loadTodayServices();
  await refreshDriverNotifications();
});
document.getElementById("refreshPendingHandoversBtn").addEventListener("click", loadPendingHandovers);
document.getElementById("handoverForm").addEventListener("submit", transferService);
fleetNumberInput.addEventListener("input", updateFleetWarning);
endTripBtn.addEventListener("click", endTrip);
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
