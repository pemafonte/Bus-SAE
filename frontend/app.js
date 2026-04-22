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

function parseGtfsTimeToParts(gtfsTime) {
  const text = String(gtfsTime || "").trim();
  if (!text.includes(":")) return null;
  const chunks = text.split(":");
  if (chunks.length < 2) return null;
  const hh = Number(chunks[0]);
  const mm = Number(chunks[1]);
  const ss = Number(chunks[2] || 0);
  if (!Number.isFinite(hh) || !Number.isFinite(mm) || !Number.isFinite(ss)) return null;
  return { hh, mm, ss };
}

function toStopScheduledDate(stop, baseStartedAt) {
  const raw = stop?.departureTime || stop?.arrivalTime;
  const parsed = parseGtfsTimeToParts(raw);
  if (!parsed || !baseStartedAt) return null;
  const base = new Date(baseStartedAt);
  if (Number.isNaN(base.getTime())) return null;
  const d = new Date(base);
  d.setHours(0, 0, 0, 0);
  const dayOffset = Math.floor(parsed.hh / 24);
  const hourInDay = parsed.hh % 24;
  d.setDate(d.getDate() + dayOffset);
  d.setHours(hourInDay, parsed.mm, parsed.ss, 0);
  return d;
}

function distanceMeters(aLat, aLng, bLat, bLng) {
  const r = 6371000;
  const dLat = ((bLat - aLat) * Math.PI) / 180;
  const dLng = ((bLng - aLng) * Math.PI) / 180;
  const lat1 = (aLat * Math.PI) / 180;
  const lat2 = (bLat * Math.PI) / 180;
  const h =
    Math.sin(dLat / 2) * Math.sin(dLat / 2) +
    Math.cos(lat1) * Math.cos(lat2) * Math.sin(dLng / 2) * Math.sin(dLng / 2);
  return 2 * r * Math.asin(Math.sqrt(h));
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
let activeServiceSyncTimer = null;
let driverMessagesRefreshTimer = null;
let driverAlertsPollTimer = null;
let driverAlertBaselineReady = false;
let lastDriverAlertSignature = "";
let todayServicesCache = [];
let activeReferenceStops = [];
let activeStopProgressIndex = 0;
let activeServiceStartedAt = null;
const AUTH_SESSION_KEY = "auth_session";
const GPS_QUEUE_STORAGE_KEY = "gps_point_queue_v1";
const GPS_BATCH_SIZE = 50;
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
const driverMessageFormEl = document.getElementById("driverMessageForm");
const driverMessagePresetEl = document.getElementById("driverMessagePreset");
const driverMessageTextEl = document.getElementById("driverMessageText");
const driverMessageTrafficAlertEl = document.getElementById("driverMessageTrafficAlert");
const refreshDriverMessagesBtnEl = document.getElementById("refreshDriverMessagesBtn");
const driverMessagesListEl = document.getElementById("driverMessagesList");
const driverAlertSoundTypeEl = document.getElementById("driverAlertSoundType");
const driverAlertSoundVolumeEl = document.getElementById("driverAlertSoundVolume");
const testDriverAlertSoundBtnEl = document.getElementById("testDriverAlertSoundBtn");
const driverNextStopDelayEl = document.getElementById("driverNextStopDelay");
const driverNextStopInfoEl = document.getElementById("driverNextStopInfo");
const driverMapLiveStateEl = document.getElementById("driverMapLiveState");
const mapTodayServicesListEl = document.getElementById("mapTodayServicesList");
const mapStopsTimelineEl = document.getElementById("mapStopsTimeline");

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

function stopActiveServiceSync() {
  if (activeServiceSyncTimer) {
    clearInterval(activeServiceSyncTimer);
    activeServiceSyncTimer = null;
  }
}

function startActiveServiceSync() {
  stopActiveServiceSync();
  activeServiceSyncTimer = setInterval(() => {
    syncActiveServiceFromServer();
  }, 45_000);
}

function stopDriverMessagesRefresh() {
  if (driverMessagesRefreshTimer) {
    clearInterval(driverMessagesRefreshTimer);
    driverMessagesRefreshTimer = null;
  }
}

function startDriverMessagesRefresh() {
  stopDriverMessagesRefresh();
  driverMessagesRefreshTimer = setInterval(() => {
    refreshDriverMessages();
  }, 20_000);
}

async function syncActiveServiceFromServer() {
  if (!token) return;
  const prevId = activeServiceId;
  if (!prevId) return;
  try {
    const response = await fetch(`${API_BASE}/services/active`, { headers: authHeaders() });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) return;
    if (data.activeService && Number(data.activeService.id) === Number(prevId)) return;
    if (!data.activeService) {
      stopActiveServiceSync();
      await refreshDriverNotifications();
      clearActiveServiceState();
      routePolyline.setLatLngs([]);
      referenceRoutePolyline.setLatLngs([]);
      gtfsStopsLayer.clearLayers();
      setSummary("Sem viagem em curso.");
      alert(
        "A viagem já não está ativa (por exemplo, encerramento automático pelo sistema após o horário de escala). Consulte as notificações."
      );
      showDriverTab("driverStepSelect");
      await loadTodayServices();
      await loadPendingHandovers();
      await refreshHistory();
    }
  } catch (_e) {
    /* rede: ignorar */
  }
}

function clearActiveServiceState() {
  stopActiveServiceSync();
  activeServiceId = null;
  gpsPointQueue = [];
  persistGpsQueue();
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
  activeReferenceStops = [];
  activeStopProgressIndex = 0;
  activeServiceStartedAt = null;
  renderStopsTimeline(0);
  resetNextStopDelayUi();
  if (driverMapLiveStateEl) driverMapLiveStateEl.textContent = "Sem viagem em execução.";
}

function persistGpsQueue() {
  try {
    const payload = {
      serviceId: activeServiceId || null,
      points: gpsPointQueue,
    };
    localStorage.setItem(GPS_QUEUE_STORAGE_KEY, JSON.stringify(payload));
  } catch (_error) {
    // ignore storage errors
  }
}

function restoreGpsQueueForActiveService() {
  try {
    const raw = localStorage.getItem(GPS_QUEUE_STORAGE_KEY);
    if (!raw) return;
    const parsed = JSON.parse(raw);
    const savedServiceId = Number(parsed?.serviceId);
    const currentServiceId = Number(activeServiceId);
    if (!savedServiceId || !currentServiceId || savedServiceId !== currentServiceId) {
      gpsPointQueue = [];
      persistGpsQueue();
      return;
    }
    gpsPointQueue = Array.isArray(parsed.points) ? parsed.points : [];
  } catch (_error) {
    gpsPointQueue = [];
  }
}

function enqueueGpsPoint(point) {
  gpsPointQueue.push(point);
  persistGpsQueue();
}

function dequeueGpsPoints(count) {
  gpsPointQueue.splice(0, count);
  persistGpsQueue();
}

async function flushGpsPointQueue() {
  if (gpsFlushInProgress || !gpsPointQueue.length || !activeServiceId || !token) return;
  gpsFlushInProgress = true;
  try {
    while (gpsPointQueue.length && activeServiceId && token) {
      const batch = gpsPointQueue.slice(0, GPS_BATCH_SIZE);
      const pointResponse = await fetch(`${API_BASE}/services/${activeServiceId}/points/batch`, {
        method: "POST",
        headers: authHeaders(),
        body: JSON.stringify({ points: batch }),
      });
      const pointData = await pointResponse.json().catch(() => ({}));
      if (!pointResponse.ok) {
        // Compatibilidade com versões antigas sem endpoint batch.
        if (pointResponse.status === 404 || pointResponse.status === 405) {
          const nextPoint = gpsPointQueue[0];
          const fallbackResponse = await fetch(`${API_BASE}/services/${activeServiceId}/points`, {
            method: "POST",
            headers: authHeaders(),
            body: JSON.stringify(nextPoint),
          });
          const fallbackData = await fallbackResponse.json().catch(() => ({}));
          if (fallbackResponse.ok) {
            dequeueGpsPoints(1);
            gpsSaveFailureWarned = false;
            if (fallbackData.routeCheck) {
              const offRouteText = fallbackData.routeCheck.isOffRoute ? "Fora da rota" : "Dentro da rota";
              const deviationText =
                fallbackData.routeCheck.deviationMeters == null ? "-" : `${fallbackData.routeCheck.deviationMeters} m`;
              const currentSummary = summaryEl.textContent.split("\n").filter((line) => !line.startsWith("Rota:"));
              currentSummary.push(`Rota: ${offRouteText} | Desvio: ${deviationText}`);
              setSummary(currentSummary.join("\n"));
            }
            continue;
          }
        }
        if (!gpsSaveFailureWarned) {
          gpsSaveFailureWarned = true;
          alert("A ligação GPS está instável. Vamos continuar a tentar guardar os pontos do percurso.");
        }
        break;
      }
      dequeueGpsPoints(pointData.acceptedCount || batch.length);
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
  if (tabId === "driverStepComms") {
    refreshDriverMessages();
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

const DRIVER_ALERT_SOUND_SETTINGS_KEY = "driver_alert_sound_settings_v1";

function loadDriverAlertSoundSettings() {
  try {
    const raw = localStorage.getItem(DRIVER_ALERT_SOUND_SETTINGS_KEY);
    const parsed = raw ? JSON.parse(raw) : null;
    return {
      type: parsed?.type || "beep",
      volume: Number.isFinite(Number(parsed?.volume)) ? Number(parsed.volume) : 70,
    };
  } catch (_error) {
    return { type: "beep", volume: 70 };
  }
}

function saveDriverAlertSoundSettings(settings) {
  localStorage.setItem(DRIVER_ALERT_SOUND_SETTINGS_KEY, JSON.stringify(settings));
}

function getDriverAlertSoundSettings() {
  return {
    type: String(driverAlertSoundTypeEl?.value || "beep"),
    volume: Number(driverAlertSoundVolumeEl?.value || 70),
  };
}

function playPatternTone(ctx, startAt, frequency, duration, volume, waveType) {
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

function playDriverAlertSound() {
  const settings = getDriverAlertSoundSettings();
  const AudioCtx = window.AudioContext || window.webkitAudioContext;
  if (!AudioCtx) return;
  const ctx = new AudioCtx();
  const now = ctx.currentTime;
  const volume = Math.max(0, Math.min(1, settings.volume / 100)) * 0.25;
  if (settings.type === "urgent") {
    playPatternTone(ctx, now, 820, 0.11, volume, "sawtooth");
    playPatternTone(ctx, now + 0.16, 740, 0.11, volume, "sawtooth");
    playPatternTone(ctx, now + 0.32, 920, 0.13, volume, "sawtooth");
  } else if (settings.type === "chime") {
    playPatternTone(ctx, now, 660, 0.12, volume, "triangle");
    playPatternTone(ctx, now + 0.15, 880, 0.18, volume, "triangle");
  } else {
    playPatternTone(ctx, now, 880, 0.15, volume, "square");
  }
}

function applyDriverAlertSoundSettingsToUi() {
  const settings = loadDriverAlertSoundSettings();
  if (driverAlertSoundTypeEl) driverAlertSoundTypeEl.value = settings.type;
  if (driverAlertSoundVolumeEl) driverAlertSoundVolumeEl.value = String(settings.volume);
}

function updateDriverAlertSignature() {
  if (!token) return;
  Promise.allSettled([
    fetch(`${API_BASE}/services/notifications`, { headers: authHeaders() }).then((r) => r.json().then((d) => ({ ok: r.ok, d }))),
    fetch(`${API_BASE}/services/messages`, { headers: authHeaders() }).then((r) => r.json().then((d) => ({ ok: r.ok, d }))),
    fetch(`${API_BASE}/services/pending-handover`, { headers: authHeaders() }).then((r) => r.json().then((d) => ({ ok: r.ok, d }))),
  ]).then((results) => {
    const notif = results[0]?.status === "fulfilled" && results[0].value.ok ? results[0].value.d : [];
    const msgs = results[1]?.status === "fulfilled" && results[1].value.ok ? results[1].value.d : [];
    const handovers = results[2]?.status === "fulfilled" && results[2].value.ok ? results[2].value.d : [];
    const unreadNotif = (Array.isArray(notif) ? notif : []).filter((n) => !n.read_at).map((n) => n.id).sort((a, b) => a - b);
    const unreadMsgs = (Array.isArray(msgs) ? msgs : [])
      .filter((m) => !m.read_at && Number(m.to_user_id) === Number(authenticatedUser?.id))
      .map((m) => `${m.id}:${m.is_traffic_alert ? 1 : 0}`)
      .sort();
    const pendingHandovers = (Array.isArray(handovers) ? handovers : []).map((h) => h.handover_id || h.id).sort((a, b) => a - b);
    const signature = JSON.stringify({ unreadNotif, unreadMsgs, pendingHandovers });
    if (driverAlertBaselineReady && signature !== lastDriverAlertSignature) {
      playDriverAlertSound();
    }
    lastDriverAlertSignature = signature;
    if (!driverAlertBaselineReady) driverAlertBaselineReady = true;
  });
}

function startDriverAlertsPolling() {
  stopDriverAlertsPolling();
  updateDriverAlertSignature();
  driverAlertsPollTimer = setInterval(() => {
    updateDriverAlertSignature();
  }, 15_000);
}

function stopDriverAlertsPolling() {
  if (driverAlertsPollTimer) {
    clearInterval(driverAlertsPollTimer);
    driverAlertsPollTimer = null;
  }
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

async function loadDriverMessagePresets() {
  if (!token || !driverMessagePresetEl) return;
  try {
    const response = await fetch(`${API_BASE}/services/message-presets`, {
      headers: authHeaders(),
    });
    const data = await response.json().catch(() => []);
    if (!response.ok || !Array.isArray(data)) return;
    driverMessagePresetEl.innerHTML = '<option value="">-- Nenhuma (escrever manualmente) --</option>';
    data.forEach((item) => {
      const option = document.createElement("option");
      option.value = item.code || "";
      option.textContent = item.label || item.code || "Preset";
      driverMessagePresetEl.appendChild(option);
    });
  } catch (_error) {
    // ignore
  }
}

function buildDriverMessageItem(message) {
  const li = document.createElement("li");
  li.className = "ops-message-item";
  if (message.is_traffic_alert) li.classList.add("traffic-alert");
  const meta = document.createElement("div");
  meta.className = "ops-message-meta";
  const direction = Number(message.from_user_id) === Number(authenticatedUser?.id) ? "Enviada" : "Recebida";
  const createdText = message.created_at ? new Date(message.created_at).toLocaleString() : "-";
  const trafficText = message.is_traffic_alert ? " | ALERTA TRANSITO" : "";
  meta.textContent = `${direction} ${createdText}${trafficText}`;
  const author = document.createElement("strong");
  author.textContent = `${message.from_name || "Utilizador"} -> ${message.to_name || "Utilizador"}`;
  const body = document.createElement("p");
  body.textContent = message.message_text || "";
  li.appendChild(meta);
  li.appendChild(author);
  li.appendChild(body);
  return li;
}

async function refreshDriverMessages() {
  if (!token || !driverMessagesListEl) return;
  try {
    const response = await fetch(`${API_BASE}/services/messages`, {
      headers: authHeaders(),
    });
    const data = await response.json().catch(() => []);
    if (!response.ok) return;
    const list = Array.isArray(data) ? data : [];
    driverMessagesListEl.innerHTML = "";
    if (!list.length) {
      const li = document.createElement("li");
      li.textContent = "Sem mensagens operacionais.";
      driverMessagesListEl.appendChild(li);
      return;
    }
    const chronological = [...list].reverse();
    for (const item of chronological) {
      driverMessagesListEl.appendChild(buildDriverMessageItem(item));
      if (!item.read_at && Number(item.to_user_id) === Number(authenticatedUser?.id)) {
        fetch(`${API_BASE}/services/messages/${item.id}/read`, {
          method: "PATCH",
          headers: authHeaders(),
        }).catch(() => {});
      }
    }
  } catch (_error) {
    // ignore
  }
}

async function sendDriverMessage(event) {
  event.preventDefault();
  if (!token || !driverMessageTextEl) return;
  const message = driverMessageTextEl.value.trim();
  if (!message) {
    alert("Escreva a mensagem para enviar.");
    return;
  }
  const payload = {
    message,
    presetCode: driverMessagePresetEl?.value || null,
    isTrafficAlert: driverMessageTrafficAlertEl?.checked === true,
    relatedServiceId: activeServiceId || null,
  };
  try {
    const response = await fetch(`${API_BASE}/services/messages`, {
      method: "POST",
      headers: authHeaders(),
      body: JSON.stringify(payload),
    });
    const data = await response.json().catch(() => ({}));
    if (!response.ok) {
      alert(data.message || "Erro ao enviar mensagem.");
      return;
    }
    driverMessageTextEl.value = "";
    if (driverMessagePresetEl) driverMessagePresetEl.value = "";
    if (driverMessageTrafficAlertEl) driverMessageTrafficAlertEl.checked = false;
    await refreshDriverMessages();
    alert("Mensagem enviada ao supervisor.");
  } catch (_error) {
    alert("Nao foi possivel enviar a mensagem.");
  }
}

function setSummary(text) {
  summaryEl.textContent = text;
}

function renderMapTodayServicesPanel() {
  if (!mapTodayServicesListEl) return;
  mapTodayServicesListEl.innerHTML = "";
  if (!todayServicesCache.length) {
    const li = document.createElement("li");
    li.textContent = "Sem serviços carregados.";
    mapTodayServicesListEl.appendChild(li);
    return;
  }
  todayServicesCache.forEach((item) => {
    const li = document.createElement("li");
    const isActive = Number(activeServiceId) === Number(item.current_service_id || item.service_id || item.id);
    li.innerHTML = `<strong>${item.service_code || "-"}</strong> | Linha ${item.line_code || "-"} | ${item.service_schedule || "-"}${
      isActive ? " | EM EXECUCAO" : ""
    }`;
    mapTodayServicesListEl.appendChild(li);
  });
}

function renderStopsTimeline(nextIndex = 0) {
  if (!mapStopsTimelineEl) return;
  mapStopsTimelineEl.innerHTML = "";
  if (!activeReferenceStops.length) {
    const li = document.createElement("li");
    li.textContent = "Sem paragens GTFS para este serviço.";
    mapStopsTimelineEl.appendChild(li);
    return;
  }
  activeReferenceStops.forEach((stop, index) => {
    const li = document.createElement("li");
    if (index < nextIndex) li.classList.add("stop-passed");
    if (index === nextIndex) li.classList.add("stop-next");
    const hhmm = String(stop.departureTime || stop.arrivalTime || "-");
    li.textContent = `#${stop.sequence ?? index + 1} ${stop.stopName || "-"} (${hhmm})`;
    mapStopsTimelineEl.appendChild(li);
  });
}

function resetNextStopDelayUi() {
  if (driverNextStopDelayEl) {
    driverNextStopDelayEl.textContent = "--";
    driverNextStopDelayEl.classList.remove("delay-positive", "delay-negative", "delay-neutral");
  }
  if (driverNextStopInfoEl) {
    driverNextStopInfoEl.textContent = "Sem dados de próxima paragem.";
  }
}

function updateNextStopDelayFromPosition(lat, lng) {
  if (!Array.isArray(activeReferenceStops) || !activeReferenceStops.length || !activeServiceStartedAt) {
    resetNextStopDelayUi();
    return;
  }

  while (activeStopProgressIndex < activeReferenceStops.length) {
    const candidate = activeReferenceStops[activeStopProgressIndex];
    const dist = distanceMeters(lat, lng, Number(candidate.lat), Number(candidate.lng));
    if (Number.isFinite(dist) && dist <= 80) {
      activeStopProgressIndex += 1;
      continue;
    }
    break;
  }
  if (activeStopProgressIndex >= activeReferenceStops.length) {
    if (driverNextStopDelayEl) {
      driverNextStopDelayEl.textContent = "0 min";
      driverNextStopDelayEl.classList.remove("delay-positive", "delay-negative");
      driverNextStopDelayEl.classList.add("delay-neutral");
    }
    if (driverNextStopInfoEl) driverNextStopInfoEl.textContent = "Paragens concluídas para este trajeto.";
    renderStopsTimeline(activeReferenceStops.length);
    return;
  }

  const nextStop = activeReferenceStops[activeStopProgressIndex];
  const scheduledDate = toStopScheduledDate(nextStop, activeServiceStartedAt);
  if (!scheduledDate) {
    resetNextStopDelayUi();
    return;
  }
  const now = new Date();
  const diffMin = Math.round((now.getTime() - scheduledDate.getTime()) / 60000);
  if (driverNextStopDelayEl) {
    driverNextStopDelayEl.classList.remove("delay-positive", "delay-negative", "delay-neutral");
    if (diffMin > 0) {
      driverNextStopDelayEl.textContent = `${diffMin} min`;
      driverNextStopDelayEl.classList.add("delay-positive");
    } else if (diffMin < 0) {
      driverNextStopDelayEl.textContent = `${Math.abs(diffMin)} min adiantado`;
      driverNextStopDelayEl.classList.add("delay-negative");
    } else {
      driverNextStopDelayEl.textContent = "0 min";
      driverNextStopDelayEl.classList.add("delay-neutral");
    }
  }
  if (driverNextStopInfoEl) {
    const planned = scheduledDate.toLocaleTimeString([], { hour: "2-digit", minute: "2-digit" });
    driverNextStopInfoEl.textContent = `Próxima paragem: ${nextStop.stopName || "-"} (${planned})`;
  }
  renderStopsTimeline(activeStopProgressIndex);
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
  await loadDriverMessagePresets();
  await refreshDriverMessages();
  startDriverMessagesRefresh();
  driverAlertBaselineReady = false;
  startDriverAlertsPolling();
  showDriverTab("driverStepSelect");
}

function logoutDriver() {
  const confirmed = window.confirm("Tem a certeza de que deseja terminar a sessão?");
  if (!confirmed) return;

  stopActiveServiceSync();
  stopDriverMessagesRefresh();
  stopDriverAlertsPolling();
  token = "";
  authenticatedUser = null;
  gpsPointQueue = [];
  persistGpsQueue();
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
  if (driverMessagesListEl) {
    driverMessagesListEl.innerHTML = "";
  }
  lastDriverAlertSignature = "";
  driverAlertBaselineReady = false;
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
  restoreGpsQueueForActiveService();
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
  activeServiceStartedAt = active.started_at || null;
  if (driverMapLiveStateEl) {
    driverMapLiveStateEl.textContent = `Serviço ${active.id} | Linha ${active.line_code || "-"} | Frota ${active.fleet_number || "-"}`;
  }
  if (navigator.geolocation) {
    navigator.geolocation.getCurrentPosition(
      (position) => {
        updateActiveBusMarker(position.coords.latitude, position.coords.longitude, active.fleet_number);
      },
      () => {}
    );
  }
  await loadReferenceRoute(active.id);
  await flushGpsPointQueue();
  startLocationTracking();
  startActiveServiceSync();
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
  persistGpsQueue();
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
  activeServiceStartedAt = data.started_at || null;
  if (driverMapLiveStateEl) {
    driverMapLiveStateEl.textContent = `Serviço ${data.id} | Linha ${data.line_code || "-"} | Frota ${data.fleet_number || "-"}`;
  }

  await loadReferenceRoute(data.id);
  startLocationTracking();
  startActiveServiceSync();
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
    activeReferenceStops = [];
    activeStopProgressIndex = 0;
    renderStopsTimeline(0);
    resetNextStopDelayUi();
    return;
  }

  const latLngs = data.points.map((p) => [p.lat, p.lng]);
  referenceRoutePolyline.setLatLngs(latLngs);
  if (routePolyline.getLatLngs().length === 0) {
    map.fitBounds(referenceRoutePolyline.getBounds(), { padding: [20, 20] });
  }

  gtfsStopsLayer.clearLayers();
  activeReferenceStops = Array.isArray(data.stops) ? data.stops : [];
  activeStopProgressIndex = 0;
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
  renderStopsTimeline(activeStopProgressIndex);
}

async function loadReferenceRoutePreview(lineCode, serviceSchedule) {
  const lc = String(lineCode || "").trim();
  const ss = String(serviceSchedule || "").trim();
  if (!lc || !ss || !token) {
    referenceRoutePolyline.setLatLngs([]);
    gtfsStopsLayer.clearLayers();
    return;
  }
  const response = await fetch(
    `${API_BASE}/services/reference-route-preview/by-header?lineCode=${encodeURIComponent(lc)}&serviceSchedule=${encodeURIComponent(ss)}`,
    {
      headers: authHeaders(),
    }
  );
  const data = await response.json().catch(() => ({}));
  if (!response.ok || !data.points?.length) {
    referenceRoutePolyline.setLatLngs([]);
    gtfsStopsLayer.clearLayers();
    return;
  }
  const latLngs = data.points.map((p) => [p.lat, p.lng]);
  referenceRoutePolyline.setLatLngs(latLngs);
  if (routePolyline.getLatLngs().length === 0 && latLngs.length > 1) {
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
      const accuracyM = Number.isFinite(position.coords.accuracy) ? Number(position.coords.accuracy) : null;
      const speedMps = Number(position.coords.speed);
      const headingDegRaw = Number(position.coords.heading);
      const speedKmh = Number.isFinite(speedMps) && speedMps >= 0 ? Number((speedMps * 3.6).toFixed(2)) : null;
      const headingDeg = Number.isFinite(headingDegRaw) ? Number(headingDegRaw.toFixed(2)) : null;

      routePolyline.addLatLng([lat, lng]);
      updateActiveBusMarker(lat, lng, fleetNumberInput.value.trim() || selectedPlannedFleetNumber);
      map.setView([lat, lng], 15);
      updateNextStopDelayFromPosition(lat, lng);

      if (!activeServiceId || !token) return;
      enqueueGpsPoint({
        lat,
        lng,
        capturedAt: new Date(position.timestamp || Date.now()).toISOString(),
        accuracyM,
        speedKmh,
        headingDeg,
        source: "mobile",
      });
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
  todayServicesCache = Array.isArray(data) ? data : [];
  renderMapTodayServicesPanel();

  todayServicesList.innerHTML = "";
  if (!data.length) {
    const li = document.createElement("li");
    li.textContent = "Sem serviços previstos para hoje.";
    todayServicesList.appendChild(li);
    renderMapTodayServicesPanel();
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
      activeServiceStartedAt = resumeData.service.started_at || null;
      if (driverMapLiveStateEl) {
        driverMapLiveStateEl.textContent = `Serviço ${resumeData.service.id} | Linha ${resumeData.service.line_code || "-"} | Frota ${
          resumeData.service.fleet_number || "-"
        }`;
      }
      await loadReferenceRoute(item.service_id);
      startLocationTracking();
      startActiveServiceSync();
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
if (driverMessageFormEl) driverMessageFormEl.addEventListener("submit", sendDriverMessage);
if (refreshDriverMessagesBtnEl) refreshDriverMessagesBtnEl.addEventListener("click", refreshDriverMessages);
if (driverAlertSoundTypeEl) {
  driverAlertSoundTypeEl.addEventListener("change", () => saveDriverAlertSoundSettings(getDriverAlertSoundSettings()));
}
if (driverAlertSoundVolumeEl) {
  driverAlertSoundVolumeEl.addEventListener("input", () => saveDriverAlertSoundSettings(getDriverAlertSoundSettings()));
}
if (testDriverAlertSoundBtnEl) {
  testDriverAlertSoundBtnEl.addEventListener("click", playDriverAlertSound);
}
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
window.addEventListener("online", () => {
  flushGpsPointQueue();
  updateDriverAlertSignature();
});

applyDriverAlertSoundSettingsToUi();

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
