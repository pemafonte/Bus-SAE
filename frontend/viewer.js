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

function todayISOInLisbon() {
  return new Intl.DateTimeFormat("en-CA", { timeZone: "Europe/Lisbon" }).format(new Date());
}

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

function labelPerfilResumidoPt(role) {
  const k = String(role || "").toLowerCase();
  const map = {
    viewer: "visualização",
    viewr: "visualização",
    supervisor: "supervisor",
    admin: "administrador",
  };
  return map[k] || role || "—";
}
let viewerToken = "";
let activeQueryString = "";
const AUTH_SESSION_KEY = "auth_session";

const overviewEl = document.getElementById("viewerOverview");
const servicesCardsEl = document.getElementById("viewerServiceCards");
const departuresListEl = document.getElementById("viewerDeparturesList");
const servicesPieEl = document.getElementById("viewerServicesPie");
const servicesStatsTextEl = document.getElementById("viewerServicesStatsText");
const blocksPieEl = document.getElementById("viewerBlocksPie");
const blocksStatsTextEl = document.getElementById("viewerBlocksStatsText");
const sessionWelcomeEl = document.getElementById("viewerSessionWelcome");
const logoutBtnEl = document.getElementById("viewerLogoutBtn");
const loginCardEl = document.getElementById("viewerLoginCard");
const appSectionsEl = document.getElementById("viewerAppSections");

function authHeaders() {
  return {
    "Content-Type": "application/json",
    Authorization: `Bearer ${viewerToken}`,
  };
}

function applyViewerSession(user) {
  sessionWelcomeEl.textContent = `Bem-vindo ${user.username || user.name || "utilizador"}`;
  sessionWelcomeEl.classList.remove("hidden");
  logoutBtnEl.classList.remove("hidden");
  loginCardEl.classList.add("hidden");
  appSectionsEl?.classList.remove("hidden");
}

function readUserFromJwt(token) {
  try {
    const payloadPart = String(token || "").split(".")[1];
    if (!payloadPart) return null;
    const normalizedBase64 = payloadPart.replace(/-/g, "+").replace(/_/g, "/");
    const json = atob(normalizedBase64);
    const payload = JSON.parse(json);
    return {
      username: payload?.username || "",
      name: payload?.name || "",
      role: String(payload?.role || "").trim().toLowerCase(),
    };
  } catch (_error) {
    return null;
  }
}

function formatTime(value) {
  if (!value) return "-";
  return new Date(value).toLocaleTimeString();
}

function formatDateOnly(value) {
  if (!value) return "-";
  return new Date(value).toLocaleDateString();
}

function buildPieBackground(parts) {
  const total = parts.reduce((sum, p) => sum + p.value, 0) || 1;
  let start = 0;
  const slices = parts.map((p) => {
    const portion = (p.value / total) * 100;
    const end = start + portion;
    const seg = `${p.color} ${start}% ${end}%`;
    start = end;
    return seg;
  });
  return `conic-gradient(${slices.join(", ")})`;
}

function renderReadOnlyServices(services) {
  servicesCardsEl.innerHTML = "";
  if (!services.length) {
    servicesCardsEl.innerHTML = "<div>Sem serviços para os filtros selecionados.</div>";
    return;
  }

  services.forEach((s) => {
    const executionDate = s.started_at ? formatDateOnly(s.started_at) : "-";
    const startedAt = s.started_at ? formatTime(s.started_at) : "-";
    const endedAt = s.ended_at ? formatTime(s.ended_at) : "-";
    const card = document.createElement("article");
    card.className = "service-card-item";
    card.innerHTML = `
      <div class="service-card-head">
        <strong>Serviço n.º ${s.id}</strong>
        <span class="status-badge status-${s.status || "other"}">${labelEstadoExecucaoServicoPt(s.status)}</span>
      </div>
      <div class="service-card-grid">
        <div><small>Motorista</small><div>${s.driver_name || "-"}</div></div>
        <div><small>Linha</small><div>${s.line_code || "-"}</div></div>
        <div><small>Data de execução</small><div>${executionDate}</div></div>
        <div><small>Saída</small><div>${startedAt}</div></div>
        <div><small>Chegada</small><div>${endedAt}</div></div>
        <div><small>Frota</small><div>${s.fleet_number || "-"}</div></div>
        <div><small>Chapa</small><div>${s.plate_number || "-"}</div></div>
        <div><small>Quilómetros</small><div>${s.total_km || 0}</div></div>
      </div>
    `;
    servicesCardsEl.appendChild(card);
  });
}

function renderChartsAndDepartures(services) {
  const completed = services.filter((s) => s.status === "completed").length;
  const inProgress = services.filter((s) => s.status === "in_progress").length;
  const waiting = services.filter((s) => s.status === "awaiting_handover").length;
  const total = services.length;
  const executed = completed;
  const remaining = Math.max(total - executed, 0);

  servicesPieEl.style.background = buildPieBackground([
    { value: completed, color: "#16a34a" },
    { value: inProgress, color: "#ea580c" },
    { value: waiting, color: "#7c3aed" },
  ]);
  servicesStatsTextEl.textContent =
    `Concluídos: ${completed} | Em curso: ${inProgress} | Aguardam transferência: ${waiting}`;

  blocksPieEl.style.background = buildPieBackground([
    { value: executed, color: "#2563eb" },
    { value: remaining, color: "#f59e0b" },
  ]);
  blocksStatsTextEl.textContent = `Total: ${total} | Realizados: ${executed} | Por realizar: ${remaining}`;

  departuresListEl.innerHTML = "";
  services
    .filter((s) => s.started_at)
    .slice(0, 10)
    .forEach((s) => {
      const li = document.createElement("li");
      li.textContent = `${formatDateOnly(s.started_at)} ${formatTime(s.started_at)} - Linha ${s.line_code || "-"} - ${s.driver_name || "-"}`;
      departuresListEl.appendChild(li);
    });
  if (!departuresListEl.innerHTML) {
    departuresListEl.innerHTML = "<li>Sem saídas para o período selecionado.</li>";
  }
}

function buildQueryString() {
  const params = new URLSearchParams();
  const driverId = document.getElementById("vfDriverId").value.trim();
  const lineCode = document.getElementById("vfLineCode").value.trim();
  const status = document.getElementById("vfStatus").value.trim();
  const fromDate = document.getElementById("vfFromDate").value;
  const toDate = document.getElementById("vfToDate").value;

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
  const query = params.toString();
  return query ? `?${query}` : "";
}

function clearViewerFilters() {
  const fields = ["vfDriverId", "vfLineCode", "vfStatus"];
  fields.forEach((id) => {
    const el = document.getElementById(id);
    if (el) el.value = "";
  });
  const fromInput = document.getElementById("vfFromDate");
  const toInput = document.getElementById("vfToDate");
  const today = todayISOInLisbon();
  if (fromInput) fromInput.value = today;
  if (toInput) toInput.value = today;
  loadServices();
}

async function loginViewer(event) {
  event.preventDefault();
  const username = document.getElementById("viewerUsername").value.trim();
  const password = document.getElementById("viewerPassword").value;

  let response;
  try {
    response = await fetch(`${API_BASE}/auth/login`, {
      method: "POST",
      headers: { "Content-Type": "application/json" },
      body: JSON.stringify({ username, password }),
    });
  } catch (_e) {
    alert("Não foi possível ligar ao servidor. Verifique se a API está ativa.");
    return;
  }
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro no início de sessão.");
    return;
  }
  const role = String(data.user.role || "").trim().toLowerCase() || readUserFromJwt(data.token)?.role;
  if (!["viewer", "viewr", "supervisor", "admin"].includes(role)) {
    alert("Este utilizador não tem permissão de visualização.");
    return;
  }

  viewerToken = data.token;
  const sessionUser = data.user || readUserFromJwt(data.token) || { username, role };
  sessionStorage.setItem(AUTH_SESSION_KEY, JSON.stringify({ token: data.token, user: data.user }));
  alert(`Sessão iniciada para ${sessionUser.username || sessionUser.name || username} (${labelPerfilResumidoPt(role)}).`);
  applyViewerSession(sessionUser);
  await loadOverview();
  await loadServices();
}

function logoutViewer() {
  const confirmed = window.confirm("Tem a certeza de que deseja terminar a sessão?");
  if (!confirmed) return;

  viewerToken = "";
  activeQueryString = "";
  sessionWelcomeEl.classList.add("hidden");
  logoutBtnEl.classList.add("hidden");
  loginCardEl.classList.remove("hidden");
  appSectionsEl?.classList.add("hidden");
  overviewEl.textContent = "Sem dados.";
  servicesCardsEl.innerHTML = "";
  departuresListEl.innerHTML = "";
  servicesStatsTextEl.textContent = "Sem dados.";
  blocksStatsTextEl.textContent = "Sem dados.";
  document.getElementById("viewerLoginForm").reset();
  sessionStorage.removeItem(AUTH_SESSION_KEY);
}

async function loadOverview() {
  if (!viewerToken) return;
  const response = await fetch(`${API_BASE}/viewer/overview`, { headers: authHeaders() });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao carregar o resumo.");
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
  if (!viewerToken) return;

  activeQueryString = buildQueryString();
  const response = await fetch(`${API_BASE}/viewer/services${activeQueryString}`, {
    headers: authHeaders(),
  });
  const data = await response.json();
  if (!response.ok) {
    alert(data.message || "Erro ao carregar os serviços.");
    return;
  }

  renderReadOnlyServices(data);
  renderChartsAndDepartures(data);
}

async function exportCsv() {
  if (!viewerToken) return;
  if (!activeQueryString) activeQueryString = buildQueryString();

  const response = await fetch(`${API_BASE}/viewer/services/export.csv${activeQueryString}`, {
    headers: { Authorization: `Bearer ${viewerToken}` },
  });
  if (!response.ok) {
    const data = await response.json().catch(() => ({}));
    alert(data.message || "Erro ao exportar o ficheiro CSV.");
    return;
  }

  const csvText = await response.text();
  const blob = new Blob([csvText], { type: "text/csv;charset=utf-8;" });
  const url = URL.createObjectURL(blob);
  const a = document.createElement("a");
  a.href = url;
  a.download = "resumo_servicos.csv";
  document.body.appendChild(a);
  a.click();
  a.remove();
  URL.revokeObjectURL(url);
}

document.getElementById("viewerLoginForm").addEventListener("submit", loginViewer);
document.getElementById("viewerFiltersForm").addEventListener("submit", loadServices);
document.getElementById("viewerClearFiltersBtn").addEventListener("click", clearViewerFilters);
document.getElementById("viewerExportBtn").addEventListener("click", exportCsv);
document.getElementById("viewerLogoutBtn").addEventListener("click", logoutViewer);

(() => {
  const fromInput = document.getElementById("vfFromDate");
  const toInput = document.getElementById("vfToDate");
  if (fromInput && toInput && !fromInput.value && !toInput.value) {
    const today = new Date().toISOString().slice(0, 10);
    fromInput.value = today;
    toInput.value = today;
  }

  const raw = sessionStorage.getItem(AUTH_SESSION_KEY);
  if (!raw) return;
  try {
    const session = JSON.parse(raw);
    if (!session?.token) return;
    const jwtUser = readUserFromJwt(session.token);
    const user = session?.user || jwtUser;
    if (!user) return;
    const role = String(user.role || "").trim().toLowerCase() || String(jwtUser?.role || "").trim().toLowerCase();
    if (!["viewer", "viewr", "supervisor", "admin"].includes(role)) return;
    viewerToken = session.token;
    applyViewerSession(user);
    loadOverview();
    loadServices();
  } catch (_error) {
    // ignore invalid persisted session
  }
})();
