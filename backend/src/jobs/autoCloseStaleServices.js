async function getAutoCloseDeadlineFromEscalacaoTerminus(row) {
  // Deprecated by GPS+GTFS auto-close at last stop.
  void row;
  return null;
}

async function autoCloseOneService(row) {
  // Deprecated: services now auto-close when GPS reaches the last GTFS stop.
  // Manual close remains available as fallback.
  void row;
  return true;
}

async function runAutoCloseStaleServicesOnce() {
  return 0;
}

function startAutoCloseStaleServicesLoop(intervalMs = 60_000) {
  if (String(process.env.DISABLE_AUTO_CLOSE_STALE_SERVICES || "").trim() === "1") {
    console.log("[autoCloseStaleServices] desativado (DISABLE_AUTO_CLOSE_STALE_SERVICES=1).");
    return () => {};
  }
  const ms = Number(process.env.AUTO_CLOSE_STALE_SERVICES_INTERVAL_MS || intervalMs);
  const tick = () => {
    runAutoCloseStaleServicesOnce().catch((e) => {
      console.error("[autoCloseStaleServices] erro no ciclo.", e);
    });
  };
  tick();
  const id = setInterval(tick, Number.isFinite(ms) && ms >= 10_000 ? ms : intervalMs);
  return () => clearInterval(id);
}

module.exports = {
  runAutoCloseStaleServicesOnce,
  startAutoCloseStaleServicesLoop,
  getAutoCloseDeadlineFromEscalacaoTerminus,
};
