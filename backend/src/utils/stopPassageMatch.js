const { haversineMeters } = require("./distance");

/**
 * Associa pontos GPS a paragens GTFS em ordem: após cada paragem, só contam passagens
 * nas paragens seguintes com instante >= ao da paragem anterior (sentido do serviço).
 */
function matchGpsPointsToGtfsStops(stops, points, options = {}) {
  const radiusM = options.radiusMeters ?? 85;
  const serviceStartedAt = options.serviceStartedAt ? new Date(options.serviceStartedAt) : null;

  const sortedPoints = [...points]
    .filter((p) => p.captured_at != null && Number.isFinite(Number(p.lat)) && Number.isFinite(Number(p.lng)))
    .sort((a, b) => new Date(a.captured_at) - new Date(b.captured_at));

  let lastPassTime = serviceStartedAt;
  const stopsOrdered = [...stops].sort((a, b) => (a.sequence || 0) - (b.sequence || 0));
  const rows = [];

  for (const stop of stopsOrdered) {
    const near = sortedPoints.filter((p) => {
      const d = haversineMeters(Number(p.lat), Number(p.lng), stop.lat, stop.lng);
      return d <= radiusM;
    });

    let candidates = near;
    if (lastPassTime) {
      candidates = near.filter((p) => new Date(p.captured_at) >= lastPassTime);
    }

    const chosen = candidates.length ? candidates[0] : null;
    const passedAt = chosen ? new Date(chosen.captured_at).toISOString() : null;
    if (chosen) {
      lastPassTime = new Date(chosen.captured_at);
    }

    const distM = chosen
      ? haversineMeters(Number(chosen.lat), Number(chosen.lng), stop.lat, stop.lng)
      : null;

    rows.push({
      stop_sequence: stop.sequence,
      stop_id: stop.stopId,
      stop_name: stop.stopName,
      stop_lat: stop.lat,
      stop_lng: stop.lng,
      scheduled_arrival: stop.arrivalTime,
      scheduled_departure: stop.departureTime,
      passed_at: passedAt,
      distance_from_stop_m: distM,
      passed_near_stop: Boolean(chosen),
    });
  }

  const matched = rows.filter((r) => r.passed_near_stop).length;
  return { rows, matched, total: rows.length };
}

module.exports = { matchGpsPointsToGtfsStops };
