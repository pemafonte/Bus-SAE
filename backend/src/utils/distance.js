function toRadians(value) {
  return (value * Math.PI) / 180;
}

function haversineKm(lat1, lon1, lat2, lon2) {
  const R = 6371;
  const dLat = toRadians(lat2 - lat1);
  const dLon = toRadians(lon2 - lon1);

  const a =
    Math.sin(dLat / 2) ** 2 +
    Math.cos(toRadians(lat1)) *
      Math.cos(toRadians(lat2)) *
      Math.sin(dLon / 2) ** 2;

  const c = 2 * Math.atan2(Math.sqrt(a), Math.sqrt(1 - a));
  return R * c;
}

function calculatePathDistance(points) {
  if (!points || points.length < 2) return 0;

  let total = 0;
  for (let i = 1; i < points.length; i += 1) {
    const prev = points[i - 1];
    const curr = points[i];
    total += haversineKm(prev.lat, prev.lng, curr.lat, curr.lng);
  }
  return Number(total.toFixed(3));
}

function pointToSegmentDistanceMeters(point, a, b) {
  const meanLat = toRadians((a.lat + b.lat + point.lat) / 3);
  const metersPerDegLat = 111320;
  const metersPerDegLon = 111320 * Math.cos(meanLat);

  const ax = a.lng * metersPerDegLon;
  const ay = a.lat * metersPerDegLat;
  const bx = b.lng * metersPerDegLon;
  const by = b.lat * metersPerDegLat;
  const px = point.lng * metersPerDegLon;
  const py = point.lat * metersPerDegLat;

  const abx = bx - ax;
  const aby = by - ay;
  const abLenSq = abx * abx + aby * aby;
  if (abLenSq === 0) {
    const dx = px - ax;
    const dy = py - ay;
    return Math.sqrt(dx * dx + dy * dy);
  }

  let t = ((px - ax) * abx + (py - ay) * aby) / abLenSq;
  t = Math.max(0, Math.min(1, t));
  const cx = ax + t * abx;
  const cy = ay + t * aby;
  const dx = px - cx;
  const dy = py - cy;
  return Math.sqrt(dx * dx + dy * dy);
}

function minDistanceToPolylineMeters(point, polylinePoints) {
  if (!polylinePoints || polylinePoints.length === 0) return Number.POSITIVE_INFINITY;
  if (polylinePoints.length === 1) {
    return Number(
      (haversineKm(point.lat, point.lng, polylinePoints[0].lat, polylinePoints[0].lng) * 1000).toFixed(2)
    );
  }

  let minMeters = Number.POSITIVE_INFINITY;
  for (let i = 1; i < polylinePoints.length; i += 1) {
    const meters = pointToSegmentDistanceMeters(point, polylinePoints[i - 1], polylinePoints[i]);
    if (meters < minMeters) minMeters = meters;
  }
  return Number(minMeters.toFixed(2));
}

function haversineMeters(lat1, lon1, lat2, lon2) {
  return Number((haversineKm(lat1, lon1, lat2, lon2) * 1000).toFixed(2));
}

module.exports = { calculatePathDistance, minDistanceToPolylineMeters, haversineMeters };
