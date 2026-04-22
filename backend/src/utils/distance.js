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

function getPointTimestampMs(point) {
  if (!point || !point.capturedAt) return null;
  const timeMs = Date.parse(point.capturedAt);
  return Number.isFinite(timeMs) ? timeMs : null;
}

function isPointAccuracyAcceptable(point, maxAccuracyMeters) {
  const accuracy = Number(point?.accuracyM);
  if (!Number.isFinite(accuracy)) return true;
  return accuracy <= maxAccuracyMeters;
}

function isSegmentPlausible(prev, curr, distanceKm, maxSpeedKmh) {
  const prevTs = getPointTimestampMs(prev);
  const currTs = getPointTimestampMs(curr);
  if (!Number.isFinite(prevTs) || !Number.isFinite(currTs) || currTs <= prevTs) {
    return true;
  }

  const elapsedHours = (currTs - prevTs) / 3600000;
  if (elapsedHours <= 0) return true;
  const computedSpeed = distanceKm / elapsedHours;
  if (computedSpeed > maxSpeedKmh) return false;

  const prevReportedSpeed = Number(prev?.speedKmh);
  const currReportedSpeed = Number(curr?.speedKmh);
  const speedCapWithTolerance = maxSpeedKmh * 1.2;
  if (Number.isFinite(prevReportedSpeed) && prevReportedSpeed > speedCapWithTolerance) return false;
  if (Number.isFinite(currReportedSpeed) && currReportedSpeed > speedCapWithTolerance) return false;

  return true;
}

function calculatePathDistance(points, options = {}) {
  if (!points || points.length < 2) return 0;
  const maxAccuracyMeters = Number(options.maxAccuracyMeters ?? 35);
  const maxSpeedKmh = Number(options.maxSpeedKmh ?? 120);

  let total = 0;
  for (let i = 1; i < points.length; i += 1) {
    const prev = points[i - 1];
    const curr = points[i];
    if (!isPointAccuracyAcceptable(prev, maxAccuracyMeters)) continue;
    if (!isPointAccuracyAcceptable(curr, maxAccuracyMeters)) continue;
    const segmentKm = haversineKm(prev.lat, prev.lng, curr.lat, curr.lng);
    if (!isSegmentPlausible(prev, curr, segmentKm, maxSpeedKmh)) continue;
    total += segmentKm;
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
