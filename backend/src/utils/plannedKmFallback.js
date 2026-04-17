const db = require("../db");

const GPS_KM_FLOOR = 0.01;

/**
 * Se o GPS não produz quilometragem útil, usa kms_carga do serviço planeado (quando existir).
 */
async function resolveTotalKmWithPlannedFallback(calculatedKm, plannedServiceId) {
  const n = Number(calculatedKm);
  const gpsWeak = !Number.isFinite(n) || n < GPS_KM_FLOOR;
  if (!gpsWeak) return n;
  if (!plannedServiceId) return Number.isFinite(n) && n >= 0 ? n : 0;
  const { rows } = await db.query(`SELECT kms_carga FROM planned_services WHERE id = $1`, [plannedServiceId]);
  const planned = rows[0]?.kms_carga;
  if (planned != null && Number(planned) > 0) return Number(planned);
  return Number.isFinite(n) && n >= 0 ? n : 0;
}

module.exports = { resolveTotalKmWithPlannedFallback, GPS_KM_FLOOR };
