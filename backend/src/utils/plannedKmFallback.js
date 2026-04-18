const db = require("../db");

const GPS_KM_FLOOR = 0.01;

/** Realizado <= 5% do planeado equivale a faltar >= 95% dos km de escala → usar kms_carga. */
const REALIZED_VS_PLANNED_MAX_RATIO = 0.05;

/**
 * Define total_km do serviço: GPS quando fiável; caso contrário kms_carga da escala (planned_services).
 *
 * Usa kms_carga quando:
 * - GPS não produz quilometragem útil (< limiar ou inválido), ou
 * - realizados <= 5% dos programados (faltam >= 95% dos km de escala face ao planeado).
 */
async function resolveTotalKmWithPlannedFallback(calculatedKm, plannedServiceId) {
  const n = Number(calculatedKm);
  const gpsKm = Number.isFinite(n) && n >= 0 ? n : null;

  let plannedKm = null;
  if (plannedServiceId) {
    const { rows } = await db.query(`SELECT kms_carga FROM planned_services WHERE id = $1`, [plannedServiceId]);
    const raw = rows[0]?.kms_carga;
    if (raw != null && Number(raw) > 0) plannedKm = Number(raw);
  }

  if (plannedKm == null) {
    return gpsKm != null ? gpsKm : 0;
  }

  const gpsWeak = gpsKm == null || gpsKm < GPS_KM_FLOOR;
  const farBelowPlanned = gpsKm != null && gpsKm <= REALIZED_VS_PLANNED_MAX_RATIO * plannedKm;

  if (gpsWeak || farBelowPlanned) return plannedKm;

  return gpsKm;
}

module.exports = {
  resolveTotalKmWithPlannedFallback,
  GPS_KM_FLOOR,
  REALIZED_VS_PLANNED_MAX_RATIO,
};
