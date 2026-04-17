const db = require("../db");

/**
 * Data do dia de escala (daily_roster.service_date) para o serviço planeado em execução.
 * Tenta primeiro o registo cuja data coincide com o dia civil de início em Lisboa;
 * se não existir, usa o escalado mais recente para esse motorista/serviço planeado.
 */
async function getRosterServiceDateForExecution(driverId, plannedServiceId, serviceStartedAt) {
  if (!plannedServiceId || !driverId) return null;
  const exact = await db.query(
    `SELECT dr.service_date
     FROM daily_roster dr
     WHERE dr.driver_id = $1
       AND dr.planned_service_id = $2
       AND dr.service_date = ($3::timestamptz AT TIME ZONE 'Europe/Lisbon')::date`,
    [driverId, plannedServiceId, serviceStartedAt]
  );
  if (exact.rowCount) return exact.rows[0].service_date;
  const fallback = await db.query(
    `SELECT dr.service_date
     FROM daily_roster dr
     WHERE dr.driver_id = $1 AND dr.planned_service_id = $2
     ORDER BY dr.service_date DESC
     LIMIT 1`,
    [driverId, plannedServiceId]
  );
  return fallback.rows[0]?.service_date || null;
}

module.exports = { getRosterServiceDateForExecution };
