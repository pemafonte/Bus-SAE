/**
 * Resumo operacional apenas para o dia civil actual em Europe/Lisbon.
 * - Concluídos: contam pelo dia de ended_at (quando a viagem terminou), não por started_at.
 * - Em curso / transferência: pelo dia de started_at.
 * - Serviços na app (svc): só contam se existir linha em daily_roster nesse dia para o mesmo
 *   planned_service_id e driver_id — evita mostrar concluídos quando não há escala importada.
 * - Agregação em svc usa DISTINCT ON (services.id) para não duplicar km quando o JOIN com a escala
 *   poderia repetir a mesma viagem.
 * - Média histórica (hist_avg) limitada aos concluídos dos últimos 365 dias.
 */
const OVERVIEW_TODAY_SQL = `
WITH day AS (
  SELECT ((NOW() AT TIME ZONE 'Europe/Lisbon'))::date AS d
),
planned AS (
  SELECT COUNT(*)::int AS planned_roster_count
  FROM daily_roster dr
  JOIN planned_services ps ON ps.id = dr.planned_service_id
  CROSS JOIN day
  WHERE dr.service_date = day.d
    AND COALESCE(ps.kms_carga, 0) > 0
),
realized_slots AS (
  SELECT COUNT(DISTINCT dr.id)::int AS realized_roster_slots
  FROM daily_roster dr
  JOIN planned_services ps ON ps.id = dr.planned_service_id
  JOIN services s ON s.planned_service_id = dr.planned_service_id
    AND s.driver_id = dr.driver_id
  CROSS JOIN day
  WHERE dr.service_date = day.d
    AND COALESCE(ps.kms_carga, 0) > 0
    AND LOWER(TRIM(s.status::text)) = 'completed'
    AND (
      (s.ended_at IS NOT NULL AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d)
      OR (
        s.ended_at IS NULL
        AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d
      )
    )
),
svc AS (
  SELECT
    COUNT(*)::int AS total_services,
    COUNT(*) FILTER (WHERE LOWER(TRIM(s.status::text)) = 'completed')::int AS completed_services,
    COUNT(*) FILTER (WHERE LOWER(TRIM(s.status::text)) IN ('in_progress', 'awaiting_handover'))::int AS in_progress_services,
    COALESCE(SUM(s.total_km) FILTER (WHERE LOWER(TRIM(s.status::text)) = 'completed'), 0)::numeric(12,3) AS km_realized_today,
    COALESCE(
      AVG(s.total_km) FILTER (WHERE LOWER(TRIM(s.status::text)) = 'completed' AND s.total_km IS NOT NULL AND s.total_km > 0),
      0
    )::numeric(12,3) AS avg_km_completed_today
  FROM (
    SELECT DISTINCT ON (s.id)
      s.id,
      s.status,
      s.total_km,
      s.started_at,
      s.ended_at
    FROM services s
    CROSS JOIN day
    INNER JOIN daily_roster dr
      ON dr.planned_service_id = s.planned_service_id
     AND dr.driver_id = s.driver_id
     AND dr.service_date = day.d
    INNER JOIN planned_services ps
      ON ps.id = dr.planned_service_id
     AND COALESCE(ps.kms_carga, 0) > 0
    WHERE (
      (
        LOWER(TRIM(s.status::text)) = 'completed'
        AND (
          (s.ended_at IS NOT NULL AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d)
          OR (
            s.ended_at IS NULL
            AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d
          )
        )
      )
      OR (
        LOWER(TRIM(s.status::text)) IN ('in_progress', 'awaiting_handover')
        AND ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d
      )
    )
    ORDER BY s.id
  ) s
),
hist_avg AS (
  SELECT COALESCE(AVG(total_km), 0)::numeric(12,3) AS avg_completed_historical
  FROM services
  WHERE LOWER(TRIM(status::text)) = 'completed'
    AND total_km IS NOT NULL
    AND total_km > 0
    AND COALESCE(ended_at, started_at) >= (NOW() - INTERVAL '365 days')
),
deadhead_today AS (
  SELECT
    COALESCE(SUM(total_km), 0)::numeric(12,3) AS deadhead_km
  FROM deadhead_movements dm
  CROSS JOIN day
  WHERE ((dm.ended_at AT TIME ZONE 'Europe/Lisbon')::date) = day.d
)
SELECT
  day.d::text AS report_date,
  svc.total_services,
  svc.completed_services,
  svc.in_progress_services,
  svc.km_realized_today AS total_km,
  CASE
    WHEN svc.completed_services > 0 THEN svc.avg_km_completed_today
    ELSE 0::numeric(12,3)
  END AS avg_km,
  planned.planned_roster_count,
  realized_slots.realized_roster_slots,
  GREATEST(0, planned.planned_roster_count - realized_slots.realized_roster_slots)::int AS not_realized_count,
  deadhead_today.deadhead_km,
  (svc.km_realized_today + deadhead_today.deadhead_km)::numeric(12,3) AS total_km_with_deadhead,
  (planned.planned_roster_count * hist_avg.avg_completed_historical)::numeric(12,3) AS estimated_planned_km_today,
  GREATEST(
    0,
    (planned.planned_roster_count * hist_avg.avg_completed_historical) - svc.km_realized_today
  )::numeric(12,3) AS km_not_realized_estimate
FROM day
CROSS JOIN planned
CROSS JOIN realized_slots
CROSS JOIN svc
CROSS JOIN deadhead_today
CROSS JOIN hist_avg
`;

module.exports = { OVERVIEW_TODAY_SQL };
