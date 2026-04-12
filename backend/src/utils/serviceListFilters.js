/**
 * Filtro por dia civil em Europe/Lisbon: inclui serviços cujo início OU fim (se existir)
 * calha no intervalo [from, to]. Evita usar timestamps UTC soltos em started_at.
 */
function normalizeDateParam(s) {
  if (s == null || s === "") return null;
  const t = String(s).trim();
  return /^\d{4}-\d{2}-\d{2}$/.test(t) ? t : null;
}

/**
 * @param {string|null|undefined} fromDate
 * @param {string|null|undefined} toDate
 * @param {number} startParamIndex — primeiro placeholder ($n) para os valores de data
 * @returns {{ sql: string, values: string[], nextIndex: number } | null}
 */
function serviceActivityLisbonDayFilter(fromDate, toDate, startParamIndex) {
  const from = normalizeDateParam(fromDate);
  const to = normalizeDateParam(toDate);
  let p = startParamIndex;

  if (from && to) {
    const a = p;
    const b = p + 1;
    return {
      sql: `(
        ((s.started_at AT TIME ZONE 'Europe/Lisbon')::date BETWEEN $${a}::date AND $${b}::date)
        OR (
          s.ended_at IS NOT NULL
          AND ((s.ended_at AT TIME ZONE 'Europe/Lisbon')::date BETWEEN $${a}::date AND $${b}::date)
        )
      )`,
      values: [from, to],
      nextIndex: p + 2,
    };
  }
  if (from) {
    return {
      sql: `((s.started_at AT TIME ZONE 'Europe/Lisbon')::date >= $${p}::date)`,
      values: [from],
      nextIndex: p + 1,
    };
  }
  if (to) {
    return {
      sql: `((s.started_at AT TIME ZONE 'Europe/Lisbon')::date <= $${p}::date)`,
      values: [to],
      nextIndex: p + 1,
    };
  }
  return null;
}

module.exports = { normalizeDateParam, serviceActivityLisbonDayFilter };
