const { parseServiceScheduleStartMinutes } = require("./gtfsTripResolve");

/** Intervalo [start,end) em minutos desde meia-noite; suporta janelas que atravessam meia-noite. */
function parseServiceScheduleRangeMinutes(serviceSchedule) {
  const text = String(serviceSchedule || "").trim();
  if (!text) return null;
  const rangeMatch = text.match(/(\d{1,2})\s*:\s*(\d{2})\s*-\s*(\d{1,2})\s*:\s*(\d{2})/);
  if (rangeMatch) {
    let start = Number(rangeMatch[1]) * 60 + Number(rangeMatch[2]);
    let end = Number(rangeMatch[3]) * 60 + Number(rangeMatch[4]);
    if (Number.isNaN(start) || Number.isNaN(end)) return null;
    if (end <= start) end += 24 * 60;
    return { start, end };
  }
  const start = parseServiceScheduleStartMinutes(text);
  if (start == null) return null;
  return { start, end: start + 4 * 60 };
}

function scheduleRangesOverlap(a, b) {
  if (!a || !b) return false;
  return a.start < b.end && b.start < a.end;
}

module.exports = {
  parseServiceScheduleRangeMinutes,
  scheduleRangesOverlap,
};
