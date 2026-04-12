const db = require("../db");

async function archiveClosedRosterDays() {
  await db.query(
    `INSERT INTO daily_roster_history (
       roster_id, driver_id, planned_service_id, service_date, status, created_at, archived_at
     )
     SELECT
       dr.id, dr.driver_id, dr.planned_service_id, dr.service_date, dr.status, dr.created_at, NOW()
     FROM daily_roster dr
     WHERE dr.service_date < CURRENT_DATE
       AND NOT EXISTS (
         SELECT 1
         FROM daily_roster_history h
         WHERE h.roster_id = dr.id
       )`
  );

  await db.query(
    `DELETE FROM daily_roster
     WHERE service_date < CURRENT_DATE`
  );
}

module.exports = { archiveClosedRosterDays };
