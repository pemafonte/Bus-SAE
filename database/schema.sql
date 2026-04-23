CREATE TABLE IF NOT EXISTS users (
  id SERIAL PRIMARY KEY,
  name VARCHAR(120) NOT NULL,
  username VARCHAR(80) UNIQUE,
  email VARCHAR(160) UNIQUE,
  mechanic_number VARCHAR(50) UNIQUE,
  company_name VARCHAR(120),
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  role VARCHAR(20) NOT NULL DEFAULT 'driver',
  password_hash TEXT NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE users
  ADD COLUMN IF NOT EXISTS username VARCHAR(80);
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS company_name VARCHAR(120);
ALTER TABLE users
  ADD COLUMN IF NOT EXISTS is_active BOOLEAN NOT NULL DEFAULT TRUE;
ALTER TABLE users
  ALTER COLUMN email DROP NOT NULL;

CREATE UNIQUE INDEX IF NOT EXISTS idx_users_username_unique
  ON users(username);

CREATE TABLE IF NOT EXISTS planned_services (
  id SERIAL PRIMARY KEY,
  service_code VARCHAR(50) UNIQUE NOT NULL,
  line_code VARCHAR(50) NOT NULL,
  start_location VARCHAR(120),
  end_location VARCHAR(120),
  fleet_number VARCHAR(50) NOT NULL,
  plate_number VARCHAR(50) NOT NULL,
  service_schedule VARCHAR(80) NOT NULL,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE planned_services
  ADD COLUMN IF NOT EXISTS start_location VARCHAR(120);
ALTER TABLE planned_services
  ADD COLUMN IF NOT EXISTS end_location VARCHAR(120);

CREATE TABLE IF NOT EXISTS daily_roster (
  id SERIAL PRIMARY KEY,
  driver_id INT NOT NULL REFERENCES users(id),
  planned_service_id INT NOT NULL REFERENCES planned_services(id),
  service_date DATE NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'assigned',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  UNIQUE(driver_id, planned_service_id, service_date)
);

CREATE TABLE IF NOT EXISTS daily_roster_history (
  id BIGSERIAL PRIMARY KEY,
  roster_id INT,
  driver_id INT NOT NULL REFERENCES users(id),
  planned_service_id INT NOT NULL REFERENCES planned_services(id),
  service_date DATE NOT NULL,
  status VARCHAR(20) NOT NULL,
  created_at TIMESTAMPTZ,
  archived_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS services (
  id SERIAL PRIMARY KEY,
  driver_id INT NOT NULL REFERENCES users(id),
  planned_service_id INT REFERENCES planned_services(id),
  gtfs_trip_id VARCHAR(120),
  plate_number VARCHAR(50) NOT NULL,
  service_schedule VARCHAR(80) NOT NULL,
  line_code VARCHAR(50) NOT NULL,
  fleet_number VARCHAR(50) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'in_progress',
  started_at TIMESTAMPTZ DEFAULT NOW(),
  ended_at TIMESTAMPTZ,
  total_km NUMERIC(10,3) DEFAULT 0,
  route_deviation_m NUMERIC(10,2) DEFAULT 0,
  is_off_route BOOLEAN NOT NULL DEFAULT FALSE,
  route_geojson JSONB,
  created_at TIMESTAMPTZ DEFAULT NOW()
);

ALTER TABLE services
  ADD COLUMN IF NOT EXISTS gtfs_trip_id VARCHAR(120);
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS route_deviation_m NUMERIC(10,2) DEFAULT 0;
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS is_off_route BOOLEAN NOT NULL DEFAULT FALSE;

CREATE TABLE IF NOT EXISTS service_points (
  id BIGSERIAL PRIMARY KEY,
  service_id INT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  service_segment_id INT,
  lat DOUBLE PRECISION NOT NULL,
  lng DOUBLE PRECISION NOT NULL,
  captured_at TIMESTAMPTZ DEFAULT NOW(),
  accuracy_m NUMERIC(8,2),
  speed_kmh NUMERIC(8,2),
  heading_deg NUMERIC(6,2),
  point_source VARCHAR(20) NOT NULL DEFAULT 'mobile'
);

ALTER TABLE service_points
  ADD COLUMN IF NOT EXISTS service_segment_id INT;
ALTER TABLE service_points
  ADD COLUMN IF NOT EXISTS accuracy_m NUMERIC(8,2);
ALTER TABLE service_points
  ADD COLUMN IF NOT EXISTS speed_kmh NUMERIC(8,2);
ALTER TABLE service_points
  ADD COLUMN IF NOT EXISTS heading_deg NUMERIC(6,2);
ALTER TABLE service_points
  ADD COLUMN IF NOT EXISTS point_source VARCHAR(20) NOT NULL DEFAULT 'mobile';

CREATE TABLE IF NOT EXISTS service_segments (
  id BIGSERIAL PRIMARY KEY,
  service_id INT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  driver_id INT NOT NULL REFERENCES users(id),
  fleet_number VARCHAR(50) NOT NULL,
  status VARCHAR(20) NOT NULL DEFAULT 'in_progress',
  started_at TIMESTAMPTZ DEFAULT NOW(),
  ended_at TIMESTAMPTZ,
  km_segment NUMERIC(10,3) DEFAULT 0
);

CREATE TABLE IF NOT EXISTS service_handover_events (
  id BIGSERIAL PRIMARY KEY,
  service_id INT NOT NULL REFERENCES services(id) ON DELETE CASCADE,
  from_segment_id BIGINT REFERENCES service_segments(id),
  from_driver_id INT NOT NULL REFERENCES users(id),
  to_driver_id INT REFERENCES users(id),
  from_fleet_number VARCHAR(50),
  to_fleet_number VARCHAR(50),
  reason VARCHAR(80) NOT NULL,
  notes TEXT,
  handover_lat DOUBLE PRECISION,
  handover_lng DOUBLE PRECISION,
  handover_location_text VARCHAR(200),
  status VARCHAR(20) NOT NULL DEFAULT 'pending',
  created_at TIMESTAMPTZ DEFAULT NOW(),
  completed_at TIMESTAMPTZ
);

CREATE TABLE IF NOT EXISTS gtfs_routes (
  route_id VARCHAR(120) PRIMARY KEY,
  route_short_name VARCHAR(80),
  route_long_name VARCHAR(255)
);

CREATE TABLE IF NOT EXISTS gtfs_trips (
  trip_id VARCHAR(120) PRIMARY KEY,
  route_id VARCHAR(120) NOT NULL REFERENCES gtfs_routes(route_id) ON DELETE CASCADE,
  service_id VARCHAR(120),
  trip_headsign VARCHAR(255),
  direction_id INT,
  shape_id VARCHAR(120)
);

ALTER TABLE gtfs_trips
  ADD COLUMN IF NOT EXISTS direction_id INT;

CREATE TABLE IF NOT EXISTS gtfs_shapes (
  id BIGSERIAL PRIMARY KEY,
  shape_id VARCHAR(120) NOT NULL,
  shape_pt_lat DOUBLE PRECISION NOT NULL,
  shape_pt_lon DOUBLE PRECISION NOT NULL,
  shape_pt_sequence INT NOT NULL
);

CREATE TABLE IF NOT EXISTS gtfs_stops (
  stop_id VARCHAR(120) PRIMARY KEY,
  stop_name VARCHAR(255),
  stop_lat DOUBLE PRECISION NOT NULL,
  stop_lon DOUBLE PRECISION NOT NULL
);

CREATE TABLE IF NOT EXISTS gtfs_stop_times (
  id BIGSERIAL PRIMARY KEY,
  trip_id VARCHAR(120) NOT NULL REFERENCES gtfs_trips(trip_id) ON DELETE CASCADE,
  arrival_time VARCHAR(20),
  departure_time VARCHAR(20),
  stop_id VARCHAR(120) REFERENCES gtfs_stops(stop_id) ON DELETE SET NULL,
  stop_sequence INT
);

CREATE INDEX IF NOT EXISTS idx_gtfs_routes_short_name
  ON gtfs_routes(route_short_name);
CREATE INDEX IF NOT EXISTS idx_gtfs_trips_route
  ON gtfs_trips(route_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_shapes_shape_seq
  ON gtfs_shapes(shape_id, shape_pt_sequence);
CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip_seq
  ON gtfs_stop_times(trip_id, stop_sequence);
CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip_departure
  ON gtfs_stop_times(trip_id, departure_time);

ALTER TABLE service_handover_events
  ADD COLUMN IF NOT EXISTS handover_lat DOUBLE PRECISION;
ALTER TABLE service_handover_events
  ADD COLUMN IF NOT EXISTS handover_lng DOUBLE PRECISION;
ALTER TABLE service_handover_events
  ADD COLUMN IF NOT EXISTS handover_location_text VARCHAR(200);

DO $$
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM pg_constraint
    WHERE conname = 'fk_service_points_segment'
  ) THEN
    ALTER TABLE service_points
      ADD CONSTRAINT fk_service_points_segment
      FOREIGN KEY (service_segment_id) REFERENCES service_segments(id) ON DELETE SET NULL;
  END IF;
END $$;

CREATE INDEX IF NOT EXISTS idx_services_driver_status
  ON services(driver_id, status);

CREATE INDEX IF NOT EXISTS idx_points_service_time
  ON service_points(service_id, captured_at);

CREATE INDEX IF NOT EXISTS idx_roster_driver_date
  ON daily_roster(driver_id, service_date);

CREATE INDEX IF NOT EXISTS idx_roster_history_date
  ON daily_roster_history(service_date, archived_at);

CREATE INDEX IF NOT EXISTS idx_segments_service_status
  ON service_segments(service_id, status);

CREATE INDEX IF NOT EXISTS idx_handover_to_driver_status
  ON service_handover_events(to_driver_id, status);

CREATE TABLE IF NOT EXISTS driver_notifications (
  id BIGSERIAL PRIMARY KEY,
  driver_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  title VARCHAR(200) NOT NULL,
  message TEXT NOT NULL,
  notification_type VARCHAR(40) NOT NULL DEFAULT 'roster_change',
  roster_id INT,
  read_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_driver_notifications_driver_created
  ON driver_notifications(driver_id, created_at DESC);

CREATE TABLE IF NOT EXISTS tracker_devices (
  id BIGSERIAL PRIMARY KEY,
  imei VARCHAR(40) UNIQUE NOT NULL,
  fleet_number VARCHAR(50),
  plate_number VARCHAR(50),
  provider VARCHAR(40) NOT NULL DEFAULT 'teltonika',
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_tracker_devices_fleet
  ON tracker_devices(fleet_number);
CREATE INDEX IF NOT EXISTS idx_tracker_devices_plate
  ON tracker_devices(plate_number);

-- ===============================
-- Incremental migration (2026-04)
-- ===============================

-- Tracker/vehicle odometer enrichment
ALTER TABLE tracker_devices
  ADD COLUMN IF NOT EXISTS install_odometer_km NUMERIC(12,1);
ALTER TABLE tracker_devices
  ADD COLUMN IF NOT EXISTS current_odometer_km NUMERIC(12,1);
ALTER TABLE tracker_devices
  ADD COLUMN IF NOT EXISTS current_odometer_updated_at TIMESTAMPTZ;

-- Service close/odometer reconciliation
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS vehicle_odometer_start_km NUMERIC(12,1);
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS vehicle_odometer_end_km NUMERIC(12,1);
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS vehicle_odometer_delta_km NUMERIC(12,3);
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS vehicle_odometer_vs_gps_diff_km NUMERIC(12,3);
ALTER TABLE services
  ADD COLUMN IF NOT EXISTS close_mode VARCHAR(30);

-- Stop progress for announcements and last-stop auto-close
CREATE TABLE IF NOT EXISTS service_stop_progress (
  service_id BIGINT PRIMARY KEY REFERENCES services(id) ON DELETE CASCADE,
  last_passed_stop_id VARCHAR(120),
  last_passed_stop_sequence INT,
  last_passed_at TIMESTAMPTZ,
  last_announced_stop_id VARCHAR(120),
  last_announced_stop_sequence INT,
  last_announcement_type VARCHAR(20),
  last_announced_at TIMESTAMPTZ,
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

-- Deadhead movement tracking (empty trips)
CREATE TABLE IF NOT EXISTS deadhead_movements (
  id BIGSERIAL PRIMARY KEY,
  imei VARCHAR(40) NOT NULL,
  fleet_number VARCHAR(50),
  plate_number VARCHAR(50),
  started_at TIMESTAMPTZ NOT NULL,
  ended_at TIMESTAMPTZ NOT NULL,
  start_lat NUMERIC(10,7),
  start_lng NUMERIC(10,7),
  end_lat NUMERIC(10,7),
  end_lng NUMERIC(10,7),
  total_km NUMERIC(12,3) NOT NULL DEFAULT 0,
  points_count INT NOT NULL DEFAULT 0,
  open_state BOOLEAN NOT NULL DEFAULT TRUE,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE TABLE IF NOT EXISTS deadhead_points (
  id BIGSERIAL PRIMARY KEY,
  movement_id BIGINT NOT NULL REFERENCES deadhead_movements(id) ON DELETE CASCADE,
  lat NUMERIC(10,7) NOT NULL,
  lng NUMERIC(10,7) NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL,
  speed_kmh NUMERIC(8,2),
  heading_deg NUMERIC(6,2),
  accuracy_m NUMERIC(8,2)
);

CREATE INDEX IF NOT EXISTS idx_deadhead_movements_open
  ON deadhead_movements(imei, open_state, updated_at DESC);
CREATE INDEX IF NOT EXISTS idx_deadhead_movements_started
  ON deadhead_movements(started_at DESC);
CREATE INDEX IF NOT EXISTS idx_deadhead_points_movement
  ON deadhead_points(movement_id, captured_at ASC);

-- Odometer log history for robust reconciliation
CREATE TABLE IF NOT EXISTS vehicle_odometer_logs (
  id BIGSERIAL PRIMARY KEY,
  imei VARCHAR(40),
  fleet_number VARCHAR(50),
  plate_number VARCHAR(50),
  odometer_km NUMERIC(12,1) NOT NULL,
  captured_at TIMESTAMPTZ NOT NULL,
  source VARCHAR(30) NOT NULL DEFAULT 'tracker',
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_vehicle_odometer_logs_captured
  ON vehicle_odometer_logs(captured_at DESC);
CREATE INDEX IF NOT EXISTS idx_vehicle_odometer_logs_fleet
  ON vehicle_odometer_logs(fleet_number, captured_at DESC);

-- Supervisor conflict table enrichment
CREATE TABLE IF NOT EXISTS supervisor_conflict_alerts (
  id BIGSERIAL PRIMARY KEY,
  driver_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  alert_type VARCHAR(50) NOT NULL DEFAULT 'roster_conflict',
  roster_id INT,
  planned_service_id INT,
  affected_driver_id INT REFERENCES users(id) ON DELETE SET NULL,
  affected_planned_service_id INT,
  service_schedule VARCHAR(80),
  line_code VARCHAR(40),
  conflict_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  unassigned_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb,
  notes TEXT,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

ALTER TABLE supervisor_conflict_alerts
  ADD COLUMN IF NOT EXISTS alert_type VARCHAR(50) NOT NULL DEFAULT 'roster_conflict';
ALTER TABLE supervisor_conflict_alerts
  ADD COLUMN IF NOT EXISTS affected_driver_id INT REFERENCES users(id) ON DELETE SET NULL;
ALTER TABLE supervisor_conflict_alerts
  ADD COLUMN IF NOT EXISTS affected_planned_service_id INT;
ALTER TABLE supervisor_conflict_alerts
  ADD COLUMN IF NOT EXISTS unassigned_planned_service_ids JSONB NOT NULL DEFAULT '[]'::jsonb;
CREATE INDEX IF NOT EXISTS idx_supervisor_conflict_alerts_created
  ON supervisor_conflict_alerts(created_at DESC);

-- Operational messaging tables
CREATE TABLE IF NOT EXISTS ops_messages (
  id BIGSERIAL PRIMARY KEY,
  from_user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  to_user_id INT NOT NULL REFERENCES users(id) ON DELETE CASCADE,
  message_text TEXT NOT NULL,
  preset_code VARCHAR(60),
  is_traffic_alert BOOLEAN NOT NULL DEFAULT FALSE,
  related_service_id BIGINT REFERENCES services(id) ON DELETE SET NULL,
  read_at TIMESTAMPTZ,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW()
);

CREATE INDEX IF NOT EXISTS idx_ops_messages_to_user_created
  ON ops_messages(to_user_id, created_at DESC);
CREATE INDEX IF NOT EXISTS idx_ops_messages_users_created
  ON ops_messages(from_user_id, to_user_id, created_at DESC);

CREATE TABLE IF NOT EXISTS ops_message_presets (
  id BIGSERIAL PRIMARY KEY,
  scope VARCHAR(20) NOT NULL,
  code VARCHAR(60) NOT NULL,
  label VARCHAR(200) NOT NULL,
  default_message_text TEXT,
  is_active BOOLEAN NOT NULL DEFAULT TRUE,
  created_by_user_id INT REFERENCES users(id) ON DELETE SET NULL,
  created_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  updated_at TIMESTAMPTZ NOT NULL DEFAULT NOW(),
  UNIQUE (scope, code)
);

CREATE INDEX IF NOT EXISTS idx_ops_message_presets_scope_active
  ON ops_message_presets(scope, is_active);

-- GTFS editor/query performance indexes
CREATE INDEX IF NOT EXISTS idx_gtfs_trips_route_id
  ON gtfs_trips(route_id);
CREATE INDEX IF NOT EXISTS idx_gtfs_stop_times_trip_seq
  ON gtfs_stop_times(trip_id, stop_sequence);
