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
