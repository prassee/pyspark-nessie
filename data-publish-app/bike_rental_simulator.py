"""
-- Stations table
CREATE TABLE stations (
  station_id VARCHAR(50) PRIMARY KEY,
  station_name VARCHAR(255) NOT NULL,
  latitude DECIMAL(10, 8) NOT NULL,
  longitude DECIMAL(11, 8) NOT NULL,
  capacity INTEGER NOT NULL,
  available_bikes INTEGER DEFAULT 0,
  available_docks INTEGER DEFAULT 0,
  is_active BOOLEAN DEFAULT TRUE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
);

-- Bikes table
CREATE TABLE bikes (
  bike_id VARCHAR(50) PRIMARY KEY,
  bike_type VARCHAR(20) NOT NULL CHECK (bike_type IN ('electric', 'classic')),
  current_station_id VARCHAR(50),
  battery_level INTEGER CHECK (battery_level BETWEEN 0 AND 100),
  is_available BOOLEAN DEFAULT TRUE,
  last_maintenance TIMESTAMP,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (current_station_id) REFERENCES stations(station_id)
);

-- Rentals table
CREATE TABLE rentals (
  rental_id VARCHAR(50) PRIMARY KEY,
  customer_id VARCHAR(50) NOT NULL,
  bike_id VARCHAR(50) NOT NULL,
  start_station_id VARCHAR(50) NOT NULL,
  end_station_id VARCHAR(50),
  start_time TIMESTAMP NOT NULL,
  end_time TIMESTAMP,
  duration_minutes INTEGER,
  customer_type VARCHAR(20) CHECK (customer_type IN ('subscriber', 'casual')),
  rental_cost DECIMAL(10, 2),
  is_completed BOOLEAN DEFAULT FALSE,
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  updated_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  FOREIGN KEY (bike_id) REFERENCES bikes(bike_id),
  FOREIGN KEY (start_station_id) REFERENCES stations(station_id),
  FOREIGN KEY (end_station_id) REFERENCES stations(station_id)
);
"""
