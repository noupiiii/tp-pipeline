-- Schéma du Data Warehouse (exécuté automatiquement au premier démarrage du
-- conteneur postgres-dwh via docker-entrypoint-initdb.d).

CREATE TABLE IF NOT EXISTS weather_hourly (
    id                SERIAL PRIMARY KEY,
    latitude          DOUBLE PRECISION NOT NULL,
    longitude         DOUBLE PRECISION NOT NULL,
    observed_at       TIMESTAMP NOT NULL,
    temperature_c     DOUBLE PRECISION,
    humidity_pct      DOUBLE PRECISION,
    wind_speed_kmh    DOUBLE PRECISION,
    precipitation_mm  DOUBLE PRECISION,
    loaded_at         TIMESTAMP NOT NULL DEFAULT now(),
    UNIQUE (latitude, longitude, observed_at)
);

CREATE INDEX IF NOT EXISTS idx_weather_hourly_observed_at ON weather_hourly (observed_at);
