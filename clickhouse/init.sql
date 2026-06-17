CREATE DATABASE IF NOT EXISTS aqi;

CREATE TABLE IF NOT EXISTS aqi.readings (
    location_id   UInt32,
    location_name String,
    city          String,
    country       String,
    parameter     String,
    value         Float32,
    unit          String,
    aqi           UInt16,
    aqi_category  String,
    latitude      Float64,
    longitude     Float64,
    measured_at   DateTime,
    ingested_at   DateTime DEFAULT now()
) ENGINE = MergeTree()
PARTITION BY toYYYYMM(measured_at)
ORDER BY (location_id, measured_at)
TTL measured_at + INTERVAL 1 YEAR;

-- Materialized view: hourly aggregates for the dashboard
CREATE TABLE IF NOT EXISTS aqi.hourly_avg (
    location_id   UInt32,
    location_name String,
    city          String,
    parameter     String,
    hour          DateTime,
    avg_value     Float32,
    avg_aqi       Float32,
    max_aqi       UInt16,
    reading_count UInt32
) ENGINE = SummingMergeTree()
ORDER BY (location_id, parameter, hour);

CREATE MATERIALIZED VIEW IF NOT EXISTS aqi.hourly_avg_mv
TO aqi.hourly_avg AS
SELECT
    location_id,
    location_name,
    city,
    parameter,
    toStartOfHour(measured_at) AS hour,
    avg(value)                 AS avg_value,
    avg(aqi)                   AS avg_aqi,
    max(aqi)                   AS max_aqi,
    count()                    AS reading_count
FROM aqi.readings
GROUP BY location_id, location_name, city, parameter, hour;
