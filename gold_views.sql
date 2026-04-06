-- ═══════════════════════════════════════════════════════════════════
-- Task 4 — Silver Layer DDL + Gold Views
-- Run these in AWS Athena (Query Editor) in order:
--   STEP 1: Create database
--   STEP 2: Create silver external tables (pointing to S3 parquet)
--   STEP 3: Create gold views on top of silver tables
-- Replace <YOUR_BUCKET> with your actual S3 bucket name.
-- ═══════════════════════════════════════════════════════════════════


-- ─────────────────────────────────────────────────────────────────
-- STEP 1: Create database
-- ─────────────────────────────────────────────────────────────────
CREATE DATABASE IF NOT EXISTS smart_facility;


-- ─────────────────────────────────────────────────────────────────
-- STEP 2: Silver External Tables
-- ─────────────────────────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS smart_facility.silver_sites (
    site_id             STRING,
    site_name           STRING,
    city                STRING,
    region              STRING,
    site_category       STRING
)
STORED AS PARQUET
LOCATION 's3://smart-facility-dhiraj/silver/silver_sites/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ─────────────────────────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS smart_facility.silver_devices (
    device_id               STRING,
    site_id                 STRING,
    device_type             STRING,
    install_date            STRING,
    firmware_version        STRING,
    status                  STRING
)
STORED AS PARQUET
LOCATION 's3://smart-facility-dhiraj/silver/silver_devices/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');

-- ─────────────────────────────────────────────────────────────────

CREATE EXTERNAL TABLE IF NOT EXISTS smart_facility.silver_sensor_events (
    event_id                    STRING,
    event_ts                    TIMESTAMP,
    device_id                   STRING,
    site_id                     STRING,
    temperature_c               DOUBLE,
    battery_pct                 DOUBLE,
    signal_strength             DOUBLE,
    alert_flag                  BOOLEAN,
    alert_type                  STRING,
    battery_pct_is_null         BOOLEAN,
    signal_strength_is_invalid  BOOLEAN,
    event_date                  STRING,
    event_hour                  INT,
    device_is_inactive          BOOLEAN
)
STORED AS PARQUET
LOCATION 's3://smart-facility-dhiraj/silver/silver_sensor_events/'
TBLPROPERTIES ('parquet.compress' = 'SNAPPY');


-- ─────────────────────────────────────────────────────────────────
-- STEP 3A: Gold View — Site Hourly Health
-- Shows per-site, per-hour aggregates: event count, alert count,
-- avg temperature, avg battery level.
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW smart_facility.vw_site_hourly_health AS
SELECT
    DATE_TRUNC('hour', e.event_ts)              AS hour,
    e.site_id,
    s.site_name,
    s.city,
    s.region,
    s.site_category,
    COUNT(*)                                    AS event_count,
    SUM(CASE WHEN e.alert_flag = TRUE THEN 1 ELSE 0 END)
                                                AS alert_count,
    ROUND(AVG(e.temperature_c), 2)              AS avg_temperature_c,
    ROUND(AVG(e.battery_pct), 2)                AS avg_battery_pct,
    ROUND(AVG(e.signal_strength), 2)            AS avg_signal_strength_dbm
FROM smart_facility.silver_sensor_events e
LEFT JOIN smart_facility.silver_sites s
    ON e.site_id = s.site_id
GROUP BY
    DATE_TRUNC('hour', e.event_ts),
    e.site_id,
    s.site_name,
    s.city,
    s.region,
    s.site_category
ORDER BY
    hour DESC,
    event_count DESC;


-- ─────────────────────────────────────────────────────────────────
-- STEP 3B: Gold View — Device Alert Summary
-- Shows per-device breakdown of all alert types.
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW smart_facility.vw_device_alert_summary AS
SELECT
    e.device_id,
    e.site_id,
    d.device_type,
    d.firmware_version,
    d.status                                    AS device_status,
    COUNT(*)                                    AS total_events,
    SUM(CASE WHEN e.alert_flag = TRUE THEN 1 ELSE 0 END)
                                                AS total_alerts,
    SUM(CASE WHEN e.alert_type = 'battery_low'      THEN 1 ELSE 0 END)
                                                AS battery_low_alerts,
    SUM(CASE WHEN e.alert_type = 'temp_high'        THEN 1 ELSE 0 END)
                                                AS temp_high_alerts,
    SUM(CASE WHEN e.alert_type = 'offline'          THEN 1 ELSE 0 END)
                                                AS offline_alerts,
    SUM(CASE WHEN e.alert_type = 'motion_detected'  THEN 1 ELSE 0 END)
                                                AS motion_detected_alerts,
    ROUND(AVG(e.temperature_c), 2)              AS avg_temperature_c,
    ROUND(AVG(e.battery_pct), 2)                AS avg_battery_pct
FROM smart_facility.silver_sensor_events e
LEFT JOIN smart_facility.silver_devices d
    ON e.device_id = d.device_id
GROUP BY
    e.device_id,
    e.site_id,
    d.device_type,
    d.firmware_version,
    d.status
ORDER BY
    total_alerts DESC;


-- ─────────────────────────────────────────────────────────────────
-- STEP 3C: Gold View — Site Daily KPIs
-- Daily roll-up per site: events, unique devices, critical alerts,
-- avg signal strength. Used for trend analysis.
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW smart_facility.vw_site_daily_kpis AS
SELECT
    CAST(e.event_date AS DATE)                  AS date,
    e.site_id,
    s.site_name,
    s.region,
    s.site_category,
    COUNT(*)                                    AS total_events,
    COUNT(DISTINCT e.device_id)                 AS unique_devices_reporting,
    SUM(CASE WHEN e.alert_flag = TRUE THEN 1 ELSE 0 END)
                                                AS total_alerts,
    SUM(CASE WHEN e.alert_type IN ('temp_high', 'offline') THEN 1 ELSE 0 END)
                                                AS critical_alerts,
    ROUND(AVG(
        CASE WHEN NOT e.signal_strength_is_invalid
             THEN e.signal_strength END
    ), 2)                                       AS avg_signal_strength_dbm,
    ROUND(AVG(e.temperature_c), 2)              AS avg_temperature_c,
    ROUND(AVG(e.battery_pct), 2)                AS avg_battery_pct
FROM smart_facility.silver_sensor_events e
LEFT JOIN smart_facility.silver_sites s
    ON e.site_id = s.site_id
GROUP BY
    CAST(e.event_date AS DATE),
    e.site_id,
    s.site_name,
    s.region,
    s.site_category
ORDER BY
    date DESC,
    total_events DESC;


-- ─────────────────────────────────────────────────────────────────
-- STEP 3D: Gold View — Abnormal Activity (last 24 hours)
-- Identifies sites/devices with spike in alerts vs. their 7-day avg.
-- ─────────────────────────────────────────────────────────────────
CREATE OR REPLACE VIEW smart_facility.vw_abnormal_activity_24h AS
WITH daily_kpis AS (
    SELECT * FROM smart_facility.vw_site_daily_kpis
),
recent AS (
    SELECT
        site_id,
        site_name,
        total_events        AS events_last_24h,
        total_alerts        AS alerts_last_24h,
        critical_alerts     AS critical_alerts_last_24h,
        avg_battery_pct     AS avg_battery_last_24h
    FROM daily_kpis
    WHERE date = (SELECT MAX(date) FROM daily_kpis)
),
baseline AS (
    SELECT
        site_id,
        AVG(total_events)    AS avg_daily_events_7d,
        AVG(total_alerts)    AS avg_daily_alerts_7d,
        AVG(critical_alerts) AS avg_critical_7d
    FROM daily_kpis
    GROUP BY site_id
)
SELECT
    r.site_id,
    r.site_name,
    r.events_last_24h,
    r.alerts_last_24h,
    r.critical_alerts_last_24h,
    ROUND(b.avg_daily_events_7d, 1)  AS avg_daily_events_7d,
    ROUND(b.avg_daily_alerts_7d, 1)  AS avg_daily_alerts_7d,
    ROUND(
        CASE WHEN b.avg_daily_alerts_7d > 0
             THEN (r.alerts_last_24h - b.avg_daily_alerts_7d) / b.avg_daily_alerts_7d * 100
             ELSE 0 END
    , 1)                             AS alert_spike_pct,
    r.avg_battery_last_24h,
    CASE
        WHEN r.critical_alerts_last_24h > b.avg_critical_7d * 1.5  THEN 'HIGH'
        WHEN r.alerts_last_24h          > b.avg_daily_alerts_7d * 1.3 THEN 'MEDIUM'
        ELSE 'NORMAL'
    END                              AS anomaly_level
FROM recent r
LEFT JOIN baseline b ON r.site_id = b.site_id
ORDER BY alert_spike_pct DESC;


-- ─────────────────────────────────────────────────────────────────
-- Verification queries — run these to confirm views work
-- ─────────────────────────────────────────────────────────────────

-- SELECT * FROM smart_facility.vw_site_hourly_health   LIMIT 10;
-- SELECT * FROM smart_facility.vw_device_alert_summary LIMIT 10;
-- SELECT * FROM smart_facility.vw_site_daily_kpis      LIMIT 10;
-- SELECT * FROM smart_facility.vw_abnormal_activity_24h;