/* =============================================================================
   GEO_PROJECT — MANUAL INGEST BY COPY (NO SNOWPIPE)
   - Loads from external stages into landing tables
   - Idempotent: skips files already loaded (by SOURCE_FILE)
   - Debug-friendly: check stage listing + copy history
   ============================================================================= */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;
USE DATABASE GEO_PROJECT;

-- =============================================================================
-- 0) QUICK SANITY: stages accessible
-- =============================================================================
-- LIST @TASKS.OSM_RAW_STAGE LIMIT 5;
-- LIST @TASKS.EUROSTAT_RAW_STAGE LIMIT 5;
-- LIST @TASKS.GISCO_RAW_STAGE LIMIT 5;

-- =============================================================================
-- 1) OPTIONAL: ensure landing tables exist (no harm if they already do)
-- =============================================================================
USE SCHEMA BRONZE;

CREATE TABLE IF NOT EXISTS BRONZE.LND_OSM_ADMIN (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_OSM_ROADS (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_OSM_CHARGING (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_OSM_POI_POINTS (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_OSM_POI_POLYGONS (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_OSM_PT_POINTS (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_OSM_PT_LINES (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_OSM_BUILDINGS_ACTIVITY (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS BRONZE.LND_GISCO_NUTS (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);

CREATE TABLE IF NOT EXISTS BRONZE.LND_EUROSTAT_LAU_DEGURBA (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_EUROSTAT_CENSUS_GRID_2021_EUROPE (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);
CREATE TABLE IF NOT EXISTS BRONZE.LND_EUROSTAT_TRAN_R_ELVEHST (RAW VARIANT, SOURCE_FILE STRING, LOAD_TS TIMESTAMP_NTZ);

-- =============================================================================
-- 2) INGEST HELPERS: "only new files" filter (idempotency)
--    We do it by selecting from stage and excluding SOURCE_FILE already present.
-- =============================================================================
USE SCHEMA TASKS;

-- -----------------------------------------------------------------------------
-- OSM.ADMIN
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_OSM_ADMIN (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT
    t.$1 AS RAW,
    t.METADATA$FILENAME::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '.*/admin/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/admin_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1
      FROM GEO_PROJECT.BRONZE.LND_OSM_ADMIN x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- OSM.ROADS
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_OSM_ROADS (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '.*/roads/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/roads_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_OSM_ROADS x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- OSM.CHARGING
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_OSM_CHARGING (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '.*/charging/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/charging_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_OSM_CHARGING x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- OSM.POI_POINTS
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_OSM_POI_POINTS (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '.*/poi_points/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/poi_points_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_OSM_POI_POINTS x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- OSM.POI_POLYGONS
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_OSM_POI_POLYGONS (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '.*/poi_polygons/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/poi_polygons_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_OSM_POI_POLYGONS x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- OSM.PT_POINTS
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_OSM_PT_POINTS (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '.*/pt_points/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/pt_points_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_OSM_PT_POINTS x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- OSM.PT_LINES
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_OSM_PT_LINES (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '.*/pt_lines/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/pt_lines_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_OSM_PT_LINES x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- OSM.BUILDINGS_ACTIVITY
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_OSM_BUILDINGS_ACTIVITY (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '.*/buildings_activity/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/buildings_activity_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_OSM_BUILDINGS_ACTIVITY x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- GISCO.NUTS
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_GISCO_NUTS (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.GISCO_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '^nuts/year=[0-9]{4}/scale=[^/]+/crs=[0-9]+/level=[0-9]+/.*[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_GISCO_NUTS x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- EUROSTAT.LAU_DEGURBA
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_EUROSTAT_LAU_DEGURBA (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.EUROSTAT_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '^degurba/lau/year=[0-9]{4}/.*_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_EUROSTAT_LAU_DEGURBA x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- EUROSTAT.CENSUS_GRID_2021_EUROPE
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_EUROSTAT_CENSUS_GRID_2021_EUROPE (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.EUROSTAT_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '^population_grid/europe/census_grid_2021/.*_sf[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_EUROSTAT_CENSUS_GRID_2021_EUROPE x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- -----------------------------------------------------------------------------
-- EUROSTAT.TRAN_R_ELVEHST
-- -----------------------------------------------------------------------------
COPY INTO GEO_PROJECT.BRONZE.LND_EUROSTAT_TRAN_R_ELVEHST (RAW, SOURCE_FILE, LOAD_TS)
FROM (
  SELECT t.$1, t.METADATA$FILENAME::STRING, CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.EUROSTAT_RAW_STAGE t
  WHERE REGEXP_LIKE(t.METADATA$FILENAME::STRING, '^tran_r_elvehst/snapshot=[^/]+/tran_r_elvehst_tidy[.]parquet$')
    AND NOT EXISTS (
      SELECT 1 FROM GEO_PROJECT.BRONZE.LND_EUROSTAT_TRAN_R_ELVEHST x
      WHERE x.SOURCE_FILE = t.METADATA$FILENAME::STRING
    )
)
FILE_FORMAT = (FORMAT_NAME = TASKS.FF_PARQUET);

-- =============================================================================
-- 3) DEBUG CHECKS 
-- =============================================================================
-- Counts
-- SELECT COUNT(*) AS CNT_ADMIN    FROM GEO_PROJECT.BRONZE.LND_OSM_ADMIN;
-- SELECT COUNT(*) AS CNT_ROADS    FROM GEO_PROJECT.BRONZE.LND_OSM_ROADS;
-- SELECT COUNT(*) AS CNT_CHARGING FROM GEO_PROJECT.BRONZE.LND_OSM_CHARGING;

-- Last loaded files (per table)
-- SELECT SOURCE_FILE, MAX(LOAD_TS) AS LAST_LOAD_TS
-- FROM GEO_PROJECT.BRONZE.LND_OSM_CHARGING
-- GROUP BY SOURCE_FILE
-- ORDER BY LAST_LOAD_TS DESC
-- LIMIT 20;

-- COPY history (last 24h, any landing)
-- SELECT *
-- FROM TABLE(INFORMATION_SCHEMA.COPY_HISTORY(
--   TABLE_NAME => 'GEO_PROJECT.BRONZE.LND_OSM_CHARGING',
--   START_TIME => DATEADD('HOUR', -24, CURRENT_TIMESTAMP())
-- ))
-- ORDER BY LAST_LOAD_TIME DESC;



MERGE INTO SILVER.ORDERS t
USING (
  WITH parsed AS (
    SELECT
      order_id::STRING                                    AS order_id,
      payload:customer_id::NUMBER                         AS customer_id,
      payload:order_cost::NUMBER(10,2)                    AS order_cost,
      TO_TIMESTAMP_NTZ(payload:order_ts::STRING)          AS order_ts,
      ingestion_ts::TIMESTAMP_NTZ                         AS ingestion_ts
    FROM BRONZE.ORDERS_RAW
  )
  SELECT
    order_id, customer_id, order_cost, order_ts, ingestion_ts
  FROM parsed
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY order_id
    ORDER BY ingestion_ts DESC
  ) = 1
) s
ON t.order_id = s.order_id
WHEN MATCHED AND s.ingestion_ts > t.ingestion_ts THEN UPDATE SET
  t.customer_id  = s.customer_id,
  t.order_cost   = s.order_cost,
  t.order_ts     = s.order_ts,
  t.ingestion_ts = s.ingestion_ts
WHEN NOT MATCHED THEN INSERT (order_id, customer_id, order_cost, order_ts, ingestion_ts)
VALUES (s.order_id, s.customer_id, s.order_cost, s.order_ts, s.ingestion_ts);


select c.customer_id
from CUSTOMERS c
where not exists (
  select 1
  from ORDERS_2026 o
  where o.customer_id = c.customer_id
)


SELECT column1, column2,...
FROM GOLD.FACT_SALES f
JOIN GOLD.DIM_CUSTOMER c
  ON f.customer_id = c.customer_id
WHERE f.event_ts >= '2026-02-10 00:00:00'::TIMESTAMP_NTZ AND f.event_ts < '2026-02-11 00:00:00'::TIMESTAMP_NTZ;

with watermark as (
  SELECT COALESCE(MAX(ingestion_ts), '1970-01-01'::TIMESTAMP_NTZ) AS max_ts
  FROM SILVER.EVENTS
),
new_events as (
  SELECT 
    event_id::STRING                                   AS event_id,
    ingestion_ts::TIMESTAMP_NTZ                        AS ingestion_ts,
    payload:user_id::NUMBER                            AS user_id,
    payload:event_type::STRING                         AS event_type,
    TO_TIMESTAMP_NTZ(payload:event_ts::STRING)         AS event_ts
  FROM BRONZE.EVENTS_RAW
  WHERE ingestion_ts >= (SELECT max_ts FROM watermark) - INTERVAL '1 day'
)
SELECT *
FROM src
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY event_id
  ORDER BY ingestion_ts DESC
) = 1;