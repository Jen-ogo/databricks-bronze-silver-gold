-- =============================================================================
-- Databricks / Delta Lake
-- SILVER BOOTSTRAP (Snowflake-like structure & naming)
-- Catalog: geo_databricks_sub
-- Schema : silver
-- =============================================================================

USE CATALOG geo_databricks_sub;
CREATE SCHEMA IF NOT EXISTS silver;
USE SCHEMA silver;

-- -----------------------------------------------------------------------------
-- 0) DROP (idempotent)
-- -----------------------------------------------------------------------------
DROP TABLE IF EXISTS geo_databricks_sub.silver.admin_areas;
DROP TABLE IF EXISTS geo_databricks_sub.silver.ev_chargers;
DROP TABLE IF EXISTS geo_databricks_sub.silver.poi_points;
DROP TABLE IF EXISTS geo_databricks_sub.silver.poi_areas;
DROP TABLE IF EXISTS geo_databricks_sub.silver.transit_points;
DROP TABLE IF EXISTS geo_databricks_sub.silver.transit_lines;
DROP TABLE IF EXISTS geo_databricks_sub.silver.road_segments;
DROP TABLE IF EXISTS geo_databricks_sub.silver.activity_places;
DROP TABLE IF EXISTS geo_databricks_sub.silver.building_footprints;
DROP TABLE IF EXISTS geo_databricks_sub.silver.building_footprints_model;

DROP TABLE IF EXISTS geo_databricks_sub.silver.eurostat_tran_r_elvehst;
DROP TABLE IF EXISTS geo_databricks_sub.silver.gisco_nuts;
DROP TABLE IF EXISTS geo_databricks_sub.silver.lau_degurba;
DROP TABLE IF EXISTS geo_databricks_sub.silver.census_grid_2021_europe;
DROP TABLE IF EXISTS geo_databricks_sub.silver.census_grid_2021_admin4;

DROP TABLE IF EXISTS geo_databricks_sub.silver.dim_h3_r10_cells;

DROP TABLE IF EXISTS geo_databricks_sub.silver.feat_h3_pop_r10;
DROP TABLE IF EXISTS geo_databricks_sub.silver.feat_h3_ev_chargers_r10;
DROP TABLE IF EXISTS geo_databricks_sub.silver.feat_h3_roads_r10;
DROP TABLE IF EXISTS geo_databricks_sub.silver.feat_h3_poi_r10;
DROP TABLE IF EXISTS geo_databricks_sub.silver.feat_h3_transit_r10;
DROP TABLE IF EXISTS geo_databricks_sub.silver.feat_h3_activity_places_r10;
DROP TABLE IF EXISTS geo_databricks_sub.silver.feat_h3_degurba_r10;

-- =============================================================================
-- 1) SILVER.EUROSTAT_TRAN_R_ELVEHST
-- Source : BRONZE.EUROSTAT_TRAN_R_ELVEHST
-- Target : SILVER.EUROSTAT_TRAN_R_ELVEHST (Delta)
-- Key    : (geo, year, vehicle, unit, freq, snapshot) latest by ingest_ts/load_ts/source_file
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.eurostat_tran_r_elvehst
USING DELTA
PARTITIONED BY (snapshot)
AS
WITH base AS (
  SELECT
    source_file, snapshot, dataset, freq, vehicle, unit, geo,
    CAST(year AS BIGINT)  AS year,
    CAST(value AS DOUBLE) AS value,
    CAST(ingest_ts_raw AS BIGINT) AS ingest_ts_raw,
    ingest_ts, load_ts
  FROM geo_databricks_sub.bronze.eurostat_tran_r_elvehst
  WHERE geo IS NOT NULL AND vehicle IS NOT NULL AND year IS NOT NULL AND value IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY geo, year, vehicle, unit, freq, snapshot
    ORDER BY ingest_ts DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.eurostat_tran_r_elvehst;
-- SELECT SUM(CASE WHEN geo IS NULL OR year IS NULL OR vehicle IS NULL THEN 1 ELSE 0 END) AS key_null_cnt
-- FROM geo_databricks_sub.silver.eurostat_tran_r_elvehst;
-- SELECT geo, year, vehicle, unit, freq, snapshot, COUNT(*) c
-- FROM geo_databricks_sub.silver.eurostat_tran_r_elvehst
-- GROUP BY 1,2,3,4,5,6 HAVING COUNT(*)>1 ORDER BY c DESC LIMIT 50;

-- =============================================================================
-- 2) SILVER.GISCO_NUTS
-- Source : BRONZE.GISCO_NUTS
-- Target : SILVER.GISCO_NUTS (Delta)
-- Key    : (level, cntr_code, nuts_id, year, scale, crs) latest by load_ts/source_file
-- Geo    : geom_wkb (BINARY), geom_wkt_4326 (STRING debug)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.gisco_nuts
USING DELTA
PARTITIONED BY (year, scale, crs, level)
AS
WITH base AS (
  SELECT
    CAST(nuts_id AS STRING)    AS nuts_id,
    CAST(cntr_code AS STRING)  AS cntr_code,
    CAST(name_latn AS STRING)  AS name_latn,
    CAST(levl_code AS STRING)  AS levl_code,
    CAST(level AS INT)         AS level,
    CAST(year AS STRING)       AS year,
    CAST(scale AS STRING)      AS scale,
    CAST(crs AS STRING)        AS crs,

    ST_AsBinary(ST_GeomFromWKT(geom_wkt)) AS geom_wkb,
    ST_AsText(ST_GeomFromWKT(geom_wkt))   AS geom_wkt_4326,

    source_file,
    CAST(load_ts AS TIMESTAMP) AS load_ts
  FROM geo_databricks_sub.bronze.gisco_nuts
  WHERE nuts_id IS NOT NULL AND cntr_code IS NOT NULL AND level IS NOT NULL AND geom_wkt IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY level, cntr_code, nuts_id, year, scale, crs
    ORDER BY load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
WHERE geom_wkb IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.gisco_nuts;
-- SELECT level, cntr_code, COUNT(*) c FROM geo_databricks_sub.silver.gisco_nuts GROUP BY 1,2 ORDER BY c DESC;
-- SELECT nuts_id, level, cntr_code, substring(geom_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.gisco_nuts LIMIT 20;
-- SELECT SUM(CASE WHEN NOT ST_IsValid(ST_GeomFromWKB(geom_wkb)) THEN 1 ELSE 0 END) AS invalid_cnt FROM geo_databricks_sub.silver.gisco_nuts;

-- =============================================================================
-- 3) SILVER.CENSUS_GRID_2021_EUROPE
-- Source : BRONZE.CENSUS_GRID_2021_EUROPE
-- Target : SILVER.CENSUS_GRID_2021_EUROPE (Delta)
-- Key    : grd_id (plus source_file); keep latest by load_ts if needed later
-- Geo    : geom_wkb (BINARY), geom_wkt_4326 (STRING debug)
-- Note   : -9999 -> NULL
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.census_grid_2021_europe
USING DELTA
PARTITIONED BY (source_file)
AS
SELECT
  CAST(grd_id AS STRING) AS grd_id,

  NULLIF(CAST(t AS BIGINT), -9999)      AS t,
  NULLIF(CAST(m AS BIGINT), -9999)      AS m,
  NULLIF(CAST(f AS BIGINT), -9999)      AS f,
  NULLIF(CAST(y_lt15 AS BIGINT), -9999) AS y_lt15,
  NULLIF(CAST(y_1564 AS BIGINT), -9999) AS y_1564,
  NULLIF(CAST(y_ge65 AS BIGINT), -9999) AS y_ge65,
  NULLIF(CAST(emp AS BIGINT), -9999)    AS emp,

  NULLIF(CAST(nat AS BIGINT), -9999)    AS nat,
  NULLIF(CAST(eu_oth AS BIGINT), -9999) AS eu_oth,
  NULLIF(CAST(oth AS BIGINT), -9999)    AS oth,
  NULLIF(CAST(same AS BIGINT), -9999)   AS same,
  NULLIF(CAST(chg_in AS BIGINT), -9999) AS chg_in,
  NULLIF(CAST(chg_out AS BIGINT), -9999)AS chg_out,

  CAST(land_surface AS DOUBLE)          AS land_surface,
  NULLIF(CAST(populated AS BIGINT), -9999) AS populated,

  ST_AsBinary(ST_GeomFromWKT(geom_wkt)) AS geom_wkb,
  ST_AsText(ST_GeomFromWKT(geom_wkt))   AS geom_wkt_4326,

  CAST(source_file AS STRING) AS source_file,
  CAST(load_ts AS TIMESTAMP)  AS load_ts
FROM geo_databricks_sub.bronze.census_grid_2021_europe
WHERE geom_wkt IS NOT NULL
  AND NULLIF(CAST(t AS BIGINT), -9999) IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.census_grid_2021_europe;
-- SELECT SUM(CASE WHEN geom_wkt_4326 IS NULL OR length(trim(geom_wkt_4326))=0 THEN 1 ELSE 0 END) AS wkt_empty_cnt
-- FROM geo_databricks_sub.silver.census_grid_2021_europe;
-- SELECT grd_id, substring(geom_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.census_grid_2021_europe LIMIT 20;

-- =============================================================================
-- 4) SILVER.ADMIN_AREAS (OSM admin polygons)
-- Source : BRONZE.OSM_ADMIN
-- Target : SILVER.ADMIN_AREAS (Delta)
-- Key    : (region_code, region, osm_id) latest by dt/load_ts/source_file
-- Geo    : geom (GEOMETRY), geom_wkt_4326 (STRING debug)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.admin_areas
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH base AS (
  SELECT
    CONCAT('A', CAST(osm_id AS STRING)) AS feature_id,
    CAST(osm_id AS STRING) AS osm_id,
    CAST(NULLIF(name,'') AS STRING) AS name,
    CAST(admin_level AS INT) AS admin_level,
    CAST(boundary AS STRING) AS boundary,
    CAST(type AS STRING) AS type,
    CAST(other_tags AS STRING) AS other_tags_raw,

    TRY_CAST(NULLIF(TRIM(regexp_extract(other_tags, '\\"population\\"=>\\"([^\\"]*)\\"', 1)), '') AS BIGINT) AS population,
    TRY_TO_DATE(NULLIF(TRIM(regexp_extract(other_tags, '\\"population:date\\"=>\\"([^\\"]*)\\"', 1)), ''))     AS population_date,
    CAST(NULLIF(TRIM(regexp_extract(other_tags, '\\"name:en\\"=>\\"([^\\"]*)\\"', 1)), '') AS STRING)          AS name_en,

    geom_wkb,
    ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326) AS geom,
    ST_AsText(ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326)) AS geom_wkt_4326,

    CAST(country AS STRING) AS region_code,
    CAST(region AS STRING)  AS region,

    CAST(dt AS DATE) AS dt,
    CAST(source_file AS STRING) AS source_file,
    CAST(load_ts AS TIMESTAMP)  AS load_ts
  FROM geo_databricks_sub.bronze.osm_admin
  WHERE osm_id IS NOT NULL AND geom_wkb IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY region_code, region, osm_id
    ORDER BY dt DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
WHERE geom_wkt_4326 IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.admin_areas;
-- SELECT admin_level, COUNT(*) cnt FROM geo_databricks_sub.silver.admin_areas GROUP BY 1 ORDER BY 1;
-- SELECT substring(geom_wkt_4326,1,120) AS wkt_prefix, COUNT(*) cnt
-- FROM geo_databricks_sub.silver.admin_areas GROUP BY 1 ORDER BY cnt DESC LIMIT 50;

-- =============================================================================
-- 5) SILVER.LAU_DEGURBA (Eurostat LAU + DEGURBA, region_code via admin4)
-- Source : BRONZE.EUROSTAT_LAU_DEGURBA
-- Target : SILVER.LAU_DEGURBA (Delta)
-- Key    : (cntr_code, lau_id, year) latest by load_ts/source_file
-- Geo    : geom (GEOMETRY), geom_wkt_4326 (STRING debug), centroid used for admin4 match
-- Fix    : invalid polygons -> ST_Buffer(geom, 0.0)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.lau_degurba
USING DELTA
PARTITIONED BY (cntr_code, region_code, year)
AS
WITH src AS (
  SELECT
    CAST(gisco_id AS STRING)  AS gisco_id,
    CAST(cntr_code AS STRING) AS cntr_code,
    CAST(lau_id AS STRING)    AS lau_id,
    CAST(lau_name AS STRING)  AS lau_name,
    CAST(dgurba AS INT)       AS degurba,
    CAST(fid AS BIGINT)       AS fid,
    CAST(geom_wkt AS STRING)  AS geom_wkt_raw,
    CAST(year AS STRING)      AS year,
    CAST(source_file AS STRING) AS source_file,
    CAST(load_ts AS TIMESTAMP)  AS load_ts
  FROM geo_databricks_sub.bronze.eurostat_lau_degurba
  WHERE cntr_code IS NOT NULL AND lau_id IS NOT NULL AND geom_wkt IS NOT NULL
),
dedup AS (
  SELECT *
  FROM src
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY cntr_code, lau_id, year
    ORDER BY load_ts DESC, source_file DESC
  ) = 1
),
geo0 AS (
  SELECT
    CONCAT('LAU:', cntr_code, ':', lau_id, ':', year) AS feature_id,
    d.*,
    ST_SetSRID(ST_GeomFromWKT(geom_wkt_raw), 4326) AS geom0
  FROM dedup d
),
fixed AS (
  SELECT
    *,
    ST_IsValid(geom0) AS is_valid0,
    CASE WHEN ST_IsValid(geom0) THEN geom0 ELSE ST_Buffer(geom0, 0.0) END AS geom_fixed
  FROM geo0
  WHERE geom0 IS NOT NULL
),
flag AS (
  SELECT
    *,
    ST_IsValid(geom_fixed) AS is_valid_fixed,
    ST_AsText(geom_fixed)  AS geom_wkt_4326,
    ST_Centroid(geom_fixed) AS lau_centroid_geom
  FROM fixed
  WHERE geom_fixed IS NOT NULL
),
admin4 AS (
  SELECT
    region_code,
    region,
    ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326) AS admin4_geom
  FROM geo_databricks_sub.silver.admin_areas
  WHERE admin_level=4 AND boundary='administrative' AND geom_wkb IS NOT NULL
)
SELECT
  f.feature_id,
  f.gisco_id,
  f.cntr_code,
  f.lau_id,
  f.lau_name,
  f.degurba,
  f.fid,
  f.year,

  a.region_code,
  a.region,

  f.geom_wkt_raw,
  f.geom_wkt_4326,
  f.geom_fixed AS geom,
  f.is_valid0,
  f.is_valid_fixed,

  f.source_file,
  f.load_ts
FROM flag f
LEFT JOIN admin4 a
  ON ST_Contains(a.admin4_geom, f.lau_centroid_geom);

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.lau_degurba;
-- SELECT SUM(CASE WHEN region_code IS NULL THEN 1 ELSE 0 END) AS region_code_null_cnt FROM geo_databricks_sub.silver.lau_degurba;
-- SELECT SUM(CASE WHEN is_valid_fixed=false THEN 1 ELSE 0 END) AS invalid_fixed_cnt FROM geo_databricks_sub.silver.lau_degurba;
-- SELECT cntr_code, lau_id, year, substring(geom_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.lau_degurba LIMIT 20;

-- =============================================================================
-- 6) SILVER.EV_CHARGERS (OSM charging points)
-- Source : BRONZE.OSM_CHARGING
-- Target : SILVER.EV_CHARGERS (Delta)
-- Key    : (region_code, region, osm_id) latest by dt/load_ts/source_file
-- Geo    : geom (GEOMETRY), geom_wkt_4326 (STRING debug)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.ev_chargers
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH base AS (
  SELECT
    CONCAT('N', CAST(osm_id AS STRING)) AS feature_id,
    CAST(osm_id AS STRING) AS osm_id,
    CAST(NULLIF(name,'') AS STRING) AS name,
    CAST(NULLIF(ref,'')  AS STRING) AS ref,
    CAST(other_tags AS STRING) AS other_tags_raw,

    TRY_CAST(NULLIF(TRIM(regexp_extract(other_tags, '\\"capacity\\"=>\\"([^\\"]*)\\"', 1)), '') AS INT) AS capacity,
    TRY_CAST(NULLIF(TRIM(regexp_extract(other_tags, '\\"socket:type2\\"=>\\"([^\\"]*)\\"', 1)), '') AS INT) AS socket_type2_cnt,
    TRY_CAST(NULLIF(TRIM(regexp_extract(other_tags, '\\"socket:chademo\\"=>\\"([^\\"]*)\\"', 1)), '') AS INT) AS socket_chademo_cnt,
    TRY_CAST(NULLIF(TRIM(regexp_extract(other_tags, '\\"socket:type2_combo\\"=>\\"([^\\"]*)\\"', 1)), '') AS INT) AS socket_type2_combo_cnt,

    geom_wkb,
    ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326) AS geom,
    ST_AsText(ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326)) AS geom_wkt_4326,

    CAST(country AS STRING) AS region_code,
    CAST(region AS STRING)  AS region,
    CAST(dt AS DATE) AS dt,
    CAST(source_file AS STRING) AS source_file,
    CAST(load_ts AS TIMESTAMP)  AS load_ts
  FROM geo_databricks_sub.bronze.osm_charging
  WHERE osm_id IS NOT NULL AND geom_wkb IS NOT NULL
),
enriched AS (
  SELECT
    *,
    COALESCE(socket_type2_cnt,0) + COALESCE(socket_chademo_cnt,0) + COALESCE(socket_type2_combo_cnt,0) AS total_sockets_cnt,
    CASE WHEN COALESCE(socket_chademo_cnt,0) + COALESCE(socket_type2_combo_cnt,0) > 0 THEN TRUE ELSE FALSE END AS has_dc,
    CASE WHEN COALESCE(socket_type2_cnt,0) > 0 THEN TRUE ELSE FALSE END AS has_ac
  FROM base
),
dedup AS (
  SELECT *
  FROM enriched
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY region_code, region, osm_id
    ORDER BY dt DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
WHERE geom_wkt_4326 IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.ev_chargers;
-- SELECT SUM(CASE WHEN geom_wkt_4326 IS NULL THEN 1 ELSE 0 END) AS wkt_null_cnt FROM geo_databricks_sub.silver.ev_chargers;
-- SELECT substring(geom_wkt_4326,1,120) AS wkt_prefix, COUNT(*) cnt FROM geo_databricks_sub.silver.ev_chargers GROUP BY 1 ORDER BY cnt DESC LIMIT 50;

-- =============================================================================
-- 7) SILVER.ROAD_SEGMENTS (OSM roads)
-- Source : BRONZE.OSM_ROADS
-- Target : SILVER.ROAD_SEGMENTS (Delta)
-- Key    : (region_code, region, osm_id) latest by dt/load_ts/source_file
-- Geo    : geom (GEOMETRY), geom_wkt_4326 (STRING debug)
-- Notes  : filter small/pedestrian/service-private; normalize oneway/maxspeed
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.road_segments
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH base AS (
  SELECT
    CONCAT('W', CAST(osm_id AS STRING)) AS feature_id,
    CAST(osm_id AS STRING) AS osm_id,
    CAST(NULLIF(name,'') AS STRING) AS name,
    CAST(highway AS STRING) AS highway,
    CAST(z_order AS INT) AS z_order,
    CAST(other_tags AS STRING) AS other_tags_raw,

    NULLIF(TRIM(regexp_extract(other_tags, '\\"access\\"=>\\"([^\\"]*)\\"', 1)), '') AS access_raw,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"motor_vehicle\\"=>\\"([^\\"]*)\\"', 1)), '') AS motor_vehicle,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"motorcar\\"=>\\"([^\\"]*)\\"', 1)), '') AS motorcar,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"service\\"=>\\"([^\\"]*)\\"', 1)), '') AS service,

    TRY_CAST(NULLIF(TRIM(regexp_extract(other_tags, '\\"lanes\\"=>\\"([^\\"]*)\\"', 1)), '') AS INT) AS lanes,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"maxspeed\\"=>\\"([^\\"]*)\\"', 1)), '') AS maxspeed_raw,
    LOWER(NULLIF(TRIM(regexp_extract(other_tags, '\\"oneway\\"=>\\"([^\\"]*)\\"', 1)), '')) AS oneway_raw,

    geom_wkb,
    ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326) AS geom,
    ST_AsText(ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326)) AS geom_wkt_4326,

    CAST(country AS STRING) AS region_code,
    CAST(region AS STRING)  AS region,
    CAST(dt AS DATE) AS dt,
    CAST(source_file AS STRING) AS source_file,
    CAST(load_ts AS TIMESTAMP)  AS load_ts
  FROM geo_databricks_sub.bronze.osm_roads
  WHERE osm_id IS NOT NULL AND geom_wkb IS NOT NULL AND highway IS NOT NULL
),
typed AS (
  SELECT
    *,
    CASE WHEN oneway_raw IN ('yes','true','1') THEN TRUE
         WHEN oneway_raw IN ('no','false','0') THEN FALSE
         ELSE NULL END AS oneway,
    CASE
      WHEN maxspeed_raw IS NULL THEN NULL
      WHEN lower(maxspeed_raw) LIKE '%mph%' THEN TRY_CAST(regexp_extract(maxspeed_raw, '(\\d+)', 1) AS DOUBLE) * 1.60934
      ELSE TRY_CAST(regexp_extract(maxspeed_raw, '(\\d+)', 1) AS DOUBLE)
    END AS maxspeed_kph
  FROM base
),
filtered AS (
  SELECT *
  FROM typed
  WHERE LOWER(highway) NOT IN ('footway','path','steps','corridor','bridleway','cycleway','pedestrian')
    AND NOT (LOWER(highway)='service' AND LOWER(COALESCE(service,'')) IN ('driveway','parking_aisle','alley','emergency_access','private'))
    AND LOWER(COALESCE(access_raw, COALESCE(motor_vehicle, COALESCE(motorcar, 'yes')))) NOT IN ('no','private')
),
dedup AS (
  SELECT *
  FROM filtered
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY region_code, region, osm_id
    ORDER BY dt DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
WHERE geom_wkt_4326 IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.road_segments;
-- SELECT highway, COUNT(*) cnt FROM geo_databricks_sub.silver.road_segments GROUP BY 1 ORDER BY cnt DESC LIMIT 50;
-- SELECT SUM(CASE WHEN maxspeed_kph IS NOT NULL AND (maxspeed_kph < 0 OR maxspeed_kph > 200) THEN 1 ELSE 0 END) AS weird_speed_cnt
-- FROM geo_databricks_sub.silver.road_segments;

-- =============================================================================
-- 8) SILVER.POI_POINTS (OSM POI points)
-- Source : BRONZE.OSM_POI_POINTS
-- Target : SILVER.POI_POINTS (Delta)
-- Key    : (country, region, osm_id) latest by dt/load_ts/source_file
-- Geo    : geom_wkt_4326 (STRING debug)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.poi_points
USING DELTA
PARTITIONED BY (country, region)
AS
WITH base AS (
  SELECT
    CONCAT('N', osm_id) AS feature_id,
    osm_id,
    NULLIF(name,'') AS name,
    other_tags AS other_tags_raw,

    NULLIF(TRIM(regexp_extract(other_tags, '\\"amenity\\"=>\\"([^\\"]*)\\"', 1)), '') AS amenity,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"shop\\"=>\\"([^\\"]*)\\"', 1)), '')    AS shop,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"tourism\\"=>\\"([^\\"]*)\\"', 1)), '') AS tourism,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"leisure\\"=>\\"([^\\"]*)\\"', 1)), '') AS leisure,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"office\\"=>\\"([^\\"]*)\\"', 1)), '')  AS office,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"craft\\"=>\\"([^\\"]*)\\"', 1)), '')   AS craft,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"man_made\\"=>\\"([^\\"]*)\\"', 1)), '') AS man_made,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"public_transport\\"=>\\"([^\\"]*)\\"', 1)), '') AS public_transport,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"railway\\"=>\\"([^\\"]*)\\"', 1)), '') AS railway,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"highway\\"=>\\"([^\\"]*)\\"', 1)), '') AS highway,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"place\\"=>\\"([^\\"]*)\\"', 1)), '')   AS place,

    geom_wkb,
    ST_AsText(ST_GeomFromWKB(geom_wkb)) AS geom_wkt_4326,
    country, region, dt, source_file, load_ts
  FROM geo_databricks_sub.bronze.osm_poi_points
  WHERE osm_id IS NOT NULL AND geom_wkb IS NOT NULL
),
typed AS (
  SELECT
    *,
    CASE
      WHEN amenity IS NOT NULL THEN 'amenity'
      WHEN shop IS NOT NULL THEN 'shop'
      WHEN tourism IS NOT NULL THEN 'tourism'
      WHEN leisure IS NOT NULL THEN 'leisure'
      WHEN office IS NOT NULL THEN 'office'
      WHEN craft IS NOT NULL THEN 'craft'
      WHEN man_made IS NOT NULL THEN 'man_made'
      WHEN public_transport IS NOT NULL THEN 'public_transport'
      WHEN railway IS NOT NULL THEN 'railway'
      WHEN highway IS NOT NULL THEN 'highway'
      WHEN place IS NOT NULL THEN 'place'
      ELSE NULL
    END AS poi_class,
    COALESCE(amenity, shop, tourism, leisure, office, craft, man_made, public_transport, railway, highway, place) AS poi_type
  FROM base
),
dedup AS (
  SELECT *
  FROM typed
  WHERE poi_class IS NOT NULL AND poi_type IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY country, region, osm_id
    ORDER BY dt DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
WHERE geom_wkt_4326 IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.poi_points;
-- SELECT poi_class, COUNT(*) cnt FROM geo_databricks_sub.silver.poi_points GROUP BY 1 ORDER BY cnt DESC;
-- SELECT substring(geom_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.poi_points LIMIT 20;

-- =============================================================================
-- 9) SILVER.POI_AREAS (OSM POI polygons)
-- Source : BRONZE.OSM_POI_POLYGONS
-- Target : SILVER.POI_AREAS (Delta)
-- Key    : (country, region, feature_id) latest by dt/load_ts/source_file
-- Geo    : geom_wkt_4326 (STRING debug)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.poi_areas
USING DELTA
PARTITIONED BY (country, region)
AS
WITH base AS (
  SELECT
    COALESCE(NULLIF(osm_id,''), CONCAT('W', osm_way_id)) AS feature_id,
    NULLIF(osm_id,'') AS osm_id,
    osm_way_id,
    NULLIF(name,'') AS name,
    other_tags AS other_tags_raw,

    COALESCE(NULLIF(amenity,''),  NULLIF(TRIM(regexp_extract(other_tags, '\\"amenity\\"=>\\"([^\\"]*)\\"', 1)), ''))  AS amenity,
    COALESCE(NULLIF(shop,''),     NULLIF(TRIM(regexp_extract(other_tags, '\\"shop\\"=>\\"([^\\"]*)\\"', 1)), ''))     AS shop,
    COALESCE(NULLIF(tourism,''),  NULLIF(TRIM(regexp_extract(other_tags, '\\"tourism\\"=>\\"([^\\"]*)\\"', 1)), ''))  AS tourism,
    COALESCE(NULLIF(office,''),   NULLIF(TRIM(regexp_extract(other_tags, '\\"office\\"=>\\"([^\\"]*)\\"', 1)), ''))   AS office,
    COALESCE(NULLIF(leisure,''),  NULLIF(TRIM(regexp_extract(other_tags, '\\"leisure\\"=>\\"([^\\"]*)\\"', 1)), ''))  AS leisure,
    COALESCE(NULLIF(sport,''),    NULLIF(TRIM(regexp_extract(other_tags, '\\"sport\\"=>\\"([^\\"]*)\\"', 1)), ''))    AS sport,
    COALESCE(NULLIF(building,''), NULLIF(TRIM(regexp_extract(other_tags, '\\"building\\"=>\\"([^\\"]*)\\"', 1)), '')) AS building,
    COALESCE(NULLIF(landuse,''),  NULLIF(TRIM(regexp_extract(other_tags, '\\"landuse\\"=>\\"([^\\"]*)\\"', 1)), ''))  AS landuse,

    geom_wkb,
    ST_AsText(ST_GeomFromWKB(geom_wkb)) AS geom_wkt_4326,
    country, region, dt, source_file, load_ts
  FROM geo_databricks_sub.bronze.osm_poi_polygons
  WHERE geom_wkb IS NOT NULL
),
typed AS (
  SELECT
    *,
    CASE
      WHEN amenity IS NOT NULL THEN 'amenity'
      WHEN shop IS NOT NULL THEN 'shop'
      WHEN tourism IS NOT NULL THEN 'tourism'
      WHEN office IS NOT NULL THEN 'office'
      WHEN leisure IS NOT NULL THEN 'leisure'
      WHEN sport IS NOT NULL THEN 'sport'
      WHEN building IS NOT NULL THEN 'building'
      WHEN landuse IS NOT NULL THEN 'landuse'
      ELSE NULL
    END AS poi_class,
    COALESCE(amenity, shop, tourism, office, leisure, sport, building, landuse) AS poi_type
  FROM base
),
filtered AS (
  SELECT *
  FROM typed
  WHERE poi_class IS NOT NULL AND poi_type IS NOT NULL
),
dedup AS (
  SELECT *
  FROM filtered
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY country, region, feature_id
    ORDER BY dt DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
WHERE geom_wkt_4326 IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.poi_areas;
-- SELECT poi_class, COUNT(*) cnt FROM geo_databricks_sub.silver.poi_areas GROUP BY 1 ORDER BY cnt DESC;
-- SELECT substring(geom_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.poi_areas LIMIT 20;

-- =============================================================================
-- 10) SILVER.TRANSIT_POINTS (OSM public transport points)
-- Source : BRONZE.OSM_PT_POINTS
-- Target : SILVER.TRANSIT_POINTS (Delta)
-- Key    : (country, region, osm_id) latest by dt/load_ts/source_file
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.transit_points
USING DELTA
PARTITIONED BY (country, region)
AS
WITH base AS (
  SELECT
    CONCAT('N', osm_id) AS feature_id,
    osm_id,
    NULLIF(name,'') AS name,
    other_tags AS other_tags_raw,

    NULLIF(TRIM(regexp_extract(other_tags, '\\"public_transport\\"=>\\"([^\\"]*)\\"', 1)), '') AS public_transport,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"railway\\"=>\\"([^\\"]*)\\"', 1)), '') AS railway,
    NULLIF(TRIM(regexp_extract(other_tags, '\\"highway\\"=>\\"([^\\"]*)\\"', 1)), '') AS highway,

    geom_wkb,
    ST_AsText(ST_GeomFromWKB(geom_wkb)) AS geom_wkt_4326,
    country, region, dt, source_file, load_ts
  FROM geo_databricks_sub.bronze.osm_pt_points
  WHERE osm_id IS NOT NULL AND geom_wkb IS NOT NULL
),
typed AS (
  SELECT
    *,
    CASE
      WHEN public_transport IS NOT NULL OR railway IS NOT NULL OR LOWER(COALESCE(highway,'')) IN ('bus_stop','platform')
      THEN 'transport'
      ELSE NULL
    END AS transit_class,
    COALESCE(public_transport, railway, highway) AS transit_type
  FROM base
),
dedup AS (
  SELECT *
  FROM typed
  WHERE transit_class IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY country, region, osm_id
    ORDER BY dt DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
WHERE geom_wkt_4326 IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.transit_points;
-- SELECT substring(geom_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.transit_points LIMIT 20;

-- =============================================================================
-- 11) SILVER.TRANSIT_LINES (OSM public transport lines)
-- Source : BRONZE.OSM_PT_LINES
-- Target : SILVER.TRANSIT_LINES (Delta)
-- Key    : (country, region, osm_id) latest by dt/load_ts/source_file
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.transit_lines
USING DELTA
PARTITIONED BY (country, region)
AS
WITH base AS (
  SELECT
    CONCAT('W', osm_id) AS feature_id,
    osm_id,
    NULLIF(name,'') AS name,
    other_tags AS other_tags_raw,

    COALESCE(NULLIF(railway,''),  NULLIF(TRIM(regexp_extract(other_tags, '\\"railway\\"=>\\"([^\\"]*)\\"', 1)), ''))  AS railway2,
    COALESCE(NULLIF(waterway,''), NULLIF(TRIM(regexp_extract(other_tags, '\\"waterway\\"=>\\"([^\\"]*)\\"', 1)), '')) AS waterway2,
    COALESCE(NULLIF(aerialway,''),NULLIF(TRIM(regexp_extract(other_tags, '\\"aerialway\\"=>\\"([^\\"]*)\\"', 1)), '')) AS aerialway2,

    geom_wkb,
    ST_AsText(ST_GeomFromWKB(geom_wkb)) AS geom_wkt_4326,
    country, region, dt, source_file, load_ts
  FROM geo_databricks_sub.bronze.osm_pt_lines
  WHERE osm_id IS NOT NULL AND geom_wkb IS NOT NULL
),
typed AS (
  SELECT
    *,
    CASE
      WHEN railway2  IS NOT NULL THEN 'railway'
      WHEN waterway2 IS NOT NULL THEN 'waterway'
      WHEN aerialway2 IS NOT NULL THEN 'aerialway'
      ELSE NULL
    END AS line_class,
    COALESCE(railway2, waterway2, aerialway2) AS line_type
  FROM base
),
dedup AS (
  SELECT *
  FROM typed
  WHERE line_class IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY country, region, osm_id
    ORDER BY dt DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
WHERE geom_wkt_4326 IS NOT NULL;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.transit_lines;
-- SELECT substring(geom_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.transit_lines LIMIT 20;

-- =============================================================================
-- 12) SILVER.ACTIVITY_PLACES (OSM buildings_activity filtered for “places”)
-- Source : BRONZE.OSM_BUILDINGS_ACTIVITY
-- Target : SILVER.ACTIVITY_PLACES (Delta)
-- Key    : feature_id latest by load_ts/source_file
-- Geo    : geom (GEOMETRY), geom_wkt_4326 (STRING debug)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.activity_places
USING DELTA
PARTITIONED BY (region_code)
AS
WITH src AS (
  SELECT
    b.*,
    CASE
      WHEN b.other_tags IS NULL OR TRIM(b.other_tags) = '' THEN NULL
      ELSE from_json(
        concat('{', replace(b.other_tags, '"=>"', '":"'), '}'),
        'map<string,string>'
      )
    END AS tags
  FROM geo_databricks_sub.bronze.osm_buildings_activity b
  WHERE b.geom_wkb IS NOT NULL
),
base AS (
  SELECT
    COALESCE(
      CASE WHEN osm_id     IS NOT NULL AND CAST(osm_id AS STRING) <> '' THEN concat('N', CAST(osm_id AS STRING)) END,
      CASE WHEN osm_way_id IS NOT NULL AND CAST(osm_way_id AS STRING) <> '' THEN concat('W', CAST(osm_way_id AS STRING)) END
    ) AS feature_id,

    CAST(NULLIF(CAST(osm_id AS STRING), '') AS STRING)     AS osm_id,
    CAST(NULLIF(CAST(osm_way_id AS STRING), '') AS STRING) AS osm_way_id,

    CAST(NULLIF(name, '') AS STRING)                       AS name,
    CAST(tags['name:en'] AS STRING)                        AS name_en,

    CASE
      WHEN COALESCE(NULLIF(amenity,''),  tags['amenity'])  IS NOT NULL THEN 'amenity'
      WHEN COALESCE(NULLIF(shop,''),     tags['shop'])     IS NOT NULL THEN 'shop'
      WHEN COALESCE(NULLIF(office,''),   tags['office'])   IS NOT NULL THEN 'office'
      WHEN COALESCE(NULLIF(tourism,''),  tags['tourism'])  IS NOT NULL THEN 'tourism'
      WHEN COALESCE(NULLIF(leisure,''),  tags['leisure'])  IS NOT NULL THEN 'leisure'
      WHEN COALESCE(NULLIF(sport,''),    tags['sport'])    IS NOT NULL THEN 'sport'
      WHEN COALESCE(NULLIF(craft,''),    tags['craft'])    IS NOT NULL THEN 'craft'
      WHEN COALESCE(NULLIF(building,''), tags['building']) IS NOT NULL THEN 'building'
      ELSE NULL
    END AS activity_class,

    COALESCE(
      NULLIF(amenity,''),  tags['amenity'],
      NULLIF(shop,''),     tags['shop'],
      NULLIF(office,''),   tags['office'],
      NULLIF(tourism,''),  tags['tourism'],
      NULLIF(leisure,''),  tags['leisure'],
      NULLIF(sport,''),    tags['sport'],
      NULLIF(craft,''),    tags['craft'],
      NULLIF(building,''), tags['building']
    ) AS activity_type,

    LOWER(
      COALESCE(
        NULLIF(amenity,''),  tags['amenity'],
        NULLIF(shop,''),     tags['shop'],
        NULLIF(office,''),   tags['office'],
        NULLIF(tourism,''),  tags['tourism'],
        NULLIF(leisure,''),  tags['leisure'],
        NULLIF(sport,''),    tags['sport'],
        NULLIF(craft,''),    tags['craft'],
        NULLIF(building,''), tags['building']
      )
    ) AS activity_type_lc,

    TRY_CAST(tags['building:levels'] AS INT) AS building_levels,

    COALESCE(tags['operator'], tags['network'], tags['brand']) AS operator_name,
    tags['opening_hours'] AS opening_hours,

    ST_GeomFromWKB(geom_wkb)            AS geom,
    ST_AsText(ST_GeomFromWKB(geom_wkb)) AS geom_wkt_4326,

    CAST(country AS STRING) AS region_code,
    CAST(region  AS STRING) AS region,

    CAST(source_file AS STRING) AS source_file,
    CAST(load_ts AS TIMESTAMP)  AS load_ts,

    tags AS tags,
    CAST(other_tags AS STRING)  AS other_tags_raw
  FROM src
),
filtered AS (
  SELECT *
  FROM base
  WHERE feature_id IS NOT NULL
    AND geom IS NOT NULL
    AND activity_class IS NOT NULL
    AND activity_type_lc IS NOT NULL
    AND NOT (activity_class = 'building' AND activity_type_lc IN (
      'house','detached','apartments','residential','terrace','semidetached_house',
      'bungalow','hut','cabin','roof',
      'outbuilding','farm_auxiliary','barn','shed','sty','stable','garage','garages',
      'greenhouse','allotment_house'
    ))
),
dedup AS (
  SELECT *
  FROM filtered
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY feature_id
    ORDER BY load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.activity_places;
-- SELECT SUM(CASE WHEN geom_wkt_4326 IS NULL THEN 1 ELSE 0 END) AS wkt_null_cnt FROM geo_databricks_sub.silver.activity_places;
-- SELECT activity_class, activity_type_lc, COUNT(*) cnt
-- FROM geo_databricks_sub.silver.activity_places GROUP BY 1,2 ORDER BY cnt DESC LIMIT 50;

-- =============================================================================
-- 13) SILVER.BUILDING_FOOTPRINTS
-- Source : BRONZE.OSM_BUILDINGS_ACTIVITY
-- Target : SILVER.BUILDING_FOOTPRINTS (Delta)
-- Key    : (country, region, feature_id) latest by dt/load_ts/source_file
-- Geo    : geom_wkt_4326 (STRING debug)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.building_footprints
USING DELTA
PARTITIONED BY (country, region)
AS
WITH base AS (
  SELECT
    COALESCE(
      CASE WHEN osm_id IS NOT NULL AND osm_id<>'' THEN CONCAT('N', osm_id) END,
      CASE WHEN osm_way_id IS NOT NULL AND osm_way_id<>'' THEN CONCAT('W', osm_way_id) END
    ) AS feature_id,
    NULLIF(osm_id,'') AS osm_id,
    NULLIF(osm_way_id,'') AS osm_way_id,

    COALESCE(NULLIF(building,''), NULLIF(TRIM(regexp_extract(other_tags, '\\"building\\"=>\\"([^\\"]*)\\"', 1)), '')) AS building_type,
    TRY_CAST(NULLIF(TRIM(regexp_extract(other_tags, '\\"building:levels\\"=>\\"([^\\"]*)\\"', 1)), '') AS INT) AS building_levels,

    geom_wkb,
    ST_AsText(ST_GeomFromWKB(geom_wkb)) AS geom_wkt_4326,
    country, region, dt, source_file, load_ts
  FROM geo_databricks_sub.bronze.osm_buildings_activity
  WHERE geom_wkb IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  WHERE feature_id IS NOT NULL
    AND geom_wkt_4326 IS NOT NULL
    AND building_type IS NOT NULL
    AND LOWER(building_type) <> 'no'
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY country, region, feature_id
    ORDER BY dt DESC, load_ts DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.building_footprints;
-- SELECT building_type, COUNT(*) cnt FROM geo_databricks_sub.silver.building_footprints GROUP BY 1 ORDER BY cnt DESC LIMIT 50;
-- SELECT substring(geom_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.building_footprints LIMIT 20;

-- =============================================================================
-- 14) SILVER.BUILDING_FOOTPRINTS_MODEL (filtered subset for ML)
-- Source : SILVER.BUILDING_FOOTPRINTS
-- Target : SILVER.BUILDING_FOOTPRINTS_MODEL (Delta)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.building_footprints_model
USING DELTA
PARTITIONED BY (country, region)
AS
SELECT *
FROM geo_databricks_sub.silver.building_footprints
WHERE LOWER(building_type) NOT IN (
  'yes','outbuilding','farm_auxiliary','shed','barn','sty','stable',
  'garage','garages','roof','greenhouse','allotment_house','hut','cabin'
);

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.building_footprints_model;
-- SELECT building_type, COUNT(*) cnt FROM geo_databricks_sub.silver.building_footprints_model GROUP BY 1 ORDER BY cnt DESC LIMIT 50;

-- =============================================================================
-- 15) SILVER.DIM_H3_R10_CELLS
-- Source : multiple SILVER tables (points/centroids) + H3
-- Target : SILVER.DIM_H3_R10_CELLS (Delta)
-- Key    : (region_code, region, h3_r10)
-- Geo    : cell_wkt_4326, cell_area_m2, cell_center_wkt_4326
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.dim_h3_r10_cells
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH all_h3 AS (
  SELECT country, region,
         CASE WHEN geom_wkb IS NOT NULL THEN h3_pointash3string(geom_wkb, 10)
              ELSE h3_pointash3string(geom_wkt_4326, 10) END AS h3_r10
  FROM geo_databricks_sub.silver.ev_chargers
  WHERE geom_wkb IS NOT NULL OR geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT country, region,
         CASE WHEN geom_wkb IS NOT NULL THEN h3_pointash3string(geom_wkb, 10)
              ELSE h3_pointash3string(geom_wkt_4326, 10) END AS h3_r10
  FROM geo_databricks_sub.silver.poi_points
  WHERE geom_wkb IS NOT NULL OR geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT country, region,
         CASE WHEN geom_wkb IS NOT NULL THEN h3_pointash3string(geom_wkb, 10)
              ELSE h3_pointash3string(geom_wkt_4326, 10) END AS h3_r10
  FROM geo_databricks_sub.silver.transit_points
  WHERE geom_wkb IS NOT NULL OR geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT country, region,
         h3_pointash3string(ST_AsText(ST_Centroid(ST_GeomFromWKT(geom_wkt_4326))), 10) AS h3_r10
  FROM geo_databricks_sub.silver.road_segments
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT country, region,
         h3_pointash3string(ST_AsText(ST_Centroid(ST_GeomFromWKT(geom_wkt_4326))), 10) AS h3_r10
  FROM geo_databricks_sub.silver.poi_areas
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT country, region,
         h3_pointash3string(ST_AsText(ST_Centroid(ST_GeomFromWKT(geom_wkt_4326))), 10) AS h3_r10
  FROM geo_databricks_sub.silver.transit_lines
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT region_code AS country, region,
         h3_pointash3string(ST_AsText(ST_Centroid(ST_GeomFromWKT(geom_wkt_4326))), 10) AS h3_r10
  FROM geo_databricks_sub.silver.activity_places
  WHERE geom_wkt_4326 IS NOT NULL
),
distinct_h3 AS (
  SELECT DISTINCT country, region, h3_r10
  FROM all_h3
  WHERE h3_r10 IS NOT NULL
)
SELECT
  country AS region_code,
  region,
  h3_r10,
  h3_boundaryaswkt(h3_stringtoh3(h3_r10)) AS cell_wkt_4326,
  ST_Area(ST_GeomFromWKT(h3_boundaryaswkt(h3_stringtoh3(h3_r10)))) AS cell_area_m2,
  h3_centeraswkt(h3_stringtoh3(h3_r10))   AS cell_center_wkt_4326
FROM distinct_h3;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.dim_h3_r10_cells;
-- SELECT region_code, region, COUNT(*) AS h3_cells_cnt FROM geo_databricks_sub.silver.dim_h3_r10_cells GROUP BY 1,2 ORDER BY h3_cells_cnt DESC;
-- SELECT region_code, region, h3_r10, substring(cell_wkt_4326,1,120) AS wkt_prefix FROM geo_databricks_sub.silver.dim_h3_r10_cells LIMIT 20;

-- =============================================================================
-- 16) SILVER.CENSUS_GRID_2021_ADMIN4 (grid -> admin4 + h3)
-- Source : SILVER.CENSUS_GRID_2021_EUROPE + SILVER.ADMIN_AREAS (admin_level=4)
-- Target : SILVER.CENSUS_GRID_2021_ADMIN4 (Delta)
-- Key    : grid_id (plus admin4 match)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.census_grid_2021_admin4
USING DELTA
AS
WITH admin4 AS (
  SELECT
    CAST(osm_id AS STRING)  AS admin4_osm_id,
    CAST(name AS STRING)    AS admin4_name,
    CAST(region_code AS STRING) AS admin4_region_code,
    ST_GeomFromWKT(geom_wkt_4326) AS admin4_geom
  FROM geo_databricks_sub.silver.admin_areas
  WHERE admin_level = 4 AND boundary = 'administrative' AND geom_wkt_4326 IS NOT NULL
),
grid_src AS (
  SELECT
    CAST(grd_id AS STRING) AS grid_id,

    NULLIF(t, -9999)      AS pop_total,
    NULLIF(m, -9999)      AS pop_male,
    NULLIF(f, -9999)      AS pop_female,
    NULLIF(y_lt15, -9999) AS pop_age_lt15,
    NULLIF(y_1564, -9999) AS pop_age_1564,
    NULLIF(y_ge65, -9999) AS pop_age_ge65,
    NULLIF(emp, -9999)    AS emp_total,

    NULLIF(nat, -9999)    AS nat,
    NULLIF(eu_oth, -9999) AS eu_oth,
    NULLIF(oth, -9999)    AS oth,
    NULLIF(same, -9999)   AS same,

    NULLIF(chg_in, -9999)  AS chg_in,
    NULLIF(chg_out, -9999) AS chg_out,

    CAST(land_surface AS DOUBLE) AS land_surface,
    NULLIF(populated, -9999)     AS populated,

    ST_GeomFromWKT(geom_wkt) AS cell_geom,
    ST_Centroid(ST_GeomFromWKT(geom_wkt)) AS cell_pt,
    geom_wkt AS cell_wkt_4326,

    source_file,
    CAST(load_ts AS TIMESTAMP) AS load_ts
  FROM geo_databricks_sub.bronze.census_grid_2021_europe
  WHERE geom_wkt IS NOT NULL
    AND NULLIF(t, -9999) IS NOT NULL
),
joined AS (
  SELECT
    g.*,
    a.admin4_osm_id,
    a.admin4_name,
    a.admin4_region_code,
    h3_pointash3string(ST_AsText(g.cell_pt), 10) AS h3_r10
  FROM grid_src g
  JOIN admin4 a
    ON ST_Contains(a.admin4_geom, g.cell_pt)
)
SELECT * FROM joined;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.census_grid_2021_admin4;
-- SELECT SUM(CASE WHEN admin4_osm_id IS NULL THEN 1 ELSE 0 END) AS admin4_null_cnt FROM geo_databricks_sub.silver.census_grid_2021_admin4;
-- SELECT grid_id, admin4_name, admin4_region_code, substring(cell_wkt_4326,1,120) AS wkt_prefix, h3_r10
-- FROM geo_databricks_sub.silver.census_grid_2021_admin4 LIMIT 20;

-- =============================================================================
-- 17) SILVER.FEAT_H3_POP_R10
-- Source : SILVER.CENSUS_GRID_2021_ADMIN4
-- Target : SILVER.FEAT_H3_POP_R10 (Delta)
-- Key    : (region_code, admin4_osm_id, h3_r10)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.feat_h3_pop_r10
USING DELTA
PARTITIONED BY (region_code)
AS
WITH g AS (
  SELECT
    admin4_region_code AS region_code,
    admin4_osm_id,
    admin4_name,

    cell_geom,
    pop_total, pop_male, pop_female,
    pop_age_lt15, pop_age_1564, pop_age_ge65,
    emp_total,
    load_ts,

    h3_polyfillash3(ST_AsBinary(cell_geom), 10) AS h3_arr
  FROM geo_databricks_sub.silver.census_grid_2021_admin4
  WHERE cell_geom IS NOT NULL
),
exploded AS (
  SELECT
    region_code, admin4_osm_id, admin4_name,
    pop_total, pop_male, pop_female,
    pop_age_lt15, pop_age_1564, pop_age_ge65,
    emp_total, load_ts,
    size(h3_arr) AS h3_cnt,
    h3_r10
  FROM g
  LATERAL VIEW explode(h3_arr) e AS h3_r10
),
weighted AS (
  SELECT
    region_code, admin4_osm_id, admin4_name, h3_r10,
    pop_total    / NULLIF(h3_cnt, 0) AS pop_total,
    pop_male     / NULLIF(h3_cnt, 0) AS pop_male,
    pop_female   / NULLIF(h3_cnt, 0) AS pop_female,
    pop_age_lt15 / NULLIF(h3_cnt, 0) AS pop_age_lt15,
    pop_age_1564 / NULLIF(h3_cnt, 0) AS pop_age_1564,
    pop_age_ge65 / NULLIF(h3_cnt, 0) AS pop_age_ge65,
    emp_total    / NULLIF(h3_cnt, 0) AS emp_total,
    1 AS grid_cells_cnt_piece,
    load_ts
  FROM exploded
),
agg AS (
  SELECT
    region_code, admin4_osm_id, admin4_name, h3_r10,

    SUM(pop_total)    AS pop_total,
    SUM(pop_male)     AS pop_male,
    SUM(pop_female)   AS pop_female,
    SUM(pop_age_lt15) AS pop_age_lt15,
    SUM(pop_age_1564) AS pop_age_1564,
    SUM(pop_age_ge65) AS pop_age_ge65,
    SUM(emp_total)    AS emp_total,

    CASE WHEN SUM(pop_total) > 0 THEN SUM(pop_age_ge65)/SUM(pop_total) END AS share_age_ge65,
    CASE WHEN SUM(pop_total) > 0 THEN SUM(pop_age_lt15)/SUM(pop_total) END AS share_age_lt15,
    CASE WHEN SUM(pop_total) > 0 THEN SUM(emp_total)/SUM(pop_total) END     AS share_emp,

    SUM(grid_cells_cnt_piece) AS grid_cells_cnt,
    MAX(load_ts) AS last_load_ts
  FROM weighted
  GROUP BY 1,2,3,4
)
SELECT
  a.region_code,
  a.h3_r10,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

  a.admin4_osm_id,
  a.admin4_name,

  a.pop_total, a.pop_male, a.pop_female,
  a.pop_age_lt15, a.pop_age_1564, a.pop_age_ge65,
  a.emp_total,

  a.share_age_ge65, a.share_age_lt15, a.share_emp,
  a.grid_cells_cnt,
  a.last_load_ts
FROM agg a
JOIN geo_databricks_sub.silver.dim_h3_r10_cells d
  ON d.region_code = a.region_code AND d.h3_r10 = a.h3_r10;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.feat_h3_pop_r10;
-- SELECT COUNT(*) AS bad_share_cnt FROM geo_databricks_sub.silver.feat_h3_pop_r10
-- WHERE (share_age_ge65 IS NOT NULL AND (share_age_ge65<0 OR share_age_ge65>1))
--    OR (share_age_lt15 IS NOT NULL AND (share_age_lt15<0 OR share_age_lt15>1))
--    OR (share_emp      IS NOT NULL AND (share_emp<0 OR share_emp>1));
-- SELECT substring(cell_wkt_4326,1,120) AS cell_wkt_prefix, substring(cell_center_wkt_4326,1,120) AS center_wkt_prefix
-- FROM geo_databricks_sub.silver.feat_h3_pop_r10 LIMIT 20;

-- =============================================================================
-- 18) SILVER.FEAT_H3_EV_CHARGERS_R10
-- Source : SILVER.EV_CHARGERS + SILVER.DIM_H3_R10_CELLS
-- Target : SILVER.FEAT_H3_EV_CHARGERS_R10 (Delta)
-- Key    : (region_code, h3_r10)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.feat_h3_ev_chargers_r10
USING DELTA
PARTITIONED BY (region_code)
AS
WITH src AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CASE
      WHEN geom_wkb IS NOT NULL THEN h3_pointash3string(geom_wkb, 10)
      ELSE h3_pointash3string(geom_wkt_4326, 10)
    END AS h3_r10,
    CAST(total_sockets_cnt AS INT) AS total_sockets_cnt,
    CAST(has_dc AS BOOLEAN)        AS has_dc,
    CAST(has_ac AS BOOLEAN)        AS has_ac,
    load_ts
  FROM geo_databricks_sub.silver.ev_chargers
  WHERE geom_wkb IS NOT NULL OR geom_wkt_4326 IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,
    COUNT(*) AS chargers_cnt,
    SUM(COALESCE(total_sockets_cnt, 0)) AS sockets_cnt_sum,
    SUM(CASE WHEN has_dc THEN 1 ELSE 0 END) AS chargers_dc_cnt,
    SUM(CASE WHEN has_ac THEN 1 ELSE 0 END) AS chargers_ac_cnt,
    MAX(load_ts) AS last_load_ts
  FROM src
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
)
SELECT
  c.region_code,
  c.region,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  COALESCE(a.chargers_cnt, 0)      AS chargers_cnt,
  COALESCE(a.sockets_cnt_sum, 0)   AS sockets_cnt_sum,
  COALESCE(a.chargers_dc_cnt, 0)   AS chargers_dc_cnt,
  COALESCE(a.chargers_ac_cnt, 0)   AS chargers_ac_cnt,

  a.last_load_ts
FROM geo_databricks_sub.silver.dim_h3_r10_cells c
LEFT JOIN agg a
  ON a.region_code = c.region_code AND a.h3_r10 = c.h3_r10;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.feat_h3_ev_chargers_r10;
-- SELECT SUM(CASE WHEN cell_wkt_4326 IS NULL THEN 1 ELSE 0 END) AS cell_wkt_null_cnt FROM geo_databricks_sub.silver.feat_h3_ev_chargers_r10;

-- =============================================================================
-- 19) SILVER.FEAT_H3_ROADS_R10
-- Source : SILVER.ROAD_SEGMENTS + SILVER.DIM_H3_R10_CELLS
-- Target : SILVER.FEAT_H3_ROADS_R10 (Delta)
-- Key    : (region_code, region, h3_r10)
-- Notes  : length meters from GEOGRAPHY (ST_GeogFromWKT)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.feat_h3_roads_r10
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH src AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region AS STRING)      AS region,
    h3_pointash3string(ST_AsText(ST_Centroid(ST_GeomFromWKT(geom_wkt_4326))), 10) AS h3_r10,
    LOWER(CAST(highway AS STRING)) AS highway_lc,
    CAST(ST_Length(ST_GeogFromWKT(geom_wkt_4326)) AS DOUBLE) AS len_m,
    CAST(oneway AS BOOLEAN) AS oneway,
    CAST(lanes  AS INT)     AS lanes,
    CAST(maxspeed_kph AS DOUBLE) AS maxspeed_kph,
    load_ts
  FROM geo_databricks_sub.silver.road_segments
  WHERE geom_wkt_4326 IS NOT NULL
),
agg AS (
  SELECT
    region_code, region, h3_r10,
    SUM(len_m) AS roads_len_m_sum,
    SUM(CASE WHEN highway_lc IN ('motorway','trunk','primary','secondary') THEN len_m ELSE 0 END) AS roads_major_len_m_sum,
    COUNT(*) AS road_segments_cnt,
    AVG(maxspeed_kph) AS maxspeed_avg_kph,
    percentile_approx(maxspeed_kph, 0.5) AS maxspeed_p50_kph,
    AVG(CAST(lanes AS DOUBLE)) AS lanes_avg,
    percentile_approx(lanes, 0.5) AS lanes_p50,
    AVG(CASE WHEN oneway IS NULL THEN NULL WHEN oneway THEN 1.0 ELSE 0.0 END) AS oneway_share,
    MAX(load_ts) AS last_load_ts
  FROM src
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2,3
)
SELECT
  c.region_code,
  c.region,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  a.roads_len_m_sum,
  a.roads_major_len_m_sum,
  a.road_segments_cnt,
  a.maxspeed_avg_kph,
  a.maxspeed_p50_kph,
  a.lanes_avg,
  a.lanes_p50,
  a.oneway_share,
  a.last_load_ts
FROM geo_databricks_sub.silver.dim_h3_r10_cells c
LEFT JOIN agg a
  ON a.region_code = c.region_code AND a.region = c.region AND a.h3_r10 = c.h3_r10;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.feat_h3_roads_r10;
-- SELECT SUM(CASE WHEN roads_len_m_sum < 0 THEN 1 ELSE 0 END) AS negative_len_cnt FROM geo_databricks_sub.silver.feat_h3_roads_r10;

-- =============================================================================
-- 20) SILVER.FEAT_H3_POI_R10
-- Source : SILVER.POI_POINTS + SILVER.POI_AREAS + SILVER.DIM_H3_R10_CELLS
-- Target : SILVER.FEAT_H3_POI_R10 (Delta)
-- Key    : (region_code, h3_r10)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.feat_h3_poi_r10
USING DELTA
PARTITIONED BY (region_code)
AS
WITH pts AS (
  SELECT
    CAST(country AS STRING) AS region_code,
    h3_pointash3string(geom_wkt_4326, 10) AS h3_r10,
    CAST(poi_class AS STRING) AS poi_class,
    load_ts
  FROM geo_databricks_sub.silver.poi_points
  WHERE geom_wkt_4326 IS NOT NULL AND poi_class IS NOT NULL
),
areas AS (
  SELECT
    CAST(country AS STRING) AS region_code,
    h3_pointash3string(ST_AsText(ST_Centroid(ST_GeomFromWKT(geom_wkt_4326))), 10) AS h3_r10,
    CAST(poi_class AS STRING) AS poi_class,
    load_ts
  FROM geo_databricks_sub.silver.poi_areas
  WHERE geom_wkt_4326 IS NOT NULL AND poi_class IS NOT NULL
),
all_poi AS (
  SELECT * FROM pts
  UNION ALL
  SELECT * FROM areas
),
agg AS (
  SELECT
    region_code,
    h3_r10,
    COUNT(*) AS poi_cnt,
    SUM(CASE WHEN poi_class = 'amenity'  THEN 1 ELSE 0 END) AS poi_amenity_cnt,
    SUM(CASE WHEN poi_class = 'shop'     THEN 1 ELSE 0 END) AS poi_shop_cnt,
    SUM(CASE WHEN poi_class = 'tourism'  THEN 1 ELSE 0 END) AS poi_tourism_cnt,
    SUM(CASE WHEN poi_class = 'building' THEN 1 ELSE 0 END) AS poi_building_cnt,
    SUM(CASE WHEN poi_class = 'landuse'  THEN 1 ELSE 0 END) AS poi_landuse_cnt,
    MAX(load_ts) AS last_load_ts
  FROM all_poi
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
)
SELECT
  c.region_code,
  c.region,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  a.poi_cnt,
  a.poi_amenity_cnt,
  a.poi_shop_cnt,
  a.poi_tourism_cnt,
  a.poi_building_cnt,
  a.poi_landuse_cnt,
  a.last_load_ts
FROM geo_databricks_sub.silver.dim_h3_r10_cells c
LEFT JOIN agg a
  ON a.region_code = c.region_code AND a.h3_r10 = c.h3_r10;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.feat_h3_poi_r10;
-- SELECT COUNT(*) AS bad_cnt FROM geo_databricks_sub.silver.feat_h3_poi_r10
-- WHERE poi_cnt < 0 OR poi_amenity_cnt < 0 OR poi_shop_cnt < 0 OR poi_tourism_cnt < 0;

-- =============================================================================
-- 21) SILVER.FEAT_H3_TRANSIT_R10
-- Source : SILVER.TRANSIT_POINTS + SILVER.TRANSIT_LINES + SILVER.DIM_H3_R10_CELLS
-- Target : SILVER.FEAT_H3_TRANSIT_R10 (Delta)
-- Key    : (region_code, h3_r10)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.feat_h3_transit_r10
USING DELTA
PARTITIONED BY (region_code)
AS
WITH tp AS (
  SELECT
    country AS region_code,
    h3_pointash3string(geom_wkt_4326, 10) AS h3_r10,
    load_ts
  FROM geo_databricks_sub.silver.transit_points
  WHERE geom_wkt_4326 IS NOT NULL
),
tl AS (
  SELECT
    country AS region_code,
    h3_pointash3string(ST_AsText(ST_Centroid(ST_GeomFromWKT(geom_wkt_4326))), 10) AS h3_r10,
    ST_Length(ST_Transform(ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326), 3035)) AS len_m,
    load_ts
  FROM geo_databricks_sub.silver.transit_lines
  WHERE geom_wkt_4326 IS NOT NULL
),
agg_points AS (
  SELECT region_code, h3_r10, COUNT(*) AS transit_points_cnt, MAX(load_ts) AS last_load_ts
  FROM tp WHERE h3_r10 IS NOT NULL GROUP BY 1,2
),
agg_lines AS (
  SELECT region_code, h3_r10, SUM(len_m) AS transit_lines_len_m_sum, MAX(load_ts) AS last_load_ts
  FROM tl WHERE h3_r10 IS NOT NULL GROUP BY 1,2
)
SELECT
  c.region_code,
  c.region,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  COALESCE(p.transit_points_cnt, 0) AS transit_points_cnt,
  COALESCE(l.transit_lines_len_m_sum, 0) AS transit_lines_len_m_sum,
  greatest(
    COALESCE(p.last_load_ts, TIMESTAMP('1970-01-01')),
    COALESCE(l.last_load_ts, TIMESTAMP('1970-01-01'))
  ) AS last_load_ts
FROM geo_databricks_sub.silver.dim_h3_r10_cells c
LEFT JOIN agg_points p ON p.region_code = c.region_code AND p.h3_r10 = c.h3_r10
LEFT JOIN agg_lines  l ON l.region_code = c.region_code AND l.h3_r10 = c.h3_r10;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.feat_h3_transit_r10;
-- SELECT min(transit_lines_len_m_sum) AS min_len, approx_percentile(transit_lines_len_m_sum,0.5) AS p50_len, max(transit_lines_len_m_sum) AS max_len
-- FROM geo_databricks_sub.silver.feat_h3_transit_r10;

-- =============================================================================
-- 22) SILVER.FEAT_H3_ACTIVITY_PLACES_R10
-- Source : SILVER.ACTIVITY_PLACES + SILVER.DIM_H3_R10_CELLS
-- Target : SILVER.FEAT_H3_ACTIVITY_PLACES_R10 (Delta)
-- Key    : (region_code, h3_r10)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.feat_h3_activity_places_r10
USING DELTA
PARTITIONED BY (region_code)
AS
WITH src AS (
  SELECT
    region_code,
    h3_pointash3string(
      ST_AsText(ST_Centroid(CASE WHEN geom IS NOT NULL THEN ST_SetSRID(geom, 4326)
                                ELSE ST_SetSRID(ST_GeomFromWKT(geom_wkt_4326), 4326) END)),
      10
    ) AS h3_r10,
    lower(activity_class) AS activity_class,
    lower(activity_type)  AS activity_type,
    load_ts
  FROM geo_databricks_sub.silver.activity_places
  WHERE geom IS NOT NULL OR geom_wkt_4326 IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,
    COUNT(*) AS places_cnt,
    SUM(CASE WHEN activity_class='amenity' THEN 1 ELSE 0 END) AS places_amenity_cnt,
    SUM(CASE WHEN activity_class='shop'    THEN 1 ELSE 0 END) AS places_shop_cnt,
    SUM(CASE WHEN activity_class='tourism' THEN 1 ELSE 0 END) AS places_tourism_cnt,
    SUM(CASE WHEN activity_class='office'  THEN 1 ELSE 0 END) AS places_office_cnt,
    SUM(CASE WHEN activity_class='leisure' THEN 1 ELSE 0 END) AS places_leisure_cnt,
    SUM(CASE WHEN activity_class='sport'   THEN 1 ELSE 0 END) AS places_sport_cnt,
    SUM(CASE WHEN activity_type IN ('parking','fuel','charging_station') THEN 1 ELSE 0 END) AS places_mobility_cnt,
    SUM(CASE WHEN activity_type IN ('supermarket','mall') THEN 1 ELSE 0 END) AS places_retail_big_cnt,
    SUM(CASE WHEN activity_type IN ('restaurant','fast_food','cafe') THEN 1 ELSE 0 END) AS places_food_cnt,
    SUM(CASE WHEN activity_type IN ('hotel','guest_house','hostel') THEN 1 ELSE 0 END) AS places_stay_cnt,
    MAX(load_ts) AS last_load_ts
  FROM src
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
)
SELECT
  c.region_code,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  a.places_cnt,
  a.places_amenity_cnt,
  a.places_shop_cnt,
  a.places_tourism_cnt,
  a.places_office_cnt,
  a.places_leisure_cnt,
  a.places_sport_cnt,
  a.places_mobility_cnt,
  a.places_retail_big_cnt,
  a.places_food_cnt,
  a.places_stay_cnt,
  a.last_load_ts
FROM geo_databricks_sub.silver.dim_h3_r10_cells c
LEFT JOIN agg a
  ON a.region_code = c.region_code AND a.h3_r10 = c.h3_r10;

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.feat_h3_activity_places_r10;
-- SELECT COUNT(*) AS bad_rows FROM geo_databricks_sub.silver.feat_h3_activity_places_r10
-- WHERE places_cnt < 0 OR places_amenity_cnt < 0 OR places_shop_cnt < 0;

-- =============================================================================
-- 23) SILVER.FEAT_H3_DEGURBA_R10
-- Source : SILVER.DIM_H3_R10_CELLS + SILVER.LAU_DEGURBA
-- Target : SILVER.FEAT_H3_DEGURBA_R10 (Delta)
-- Key    : (region_code, h3_r10, year)
-- =============================================================================
CREATE OR REPLACE TABLE geo_databricks_sub.silver.feat_h3_degurba_r10
USING DELTA
PARTITIONED BY (region_code)
AS
WITH h AS (
  SELECT
    region_code,
    h3_r10,
    st_setsrid(ST_GeomFromWKT(cell_center_wkt_4326), 4326) AS cell_center_geom
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
  WHERE cell_center_wkt_4326 IS NOT NULL
),
lau AS (
  SELECT
    region_code, lau_id, lau_name, degurba, year, geom AS lau_geom, load_ts
  FROM geo_databricks_sub.silver.lau_degurba
  WHERE geom IS NOT NULL
)
SELECT
  h.region_code,
  h.h3_r10,
  l.year,
  l.degurba,
  l.lau_id,
  l.lau_name,
  l.load_ts AS last_load_ts
FROM h
JOIN lau l
  ON h.region_code = l.region_code
 AND ST_Contains(l.lau_geom, h.cell_center_geom);

-- -- checks
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.silver.feat_h3_degurba_r10;
-- SELECT COUNT(*) AS dup_cnt FROM (
--   SELECT region_code, h3_r10, year, COUNT(*) c
--   FROM geo_databricks_sub.silver.feat_h3_degurba_r10
--   GROUP BY 1,2,3 HAVING COUNT(*)>1
-- ) t;

-- =============================================================================
-- 99) SUMMARY CHECKS (optional, commented)
-- =============================================================================
-- SELECT 'admin_areas' t, COUNT(*) c FROM geo_databricks_sub.silver.admin_areas
-- UNION ALL SELECT 'ev_chargers', COUNT(*) FROM geo_databricks_sub.silver.ev_chargers
-- UNION ALL SELECT 'poi_points', COUNT(*) FROM geo_databricks_sub.silver.poi_points
-- UNION ALL SELECT 'poi_areas', COUNT(*) FROM geo_databricks_sub.silver.poi_areas
-- UNION ALL SELECT 'transit_points', COUNT(*) FROM geo_databricks_sub.silver.transit_points
-- UNION ALL SELECT 'transit_lines', COUNT(*) FROM geo_databricks_sub.silver.transit_lines
-- UNION ALL SELECT 'road_segments', COUNT(*) FROM geo_databricks_sub.silver.road_segments
-- UNION ALL SELECT 'activity_places', COUNT(*) FROM geo_databricks_sub.silver.activity_places
-- UNION ALL SELECT 'building_footprints', COUNT(*) FROM geo_databricks_sub.silver.building_footprints
-- UNION ALL SELECT 'building_footprints_model', COUNT(*) FROM geo_databricks_sub.silver.building_footprints_model
-- UNION ALL SELECT 'dim_h3_r10_cells', COUNT(*) FROM geo_databricks_sub.silver.dim_h3_r10_cells
-- UNION ALL SELECT 'feat_h3_pop_r10', COUNT(*) FROM geo_databricks_sub.silver.feat_h3_pop_r10
-- UNION ALL SELECT 'feat_h3_ev_chargers_r10', COUNT(*) FROM geo_databricks_sub.silver.feat_h3_ev_chargers_r10
-- UNION ALL SELECT 'feat_h3_roads_r10', COUNT(*) FROM geo_databricks_sub.silver.feat_h3_roads_r10
-- UNION ALL SELECT 'feat_h3_poi_r10', COUNT(*) FROM geo_databricks_sub.silver.feat_h3_poi_r10
-- UNION ALL SELECT 'feat_h3_transit_r10', COUNT(*) FROM geo_databricks_sub.silver.feat_h3_transit_r10
-- UNION ALL SELECT 'feat_h3_activity_places_r10', COUNT(*) FROM geo_databricks_sub.silver.feat_h3_activity_places_r10
-- UNION ALL SELECT 'feat_h3_degurba_r10', COUNT(*) FROM geo_databricks_sub.silver.feat_h3_degurba_r10
-- UNION ALL SELECT 'gisco_nuts', COUNT(*) FROM geo_databricks_sub.silver.gisco_nuts
-- UNION ALL SELECT 'eurostat_tran_r_elvehst', COUNT(*) FROM geo_databricks_sub.silver.eurostat_tran_r_elvehst
-- UNION ALL SELECT 'lau_degurba', COUNT(*) FROM geo_databricks_sub.silver.lau_degurba
-- UNION ALL SELECT 'census_grid_2021_europe', COUNT(*) FROM geo_databricks_sub.silver.census_grid_2021_europe
-- UNION ALL SELECT 'census_grid_2021_admin4', COUNT(*) FROM geo_databricks_sub.silver.census_grid_2021_admin4
-- ;

-- =============================================================================
-- 100) OPTIMIZE / ZORDER / ANALYZE
-- NOTE: ZORDER BY НЕ делаем по partition columns (как ты просил).
-- =============================================================================

-- ANALYZE (stats)
ANALYZE TABLE geo_databricks_sub.silver.dim_h3_r10_cells COMPUTE STATISTICS FOR COLUMNS region_code, region, h3_r10;
ANALYZE TABLE geo_databricks_sub.silver.ev_chargers     COMPUTE STATISTICS FOR COLUMNS country, region, osm_id, load_ts;
ANALYZE TABLE geo_databricks_sub.silver.road_segments   COMPUTE STATISTICS FOR COLUMNS country, region, osm_id, highway, load_ts;
ANALYZE TABLE geo_databricks_sub.silver.poi_points      COMPUTE STATISTICS FOR COLUMNS country, region, osm_id, poi_class, load_ts;
ANALYZE TABLE geo_databricks_sub.silver.poi_areas       COMPUTE STATISTICS FOR COLUMNS country, region, feature_id, poi_class, load_ts;
ANALYZE TABLE geo_databricks_sub.silver.transit_points  COMPUTE STATISTICS FOR COLUMNS country, region, osm_id, load_ts;
ANALYZE TABLE geo_databricks_sub.silver.transit_lines   COMPUTE STATISTICS FOR COLUMNS country, region, osm_id, load_ts;
ANALYZE TABLE geo_databricks_sub.silver.activity_places COMPUTE STATISTICS FOR COLUMNS region_code, feature_id, activity_class, activity_type, load_ts;
ANALYZE TABLE geo_databricks_sub.silver.building_footprints_model COMPUTE STATISTICS FOR COLUMNS country, region, feature_id, building_type, load_ts;
ANALYZE TABLE geo_databricks_sub.silver.lau_degurba     COMPUTE STATISTICS FOR COLUMNS cntr_code, region_code, year, lau_id, degurba;
ANALYZE TABLE geo_databricks_sub.silver.gisco_nuts      COMPUTE STATISTICS FOR COLUMNS cntr_code, nuts_id, level, year, scale, crs;

ANALYZE TABLE geo_databricks_sub.silver.feat_h3_pop_r10           COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10, last_load_ts;
ANALYZE TABLE geo_databricks_sub.silver.feat_h3_ev_chargers_r10   COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10, last_load_ts;
ANALYZE TABLE geo_databricks_sub.silver.feat_h3_roads_r10         COMPUTE STATISTICS FOR COLUMNS region_code, region, h3_r10, last_load_ts;
ANALYZE TABLE geo_databricks_sub.silver.feat_h3_poi_r10           COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10, last_load_ts;
ANALYZE TABLE geo_databricks_sub.silver.feat_h3_transit_r10       COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10, last_load_ts;
ANALYZE TABLE geo_databricks_sub.silver.feat_h3_activity_places_r10 COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10, last_load_ts;
ANALYZE TABLE geo_databricks_sub.silver.feat_h3_degurba_r10       COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10, year, degurba;

-- OPTIMIZE + ZORDER (не по partition columns)
OPTIMIZE geo_databricks_sub.silver.dim_h3_r10_cells ZORDER BY (h3_r10);
OPTIMIZE geo_databricks_sub.silver.ev_chargers     ZORDER BY (osm_id, load_ts);
OPTIMIZE geo_databricks_sub.silver.road_segments   ZORDER BY (osm_id, load_ts);
OPTIMIZE geo_databricks_sub.silver.poi_points      ZORDER BY (osm_id, load_ts);
OPTIMIZE geo_databricks_sub.silver.poi_areas       ZORDER BY (feature_id, load_ts);
OPTIMIZE geo_databricks_sub.silver.transit_points  ZORDER BY (osm_id, load_ts);
OPTIMIZE geo_databricks_sub.silver.transit_lines   ZORDER BY (osm_id, load_ts);
OPTIMIZE geo_databricks_sub.silver.activity_places ZORDER BY (feature_id, load_ts);
OPTIMIZE geo_databricks_sub.silver.building_footprints_model ZORDER BY (feature_id, load_ts);
OPTIMIZE geo_databricks_sub.silver.lau_degurba     ZORDER BY (lau_id, degurba);
OPTIMIZE geo_databricks_sub.silver.gisco_nuts      ZORDER BY (cntr_code, nuts_id);
OPTIMIZE geo_databricks_sub.silver.feat_h3_pop_r10           ZORDER BY (h3_r10, last_load_ts);
OPTIMIZE geo_databricks_sub.silver.feat_h3_ev_chargers_r10   ZORDER BY (h3_r10, last_load_ts);
OPTIMIZE geo_databricks_sub.silver.feat_h3_roads_r10         ZORDER BY (h3_r10, last_load_ts);
OPTIMIZE geo_databricks_sub.silver.feat_h3_poi_r10           ZORDER BY (h3_r10, last_load_ts);
OPTIMIZE geo_databricks_sub.silver.feat_h3_transit_r10       ZORDER BY (h3_r10, last_load_ts);
OPTIMIZE geo_databricks_sub.silver.feat_h3_activity_places_r10 ZORDER BY (h3_r10, last_load_ts);
OPTIMIZE geo_databricks_sub.silver.feat_h3_degurba_r10       ZORDER BY (h3_r10, year);