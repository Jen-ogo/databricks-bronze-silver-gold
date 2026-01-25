/* =============================================================================
   GEO_PROJECT — SILVER (country-agnostic) bootstrap
   Platform : Snowflake
   Database : GEO_PROJECT
   Schema   : SILVER
   Storage  : Dynamic Tables (TARGET_LAG = 48 hours)
   Compute  : COMPUTE_WH

   Design goals
   - Country/region agnostic naming and keys
   - Deterministic, idempotent reruns (DROP + CREATE OR REPLACE)
   - Unified columns across OSM/EUROSTAT/GISCO:
       feature_id, region_code, source_file, load_ts
       geog (GEOGRAPHY), geom (optional GEOMETRY), geom_wkt_4326 (WKT STRING)
   - Mandatory geo checks included (rowcount + WKT sample for each geotable)

   Notes
   - This script assumes BRONZE tables already exist and are populated.
   - Dependency order matters (ADMIN_AREAS first; H3 DIM before H3 FEAT tables; etc.).
   - Optional storage optimizations are included as commented statements.
============================================================================= */

-- =============================================================================
-- 0) CONTEXT
-- =============================================================================
USE DATABASE GEO_PROJECT;

CREATE SCHEMA IF NOT EXISTS SILVER;
USE SCHEMA SILVER;

-- Recommended: use a dedicated warehouse for DT refresh if COMPUTE_WH is shared.
-- USE WAREHOUSE COMPUTE_WH;

-- =============================================================================
-- 1) CLEANUP / DROP (idempotent)
--    Order: FEAT -> DIM -> CENSUS -> EUROSTAT/GISCO -> OSM base
-- =============================================================================

-- Feature tables
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_DEGURBA_R10;
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_ACTIVITY_PLACES_R10;
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_TRANSIT_R10;
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_POI_R10;
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_ROADS_R10;
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_EV_CHARGERS_R10;
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_POP_R10;

-- Dimensions
DROP DYNAMIC TABLE IF EXISTS SILVER.DIM_H3_R10_CELLS;

-- Census grid
DROP DYNAMIC TABLE IF EXISTS SILVER.CENSUS_GRID_2021_ADMIN4;

-- Eurostat / Gisco
DROP DYNAMIC TABLE IF EXISTS SILVER.LAU_DEGURBA;
DROP DYNAMIC TABLE IF EXISTS SILVER.GISCO_NUTS;
DROP DYNAMIC TABLE IF EXISTS SILVER.EUROSTAT_TRAN_R_ELVEHST;

-- Buildings
DROP DYNAMIC TABLE IF EXISTS SILVER.BUILDING_FOOTPRINTS_MODEL;
DROP DYNAMIC TABLE IF EXISTS SILVER.BUILDING_FOOTPRINTS;
DROP DYNAMIC TABLE IF EXISTS SILVER.ACTIVITY_PLACES;

-- OSM base
DROP DYNAMIC TABLE IF EXISTS SILVER.ROAD_SEGMENTS;
DROP DYNAMIC TABLE IF EXISTS SILVER.TRANSIT_LINES;
DROP DYNAMIC TABLE IF EXISTS SILVER.TRANSIT_POINTS;
DROP DYNAMIC TABLE IF EXISTS SILVER.POI_AREAS;
DROP DYNAMIC TABLE IF EXISTS SILVER.POI_POINTS;
DROP DYNAMIC TABLE IF EXISTS SILVER.EV_CHARGERS;
DROP DYNAMIC TABLE IF EXISTS SILVER.ADMIN_AREAS;

-- Old/stale names cleanup (safe no-ops)
DROP DYNAMIC TABLE IF EXISTS SILVER.ADMIN;
DROP DYNAMIC TABLE IF EXISTS SILVER.CHARGING;
DROP DYNAMIC TABLE IF EXISTS SILVER.POI_POLYGONS;
DROP DYNAMIC TABLE IF EXISTS SILVER.PT_POINTS;
DROP DYNAMIC TABLE IF EXISTS SILVER.PT_LINES;
DROP DYNAMIC TABLE IF EXISTS SILVER.ROADS;
DROP DYNAMIC TABLE IF EXISTS SILVER.BUILDINGS_ACTIVITY_PLACES;
DROP DYNAMIC TABLE IF EXISTS SILVER.CENSUS_GRID_2021_PL_L4;
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_CHARGING_R10;
DROP DYNAMIC TABLE IF EXISTS SILVER.FEAT_H3_PT_R10;

-- =============================================================================
-- 2) BASE SILVER TABLES (OSM core)
-- =============================================================================

/* -----------------------------------------------------------------------------
   2.1) SILVER.ADMIN_AREAS
   Source : BRONZE.ADMIN
   Grain  : latest row per osm_id
   Geo    : geog (GEOGRAPHY), geom (GEOMETRY optional), geom_wkt_4326 (WKT STRING)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.ADMIN_AREAS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  -- Optional: cluster for common filters (admin_level/boundary/region_code)
  CLUSTER BY (region_code, admin_level, boundary)
AS
WITH src AS (
  SELECT
    b.*,
    /* Parse OSM "other_tags" (hstore-like) into JSON if present */
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags
  FROM BRONZE.ADMIN b
  WHERE b.osm_id IS NOT NULL
),
typed AS (
  SELECT
    ('A:' || osm_id::STRING)                 AS feature_id,
    osm_id::STRING                           AS osm_id,

    NULLIF(name,'')::STRING                  AS name,
    tags:"name:en"::STRING                   AS name_en,

    admin_level::INT                         AS admin_level,
    boundary::STRING                         AS boundary,
    type::STRING                             AS type,

    TRY_TO_NUMBER(tags:"population"::STRING)      AS population,
    TRY_TO_DATE(tags:"population:date"::STRING)   AS population_date,

    tags                                     AS tags,
    other_tags::STRING                       AS other_tags_raw,

    TRY_TO_GEOGRAPHY(geom_wkt)               AS geog0,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))     AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))) AS geom,

    region::STRING                           AS region_code,
    source_file::STRING                      AS source_file,
    load_ts::TIMESTAMP_NTZ                   AS load_ts
  FROM src
)
SELECT *
FROM typed
WHERE geog0 IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY osm_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.2) SILVER.EV_CHARGERS
   Source : BRONZE.CHARGING
   Grain  : latest row per osm_id
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.EV_CHARGERS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code)
AS
WITH src AS (
  SELECT
    b.*,
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags
  FROM BRONZE.CHARGING b
  WHERE b.osm_id IS NOT NULL
),
typed AS (
  SELECT
    ('EV:' || osm_id::STRING)                AS feature_id,
    osm_id::STRING                           AS osm_id,

    NULLIF(name,'')::STRING                  AS name,
    tags:"name:en"::STRING                   AS name_en,

    tags:"amenity"::STRING                   AS amenity,
    tags:"operator"::STRING                  AS operator,
    tags:"operator:wikidata"::STRING         AS operator_wikidata,
    tags:"opening_hours"::STRING             AS opening_hours,
    tags:"website"::STRING                   AS website,
    tags:"access"::STRING                    AS access,

    IFF(LOWER(tags:"fee"::STRING) IN ('yes','true','1'), TRUE,
        IFF(LOWER(tags:"fee"::STRING) IN ('no','false','0'), FALSE, NULL)
    )                                        AS fee_bool,

    TRY_TO_NUMBER(tags:"capacity"::STRING)   AS capacity,

    TRY_TO_NUMBER(tags:"socket:type2"::STRING)       AS socket_type2_cnt,
    TRY_TO_NUMBER(tags:"socket:chademo"::STRING)     AS socket_chademo_cnt,
    TRY_TO_NUMBER(tags:"socket:type2_combo"::STRING) AS socket_type2_combo_cnt,

    tags:"ref:EU:EVSE"::STRING               AS ref_eu_evse,
    tags:"ref:EU:EVSE:pool"::STRING          AS ref_eu_evse_pool,

    tags:"motorcar"::STRING                  AS motorcar,
    tags:"motorcycle"::STRING                AS motorcycle,

    ref::STRING                              AS ref,

    tags                                     AS tags,
    other_tags::STRING                       AS other_tags_raw,

    TRY_TO_GEOGRAPHY(geom_wkt)               AS geog,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))     AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))) AS geom,

    region::STRING                           AS region_code,
    source_file::STRING                      AS source_file,
    load_ts::TIMESTAMP_NTZ                   AS load_ts
  FROM src
)
SELECT
  *,
  COALESCE(socket_type2_cnt,0)
  + COALESCE(socket_chademo_cnt,0)
  + COALESCE(socket_type2_combo_cnt,0)       AS total_sockets_cnt,
  IFF(COALESCE(socket_chademo_cnt,0) + COALESCE(socket_type2_combo_cnt,0) > 0, TRUE, NULL) AS has_dc,
  IFF(COALESCE(socket_type2_cnt,0) > 0, TRUE, NULL) AS has_ac
FROM typed
WHERE geog IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY osm_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.3) SILVER.POI_POINTS
   Source : BRONZE.POI_POINTS
   Grain  : latest row per osm_id
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.POI_POINTS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, poi_class)
AS
WITH src AS (
  SELECT
    b.*,
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags
  FROM BRONZE.POI_POINTS b
  WHERE b.osm_id IS NOT NULL
),
typed AS (
  SELECT
    ('POIP:' || osm_id::STRING)              AS feature_id,
    osm_id::STRING                           AS osm_id,
    NULLIF(name,'')::STRING                  AS name,

    tags:"addr:housenumber"::STRING          AS addr_housenumber,
    tags:"addr:street"::STRING               AS addr_street,
    tags:"addr:postcode"::STRING             AS addr_postcode,
    COALESCE(tags:"addr:city"::STRING, tags:"addr:place"::STRING) AS addr_city_or_place,

    /* Primary type: pick first available in priority order */
    COALESCE(
      tags:"amenity"::STRING,
      tags:"shop"::STRING,
      tags:"tourism"::STRING,
      tags:"leisure"::STRING,
      tags:"office"::STRING,
      tags:"craft"::STRING,
      tags:"man_made"::STRING,
      tags:"emergency"::STRING,
      tags:"public_transport"::STRING,
      tags:"railway"::STRING,
      tags:"highway"::STRING,
      tags:"place"::STRING
    ) AS poi_type,

    CASE
      WHEN tags:"amenity" IS NOT NULL THEN 'amenity'
      WHEN tags:"shop" IS NOT NULL THEN 'shop'
      WHEN tags:"tourism" IS NOT NULL THEN 'tourism'
      WHEN tags:"leisure" IS NOT NULL THEN 'leisure'
      WHEN tags:"office" IS NOT NULL THEN 'office'
      WHEN tags:"craft" IS NOT NULL THEN 'craft'
      WHEN tags:"man_made" IS NOT NULL THEN 'man_made'
      WHEN tags:"emergency" IS NOT NULL THEN 'emergency'
      WHEN tags:"public_transport" IS NOT NULL THEN 'public_transport'
      WHEN tags:"railway" IS NOT NULL THEN 'railway'
      WHEN tags:"highway" IS NOT NULL THEN 'highway'
      WHEN tags:"place" IS NOT NULL THEN 'place'
      ELSE NULL
    END AS poi_class,

    tags                                     AS tags,
    other_tags::STRING                       AS other_tags_raw,

    TRY_TO_GEOGRAPHY(geom_wkt)               AS geog,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))     AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))) AS geom,

    region::STRING                           AS region_code,
    source_file::STRING                      AS source_file,
    load_ts::TIMESTAMP_NTZ                   AS load_ts
  FROM src
)
SELECT *
FROM typed
WHERE geog IS NOT NULL
  AND poi_class IS NOT NULL
  AND poi_type IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY osm_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.4) SILVER.POI_AREAS
   Source : BRONZE.POI_POLYGONS
   Grain  : latest row per (feature_id) where feature_id is osm_id or osm_way_id
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.POI_AREAS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, poi_class)
AS
WITH src AS (
  SELECT
    b.*,
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags
  FROM BRONZE.POI_POLYGONS b
),
typed AS (
  SELECT
    COALESCE(NULLIF(osm_id,''), 'W' || osm_way_id::STRING)::STRING AS feature_id,
    NULLIF(osm_id,'')::STRING AS osm_id,
    osm_way_id::STRING        AS osm_way_id,

    NULLIF(name,'')::STRING   AS name,

    CASE
      WHEN amenity   IS NOT NULL THEN 'amenity'
      WHEN shop      IS NOT NULL THEN 'shop'
      WHEN tourism   IS NOT NULL THEN 'tourism'
      WHEN office    IS NOT NULL THEN 'office'
      WHEN leisure   IS NOT NULL THEN 'leisure'
      WHEN sport     IS NOT NULL THEN 'sport'
      WHEN building  IS NOT NULL THEN 'building'
      WHEN landuse   IS NOT NULL THEN 'landuse'
      ELSE NULL
    END::STRING AS poi_class,

    COALESCE(amenity, shop, tourism, office, leisure, sport, building, landuse)::STRING AS poi_type,

    TRY_TO_GEOGRAPHY(geom_wkt)               AS geog,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))     AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))) AS geom,

    region::STRING                           AS region_code,
    source_file::STRING                      AS source_file,
    load_ts::TIMESTAMP_NTZ                   AS load_ts,

    tags                                     AS tags,
    other_tags::STRING                       AS other_tags_raw
  FROM src
)
SELECT *
FROM typed
WHERE geog IS NOT NULL
  AND poi_class IS NOT NULL
  AND poi_type IS NOT NULL
  /* Allowlist for EV suitability model (extend anytime) */
  AND (
    (poi_class='amenity' AND poi_type IN (
      'charging_station','fuel','parking','restaurant','cafe','fast_food',
      'toilets','hospital','pharmacy','bank','atm',
      'school','kindergarten','university',
      'police','fire_station','post_office',
      'place_of_worship','cinema','theatre',
      'bus_station','ferry_terminal'
    ))
    OR (poi_class='shop' AND poi_type IN (
      'supermarket','convenience','mall','car_repair',
      'clothes','department_store','bakery','butcher','chemist','electronics',
      'hardware','kiosk','pet','doityourself','car','car_parts'
    ))
    OR (poi_class='tourism' AND poi_type IN (
      'hotel','motel','hostel','guest_house','camp_site','attraction','museum'
    ))
    OR (poi_class='building' AND poi_type IN (
      'retail','commercial','industrial','public',
      'hospital','school','university','transportation'
    ))
    OR (poi_class='landuse' AND poi_type IN ('retail','commercial','industrial'))
  )
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY feature_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.5) SILVER.TRANSIT_POINTS
   Source : BRONZE.PT_POINTS
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.TRANSIT_POINTS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, poi_class)
AS
WITH src AS (
  SELECT
    b.*,
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags
  FROM BRONZE.PT_POINTS b
  WHERE b.osm_id IS NOT NULL
),
typed AS (
  SELECT
    ('TP:' || osm_id::STRING)                AS feature_id,
    osm_id::STRING                           AS osm_id,
    COALESCE(NULLIF(name,''), tags:"name"::STRING) AS name,

    CASE
      WHEN tags:"public_transport" IS NOT NULL
        OR tags:"railway" IS NOT NULL
        OR LOWER(tags:"highway"::STRING) IN ('bus_stop','platform')
      THEN 'transport'
      WHEN tags:"amenity" IS NOT NULL THEN 'amenity'
      WHEN tags:"emergency" IS NOT NULL THEN 'emergency'
      ELSE NULL
    END AS poi_class,

    CASE
      WHEN tags:"public_transport" IS NOT NULL THEN tags:"public_transport"::STRING
      WHEN tags:"railway" IS NOT NULL THEN tags:"railway"::STRING
      WHEN tags:"highway" IS NOT NULL THEN tags:"highway"::STRING
      WHEN tags:"amenity" IS NOT NULL THEN tags:"amenity"::STRING
      WHEN tags:"emergency" IS NOT NULL THEN tags:"emergency"::STRING
      ELSE NULL
    END AS poi_type,

    COALESCE(
      NULLIF(TRIM(tags:"operator"::STRING), ''),
      NULLIF(TRIM(tags:"network"::STRING), ''),
      NULLIF(TRIM(tags:"brand"::STRING), '')
    ) AS provider,

    tags                                     AS tags,
    other_tags::STRING                       AS other_tags_raw,

    TRY_TO_GEOGRAPHY(geom_wkt)               AS geog,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))     AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))) AS geom,

    region::STRING                           AS region_code,
    source_file::STRING                      AS source_file,
    load_ts::TIMESTAMP_NTZ                   AS load_ts
  FROM src
)
SELECT *
FROM typed
WHERE geog IS NOT NULL
  AND poi_class IS NOT NULL
  AND (
    poi_class = 'transport'
    OR (
      poi_class = 'amenity'
      AND LOWER(poi_type) IN (
        'charging_station','fuel','parking',
        'library','school','university','kindergarten',
        'hospital','clinic','doctors','pharmacy',
        'police','fire_station',
        'bank','atm','post_office',
        'bus_station'
      )
    )
  )
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY osm_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.6) SILVER.TRANSIT_LINES
   Source : BRONZE.PT_LINES
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.TRANSIT_LINES
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, line_class)
AS
WITH src AS (
  SELECT
    b.*,
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags
  FROM BRONZE.PT_LINES b
  WHERE b.osm_id IS NOT NULL
),
typed AS (
  SELECT
    ('TL:' || osm_id::STRING)                AS feature_id,
    osm_id::STRING                           AS osm_id,

    CASE
      WHEN railway   IS NOT NULL OR tags:"railway"   IS NOT NULL THEN 'railway'
      WHEN waterway  IS NOT NULL OR tags:"waterway"  IS NOT NULL THEN 'waterway'
      WHEN aerialway IS NOT NULL OR tags:"aerialway" IS NOT NULL THEN 'aerialway'
      ELSE NULL
    END AS line_class,

    COALESCE(
      railway::STRING,  tags:"railway"::STRING,
      waterway::STRING, tags:"waterway"::STRING,
      aerialway::STRING, tags:"aerialway"::STRING
    ) AS line_type,

    COALESCE(tags:"tunnel"::STRING, 'no') AS tunnel_raw,
    COALESCE(
      TRY_TO_NUMBER(tags:"layer"::STRING),
      TRY_TO_NUMBER(REGEXP_SUBSTR(tags:"layer"::STRING, '^-?\\d+')),
      0
    )::INT AS layer,

    tags                                     AS tags,
    other_tags::STRING                       AS other_tags_raw,

    TRY_TO_GEOGRAPHY(geom_wkt)               AS geog,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))     AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))) AS geom,

    region::STRING                           AS region_code,
    source_file::STRING                      AS source_file,
    load_ts::TIMESTAMP_NTZ                   AS load_ts
  FROM src
)
SELECT *
FROM typed
WHERE geog IS NOT NULL
  AND line_class IS NOT NULL
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY osm_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.7) SILVER.ROAD_SEGMENTS
   Source : BRONZE.ROADS
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.ROAD_SEGMENTS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, highway)
AS
WITH src AS (
  SELECT
    b.*,
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags
  FROM BRONZE.ROADS b
  WHERE b.osm_id IS NOT NULL
),
typed AS (
  SELECT
    ('RS:' || osm_id::STRING)                AS feature_id,
    osm_id::STRING                           AS osm_id,
    NULLIF(name,'')::STRING                  AS name,

    highway::STRING                          AS highway,
    tags:"ref"::STRING                       AS ref,

    COALESCE(
      tags:"motorcar"::STRING,
      tags:"motor_vehicle"::STRING,
      tags:"vehicle"::STRING,
      tags:"access"::STRING
    ) AS access_raw,

    tags:"service"::STRING                   AS service,

    LOWER(COALESCE(tags:"oneway"::STRING, 'no')) AS oneway_raw,
    IFF(LOWER(COALESCE(tags:"oneway"::STRING, 'no')) IN ('yes','true','1'), TRUE, FALSE) AS oneway,

    TRY_TO_NUMBER(tags:"lanes"::STRING)::INT  AS lanes,
    tags:"surface"::STRING                   AS surface,

    IFF(LOWER(COALESCE(tags:"lit"::STRING,'no')) IN ('yes','true','1'), TRUE, FALSE) AS lit,
    IFF(LOWER(COALESCE(tags:"bridge"::STRING,'no')) IN ('yes','true','1'), TRUE, FALSE) AS bridge,
    IFF(LOWER(COALESCE(tags:"tunnel"::STRING,'no')) IN ('yes','true','1'), TRUE, FALSE) AS tunnel,

    COALESCE(
      TRY_TO_NUMBER(tags:"layer"::STRING),
      TRY_TO_NUMBER(REGEXP_SUBSTR(tags:"layer"::STRING, '^-?\\d+')),
      0
    )::INT AS layer,

    tags:"maxspeed"::STRING AS maxspeed_raw,
    CASE
      WHEN tags:"maxspeed"::STRING ILIKE '%mph%' THEN
        TRY_TO_NUMBER(REGEXP_SUBSTR(tags:"maxspeed"::STRING, '\\d+')) * 1.60934
      ELSE
        TRY_TO_NUMBER(REGEXP_SUBSTR(tags:"maxspeed"::STRING, '\\d+'))
    END::NUMBER(10,2) AS maxspeed_kph,

    tags                                     AS tags,
    other_tags::STRING                       AS other_tags_raw,

    TRY_TO_GEOGRAPHY(geom_wkt)               AS geog,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))     AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))) AS geom,

    region::STRING                           AS region_code,
    source_file::STRING                      AS source_file,
    load_ts::TIMESTAMP_NTZ                   AS load_ts
  FROM src
)
SELECT *
FROM typed
WHERE geog IS NOT NULL
  AND highway IS NOT NULL
  -- Filter out pedestrian-only / low-value highways for EV suitability baseline
  AND LOWER(highway) NOT IN ('footway','path','steps','corridor','bridleway','cycleway','pedestrian')
  AND NOT (
    LOWER(highway) = 'service'
    AND LOWER(COALESCE(service,'')) IN ('driveway','parking_aisle','alley','emergency_access','private')
  )
  AND LOWER(COALESCE(access_raw,'yes')) NOT IN ('no','private')
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY osm_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.8) SILVER.ACTIVITY_PLACES
   Source : BRONZE.BUILDINGS_ACTIVITY
   Meaning: non-residential activity proxies (amenity/shop/office/tourism/...)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.ACTIVITY_PLACES
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, activity_class)
AS
WITH src AS (
  SELECT
    b.*,
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags
  FROM BRONZE.BUILDINGS_ACTIVITY b
),
typed AS (
  SELECT
    COALESCE(
      IFF(osm_id     IS NOT NULL, 'N' || osm_id::STRING, NULL),
      IFF(osm_way_id IS NOT NULL, 'W' || osm_way_id::STRING, NULL)
    ) AS feature_id,

    osm_id::STRING     AS osm_id,
    osm_way_id::STRING AS osm_way_id,

    NULLIF(name,'')::STRING AS name,
    tags:"name:en"::STRING  AS name_en,

    CASE
      WHEN COALESCE(amenity::STRING, tags:"amenity"::STRING) IS NOT NULL THEN 'amenity'
      WHEN COALESCE(shop::STRING,    tags:"shop"::STRING)    IS NOT NULL THEN 'shop'
      WHEN COALESCE(office::STRING,  tags:"office"::STRING)  IS NOT NULL THEN 'office'
      WHEN COALESCE(tourism::STRING, tags:"tourism"::STRING) IS NOT NULL THEN 'tourism'
      WHEN COALESCE(leisure::STRING, tags:"leisure"::STRING) IS NOT NULL THEN 'leisure'
      WHEN COALESCE(sport::STRING,   tags:"sport"::STRING)   IS NOT NULL THEN 'sport'
      WHEN COALESCE(craft::STRING,   tags:"craft"::STRING)   IS NOT NULL THEN 'craft'
      WHEN COALESCE(building::STRING,tags:"building"::STRING)IS NOT NULL THEN 'building'
      ELSE NULL
    END AS activity_class,

    COALESCE(
      amenity::STRING,  tags:"amenity"::STRING,
      shop::STRING,     tags:"shop"::STRING,
      office::STRING,   tags:"office"::STRING,
      tourism::STRING,  tags:"tourism"::STRING,
      leisure::STRING,  tags:"leisure"::STRING,
      sport::STRING,    tags:"sport"::STRING,
      craft::STRING,    tags:"craft"::STRING,
      building::STRING, tags:"building"::STRING
    ) AS activity_type,

    LOWER(
      COALESCE(
        amenity::STRING,  tags:"amenity"::STRING,
        shop::STRING,     tags:"shop"::STRING,
        office::STRING,   tags:"office"::STRING,
        tourism::STRING,  tags:"tourism"::STRING,
        leisure::STRING,  tags:"leisure"::STRING,
        sport::STRING,    tags:"sport"::STRING,
        craft::STRING,    tags:"craft"::STRING,
        building::STRING, tags:"building"::STRING
      )
    ) AS activity_type_lc,

    TRY_TO_NUMBER(tags:"building:levels"::STRING)::INT AS building_levels,
    COALESCE(tags:"operator"::STRING, tags:"network"::STRING, tags:"brand"::STRING) AS operator_name,
    tags:"opening_hours"::STRING AS opening_hours,

    tags                                     AS tags,
    other_tags::STRING                       AS other_tags_raw,

    TRY_TO_GEOGRAPHY(geom_wkt)               AS geog,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))     AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))) AS geom,

    region::STRING                           AS region_code,
    source_file::STRING                      AS source_file,
    load_ts::TIMESTAMP_NTZ                   AS load_ts
  FROM src
)
SELECT *
FROM typed
WHERE feature_id IS NOT NULL
  AND geog IS NOT NULL
  AND activity_class IS NOT NULL
  AND activity_type_lc IS NOT NULL
  -- Model-focused allowlist (tune per training iteration)
  AND (
    (activity_class = 'amenity' AND activity_type_lc IN (
      'fuel','charging_station',
      'pharmacy','hospital','clinic','doctors',
      'school','kindergarten','university','college',
      'police','fire_station','townhall','post_office',
      'bank','atm',
      'restaurant','fast_food','cafe',
      'marketplace'
    ))
    OR (activity_class = 'leisure' AND activity_type_lc IN (
      'park','playground','pitch','track',
      'sports_centre','stadium','sports_hall','swimming_pool',
      'fitness_station'
    ))
    OR (activity_class = 'shop' AND activity_type_lc IN (
      'supermarket','convenience','mall','clothes','doityourself','furniture','shoes','chemist','greengrocer'
    ))
    OR (activity_class = 'tourism' AND activity_type_lc IN (
      'hotel','guest_house','hostel','museum','attraction','camp_site'
    ))
    OR (activity_class = 'office' AND activity_type_lc IN ('government','company'))
    OR (activity_class = 'sport' AND activity_type_lc IN ('sports_centre','stadium','swimming_pool'))
    OR (activity_class = 'building' AND activity_type_lc IN (
      'school','kindergarten','university','college',
      'hospital','clinic','fire_station','police',
      'commercial','retail','industrial','warehouse','manufacture',
      'supermarket','mall','office',
      'hotel','guest_house',
      'train_station','bus_station','station'
    ))
  )
  -- Exclude residential / low-signal building types
  AND NOT (activity_class = 'building' AND activity_type_lc IN (
    'house','detached','apartments','residential','terrace','semidetached_house',
    'bungalow','hut','cabin','roof',
    'outbuilding','farm_auxiliary','barn','shed','sty','stable','garage','garages',
    'greenhouse','allotment_house'
  ))
  -- Remove pure mobility parking from "activity places" (covered by other features)
  AND activity_type_lc NOT IN ('parking', 'parking_space')
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY feature_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.9) SILVER.BUILDING_FOOTPRINTS
   Source : BRONZE.BUILDINGS_ACTIVITY (building polygons)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.BUILDING_FOOTPRINTS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH src AS (
  SELECT
    b.*,
    IFF(
      b.other_tags IS NULL OR TRIM(b.other_tags) = '',
      NULL,
      TRY_PARSE_JSON('{' || REPLACE(b.other_tags, '"=>"', '":"') || '}')
    ) AS tags,
    TRY_TO_GEOGRAPHY(b.geom_wkt) AS geog
  FROM BRONZE.BUILDINGS_ACTIVITY b
),
typed AS (
  SELECT
    COALESCE(
      CASE WHEN osm_id     IS NOT NULL THEN 'N' || osm_id::STRING END,
      CASE WHEN osm_way_id IS NOT NULL THEN 'W' || osm_way_id::STRING END
    ) AS feature_id,

    osm_id::STRING     AS osm_id,
    osm_way_id::STRING AS osm_way_id,

    COALESCE(building::STRING, tags:"building"::STRING) AS building_type,
    TRY_TO_NUMBER(tags:"building:levels"::STRING)::INT  AS building_levels,

    geog AS geog,
    ST_ASWKT(geog) AS geom_wkt_4326,
    TRY_TO_GEOMETRY(ST_ASWKT(geog)) AS geom, -- optional mirror

    ST_CENTROID(geog) AS centroid_geog,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(geog), 10) AS h3_r10,

    region::STRING        AS region_code,
    source_file::STRING   AS source_file,
    load_ts::TIMESTAMP_NTZ AS load_ts,

    tags AS tags,
    other_tags::STRING AS other_tags_raw
  FROM src
)
SELECT *
FROM typed
WHERE feature_id IS NOT NULL
  AND geog IS NOT NULL
  AND building_type IS NOT NULL
  AND LOWER(building_type) <> 'no'
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY feature_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   2.10) SILVER.BUILDING_FOOTPRINTS_MODEL
   Purpose: filtered subset suitable for training/inference (remove low-signal)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.BUILDING_FOOTPRINTS_MODEL
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
SELECT *
FROM SILVER.BUILDING_FOOTPRINTS
WHERE LOWER(building_type) NOT IN (
  'yes',
  'outbuilding','farm_auxiliary','shed','barn','sty','stable',
  'garage','garages','roof','greenhouse',
  'allotment_house',
  'hut','cabin'
)
;

-- =============================================================================
-- 3) EUROSTAT / GISCO SILVER TABLES
-- =============================================================================

/* -----------------------------------------------------------------------------
   3.1) SILVER.EUROSTAT_TRAN_R_ELVEHST
   Source : BRONZE.EUROSTAT_TRAN_R_ELVEHST
   Grain  : latest row per (geo, year, vehicle, unit, freq)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.EUROSTAT_TRAN_R_ELVEHST
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (geo, year, vehicle)
AS
WITH base AS (
  SELECT
    source_file::STRING        AS source_file,
    snapshot::STRING           AS snapshot,
    dataset::STRING            AS dataset,
    freq::STRING               AS freq,
    vehicle::STRING            AS vehicle,
    unit::STRING               AS unit,
    geo::STRING                AS geo,
    year::INT                  AS year,
    value::NUMBER(38,0)        AS value,
    ingest_ts::TIMESTAMP_NTZ   AS ingest_ts,
    load_ts::TIMESTAMP_NTZ     AS load_ts
  FROM BRONZE.EUROSTAT_TRAN_R_ELVEHST
  WHERE geo IS NOT NULL
    AND year IS NOT NULL
    AND vehicle IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY geo, year, vehicle, unit, freq
    ORDER BY ingest_ts DESC, load_ts DESC, snapshot DESC, source_file DESC
  ) = 1
)
SELECT * FROM dedup
;

/* -----------------------------------------------------------------------------
   3.2) SILVER.GISCO_NUTS (NUTS 2024)
   Source : BRONZE.GISCO_NUTS
   Grain  : latest row per (level, cntr_code, nuts_id, year, scale, crs)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.GISCO_NUTS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (level, cntr_code, nuts_id)
AS
WITH base AS (
  SELECT
    NUTS_ID::STRING    AS nuts_id,
    CNTR_CODE::STRING  AS cntr_code,
    NAME_LATN::STRING  AS name_latn,
    LEVL_CODE::STRING  AS levl_code,
    LEVEL::INT         AS level,

    YEAR::STRING       AS year,
    SCALE::STRING      AS scale,
    CRS::STRING        AS crs,

    GEOM_WKT::STRING   AS geom_wkt_4326_raw,
    SOURCE_FILE::STRING      AS source_file,
    LOAD_TS::TIMESTAMP_NTZ   AS load_ts
  FROM BRONZE.GISCO_NUTS
  WHERE NUTS_ID IS NOT NULL
    AND CNTR_CODE IS NOT NULL
    AND LEVEL IS NOT NULL
    AND GEOM_WKT IS NOT NULL
),
dedup AS (
  SELECT *
  FROM base
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY level, cntr_code, nuts_id, year, scale, crs
    ORDER BY load_ts DESC, source_file DESC
  ) = 1
),
geo AS (
  SELECT
    ('NUTS:' || year || ':' || scale || ':' || crs || ':' || level::STRING || ':' || nuts_id) AS feature_id,
    d.*,
    COALESCE(
      TRY_TO_GEOGRAPHY(d.geom_wkt_4326_raw),
      TRY_TO_GEOGRAPHY(d.geom_wkt_4326_raw, TRUE)
    ) AS geog
  FROM dedup d
)
SELECT
  feature_id,
  nuts_id, cntr_code, name_latn, levl_code, level,
  year, scale, crs,
  geom_wkt_4326_raw AS geom_wkt_4326,
  geog,
  TRY_TO_GEOMETRY(geom_wkt_4326_raw) AS geom, -- optional
  source_file,
  load_ts
FROM geo
WHERE geog IS NOT NULL
;

/* -----------------------------------------------------------------------------
   3.3) SILVER.LAU_DEGURBA
   Source : BRONZE.EUROSTAT_LAU_DEGURBA
   Adds   : region_code via ADMIN_AREAS admin_level=4 (contains centroid)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.LAU_DEGURBA
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (cntr_code, region_code, year)
AS
WITH dedup AS (
  SELECT
    GISCO_ID::STRING    AS gisco_id,
    CNTR_CODE::STRING   AS cntr_code,
    LAU_ID::STRING      AS lau_id,
    LAU_NAME::STRING    AS lau_name,
    DGURBA::INT         AS degurba,
    FID::NUMBER(38,0)   AS fid,
    GEOM_WKT::STRING    AS geom_wkt_raw,
    YEAR::STRING        AS year,
    SOURCE_FILE::STRING AS source_file,
    LOAD_TS::TIMESTAMP_NTZ AS load_ts
  FROM BRONZE.EUROSTAT_LAU_DEGURBA
  WHERE CNTR_CODE IS NOT NULL
    AND LAU_ID    IS NOT NULL
    AND GEOM_WKT  IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY CNTR_CODE, LAU_ID, YEAR
    ORDER BY LOAD_TS DESC, SOURCE_FILE DESC
  ) = 1
),
geo0 AS (
  SELECT
    ('LAU:' || cntr_code || ':' || lau_id || ':' || year) AS feature_id,
    d.*,
    COALESCE(
      TRY_TO_GEOGRAPHY(geom_wkt_raw),
      TRY_TO_GEOGRAPHY(geom_wkt_raw, TRUE)
    ) AS geog0
  FROM dedup d
),
fix AS (
  SELECT
    *,
    ST_ISVALID(geog0) AS is_valid0,
    /* Conservative fix attempt if invalid:
       - convert to GEOMETRY (allow), apply tiny negative buffer, convert back to GEOGRAPHY */
    IFF(
      ST_ISVALID(geog0),
      geog0,
      TRY_TO_GEOGRAPHY(
        ST_ASWKT(
          ST_BUFFER(TO_GEOMETRY(geom_wkt_raw, TRUE), -0.001)
        )
      )
    ) AS geog_fixed
  FROM geo0
),
admin4 AS (
  SELECT
    region_code,
    TRY_TO_GEOGRAPHY(geom_wkt_4326) AS admin4_geog
  FROM SILVER.ADMIN_AREAS
  WHERE admin_level = 4
    AND boundary = 'administrative'
    AND TRY_TO_GEOGRAPHY(geom_wkt_4326) IS NOT NULL
),
final0 AS (
  SELECT
    f.*,
    ST_ISVALID(f.geog_fixed) AS is_valid_fixed,
    ST_ASWKT(f.geog_fixed)   AS geom_wkt_4326,
    ST_CENTROID(f.geog_fixed) AS lau_centroid_geog
  FROM fix f
  WHERE f.geog_fixed IS NOT NULL
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

  f.geom_wkt_raw,
  f.geom_wkt_4326,
  f.geog_fixed AS geog,
  TRY_TO_GEOMETRY(f.geom_wkt_4326) AS geom,

  f.is_valid0,
  f.is_valid_fixed,

  f.source_file,
  f.load_ts
FROM final0 f
LEFT JOIN admin4 a
  ON ST_CONTAINS(a.admin4_geog, f.lau_centroid_geog)
;

-- =============================================================================
-- 4) CENSUS GRID (admin4-based naming) + POP H3 FEAT
-- =============================================================================

/* -----------------------------------------------------------------------------
   4.1) SILVER.CENSUS_GRID_2021_ADMIN4
   Source : BRONZE.CENSUS_GRID_2021_EUROPE
   Join   : ADMIN_AREAS (admin_level=4) by cell centroid containment
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.CENSUS_GRID_2021_ADMIN4
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (admin4_osm_id, grid_id)
AS
WITH admin4 AS (
  SELECT
    osm_id::STRING      AS admin4_osm_id,
    name::STRING        AS admin4_name,
    region_code::STRING AS admin4_region_code,
    TRY_TO_GEOGRAPHY(geom_wkt_4326) AS admin4_geog
  FROM SILVER.ADMIN_AREAS
  WHERE admin_level = 4
    AND boundary = 'administrative'
    AND TRY_TO_GEOGRAPHY(geom_wkt_4326) IS NOT NULL
),
grid_src AS (
  SELECT
    grd_id::STRING AS grid_id,

    /* Normalize -9999 -> NULL */
    NULLIF(t, -9999)       AS pop_total,
    NULLIF(m, -9999)       AS pop_male,
    NULLIF(f, -9999)       AS pop_female,
    NULLIF(y_lt15, -9999)  AS pop_age_lt15,
    NULLIF(y_1564, -9999)  AS pop_age_1564,
    NULLIF(y_ge65, -9999)  AS pop_age_ge65,
    NULLIF(emp, -9999)     AS emp_total,

    NULLIF(nat, -9999)     AS nat,
    NULLIF(eu_oth, -9999)  AS eu_oth,
    NULLIF(oth, -9999)     AS oth,
    NULLIF(same, -9999)    AS same,

    NULLIF(chg_in, -9999)  AS chg_in,
    NULLIF(chg_out, -9999) AS chg_out,

    NULLIF(land_surface, -9999) AS land_surface,
    NULLIF(populated, -9999)    AS populated,

    TRY_TO_GEOGRAPHY(geom_wkt)        AS cell_geog,
    ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt)) AS cell_pt,
    ST_ASWKT(TRY_TO_GEOGRAPHY(geom_wkt))    AS cell_wkt_4326,

    source_file::STRING         AS source_file,
    load_ts::TIMESTAMP_NTZ      AS load_ts
  FROM BRONZE.CENSUS_GRID_2021_EUROPE
  WHERE TRY_TO_GEOGRAPHY(geom_wkt) IS NOT NULL
    AND NULLIF(t, -9999) IS NOT NULL
),
joined AS (
  SELECT
    g.*,
    a.admin4_osm_id,
    a.admin4_name,
    a.admin4_region_code,
    H3_POINT_TO_CELL_STRING(g.cell_pt, 10) AS h3_r10
  FROM grid_src g
  JOIN admin4 a
    ON ST_CONTAINS(a.admin4_geog, g.cell_pt)
)
SELECT *
FROM joined
QUALIFY ROW_NUMBER() OVER (
  PARTITION BY grid_id
  ORDER BY load_ts DESC, source_file DESC
) = 1
;

/* -----------------------------------------------------------------------------
   4.2) SILVER.FEAT_H3_POP_R10
   Logic : distribute each 1km cell's population uniformly across its H3 R10 polyfill
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.FEAT_H3_POP_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH g AS (
  SELECT
    admin4_region_code::STRING AS region_code,
    admin4_osm_id,
    admin4_name,

    cell_geog,
    pop_total, pop_male, pop_female, pop_age_lt15, pop_age_1564, pop_age_ge65, emp_total,
    load_ts
  FROM SILVER.CENSUS_GRID_2021_ADMIN4
  WHERE cell_geog IS NOT NULL
),
exploded AS (
  SELECT
    g.region_code,
    g.admin4_osm_id,
    g.admin4_name,

    f.value::STRING AS h3_r10,

    g.pop_total, g.pop_male, g.pop_female, g.pop_age_lt15, g.pop_age_1564, g.pop_age_ge65, g.emp_total,
    g.load_ts,

    ARRAY_SIZE(H3_POLYGON_TO_CELLS_STRINGS(g.cell_geog, 10)) AS h3_cnt
  FROM g,
  LATERAL FLATTEN(INPUT => H3_POLYGON_TO_CELLS_STRINGS(g.cell_geog, 10)) f
),
weighted AS (
  SELECT
    region_code, admin4_osm_id, admin4_name, h3_r10,

    pop_total     / NULLIF(h3_cnt, 0) AS pop_total,
    pop_male      / NULLIF(h3_cnt, 0) AS pop_male,
    pop_female    / NULLIF(h3_cnt, 0) AS pop_female,
    pop_age_lt15  / NULLIF(h3_cnt, 0) AS pop_age_lt15,
    pop_age_1564  / NULLIF(h3_cnt, 0) AS pop_age_1564,
    pop_age_ge65  / NULLIF(h3_cnt, 0) AS pop_age_ge65,
    emp_total     / NULLIF(h3_cnt, 0) AS emp_total,

    1 AS grid_cells_cnt_piece,
    load_ts
  FROM exploded
),
agg AS (
  SELECT
    region_code, admin4_osm_id, admin4_name, h3_r10,

    SUM(pop_total)     AS pop_total,
    SUM(pop_male)      AS pop_male,
    SUM(pop_female)    AS pop_female,
    SUM(pop_age_lt15)  AS pop_age_lt15,
    SUM(pop_age_1564)  AS pop_age_1564,
    SUM(pop_age_ge65)  AS pop_age_ge65,
    SUM(emp_total)     AS emp_total,

    IFF(SUM(pop_total) > 0, SUM(pop_age_ge65)/SUM(pop_total), NULL) AS share_age_ge65,
    IFF(SUM(pop_total) > 0, SUM(pop_age_lt15)/SUM(pop_total), NULL) AS share_age_lt15,
    IFF(SUM(pop_total) > 0, SUM(emp_total)/SUM(pop_total), NULL)    AS share_emp,

    SUM(grid_cells_cnt_piece) AS grid_cells_cnt,
    MAX(load_ts) AS last_load_ts
  FROM weighted
  GROUP BY 1,2,3,4
),
cell AS (
  SELECT DISTINCT
    region_code,
    h3_r10,
    ST_ASWKT(H3_CELL_TO_BOUNDARY(h3_r10)) AS cell_wkt_4326,
    ST_AREA(H3_CELL_TO_BOUNDARY(h3_r10))  AS cell_area_m2,
    ST_ASWKT(H3_CELL_TO_POINT(h3_r10))    AS cell_center_wkt_4326
  FROM agg
)
SELECT
  a.region_code,
  a.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,
  a.admin4_osm_id,
  a.admin4_name,
  a.pop_total, a.pop_male, a.pop_female, a.pop_age_lt15, a.pop_age_1564, a.pop_age_ge65, a.emp_total,
  a.share_age_ge65, a.share_age_lt15, a.share_emp,
  a.grid_cells_cnt,
  a.last_load_ts
FROM agg a
JOIN cell c
  ON c.region_code=a.region_code AND c.h3_r10=a.h3_r10
;

-- =============================================================================
-- 5) H3 DIMENSION + H3 FEATURE TABLES (OSM-derived)
-- =============================================================================

/* -----------------------------------------------------------------------------
   5.1) SILVER.DIM_H3_R10_CELLS
   Meaning: canonical set of H3 cells present in OSM layers (region-scoped)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.DIM_H3_R10_CELLS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH all_h3 AS (
  SELECT region_code::STRING AS region_code,
         H3_POINT_TO_CELL_STRING(TRY_TO_GEOGRAPHY(geom_wkt_4326), 10) AS h3_r10
  FROM SILVER.EV_CHARGERS
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT region_code::STRING,
         H3_POINT_TO_CELL_STRING(ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt_4326)), 10)
  FROM SILVER.ROAD_SEGMENTS
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT region_code::STRING,
         H3_POINT_TO_CELL_STRING(TRY_TO_GEOGRAPHY(geom_wkt_4326), 10)
  FROM SILVER.POI_POINTS
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT region_code::STRING,
         H3_POINT_TO_CELL_STRING(ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt_4326)), 10)
  FROM SILVER.POI_AREAS
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT region_code::STRING,
         H3_POINT_TO_CELL_STRING(TRY_TO_GEOGRAPHY(geom_wkt_4326), 10)
  FROM SILVER.TRANSIT_POINTS
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT region_code::STRING,
         H3_POINT_TO_CELL_STRING(ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt_4326)), 10)
  FROM SILVER.TRANSIT_LINES
  WHERE geom_wkt_4326 IS NOT NULL

  UNION ALL
  SELECT region_code::STRING,
         H3_POINT_TO_CELL_STRING(ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt_4326)), 10)
  FROM SILVER.ACTIVITY_PLACES
  WHERE geom_wkt_4326 IS NOT NULL
),
distinct_h3 AS (
  SELECT DISTINCT region_code, h3_r10
  FROM all_h3
  WHERE h3_r10 IS NOT NULL
)
SELECT
  region_code,
  h3_r10,
  H3_CELL_TO_BOUNDARY(h3_r10) AS cell_geog,
  ST_ASWKT(H3_CELL_TO_BOUNDARY(h3_r10)) AS cell_wkt_4326,
  ST_AREA(H3_CELL_TO_BOUNDARY(h3_r10)) AS cell_area_m2,
  H3_CELL_TO_POINT(h3_r10) AS cell_center_geog,
  ST_ASWKT(H3_CELL_TO_POINT(h3_r10)) AS cell_center_wkt_4326
FROM distinct_h3
;

/* -----------------------------------------------------------------------------
   5.2) SILVER.FEAT_H3_EV_CHARGERS_R10
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.FEAT_H3_EV_CHARGERS_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH src AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(TRY_TO_GEOGRAPHY(geom_wkt_4326), 10) AS h3_r10,
    total_sockets_cnt::INT AS total_sockets_cnt,
    has_dc::BOOLEAN AS has_dc,
    has_ac::BOOLEAN AS has_ac,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.EV_CHARGERS
  WHERE geom_wkt_4326 IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,
    COUNT(*) AS chargers_cnt,
    SUM(COALESCE(total_sockets_cnt,0)) AS sockets_cnt_sum,
    COUNT_IF(has_dc) AS chargers_dc_cnt,
    COUNT_IF(has_ac) AS chargers_ac_cnt,
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
  a.chargers_cnt,
  a.sockets_cnt_sum,
  a.chargers_dc_cnt,
  a.chargers_ac_cnt,
  a.last_load_ts
FROM SILVER.DIM_H3_R10_CELLS c
LEFT JOIN agg a
  ON a.region_code = c.region_code AND a.h3_r10 = c.h3_r10
;

/* -----------------------------------------------------------------------------
   5.3) SILVER.FEAT_H3_ROADS_R10
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.FEAT_H3_ROADS_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH src AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt_4326)), 10) AS h3_r10,
    LOWER(highway)::STRING AS highway_lc,
    ST_LENGTH(TRY_TO_GEOGRAPHY(geom_wkt_4326))::FLOAT AS len_m,
    oneway::BOOLEAN AS oneway,
    lanes::INT AS lanes,
    maxspeed_kph::FLOAT AS maxspeed_kph,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.ROAD_SEGMENTS
  WHERE geom_wkt_4326 IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    SUM(len_m) AS roads_len_m_sum,
    SUM(IFF(highway_lc IN ('motorway','trunk','primary','secondary'), len_m, 0)) AS roads_major_len_m_sum,
    COUNT(*) AS road_segments_cnt,

    AVG(IFF(maxspeed_kph IS NULL, NULL, maxspeed_kph))::FLOAT AS maxspeed_avg_kph,
    APPROX_PERCENTILE(maxspeed_kph, 0.5) AS maxspeed_p50_kph,

    AVG(IFF(lanes IS NULL, NULL, lanes))::FLOAT AS lanes_avg,
    APPROX_PERCENTILE(lanes, 0.5) AS lanes_p50,

    AVG(IFF(oneway IS NULL, NULL, IFF(oneway,1,0)))::FLOAT AS oneway_share,

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

  a.roads_len_m_sum,
  a.roads_major_len_m_sum,
  a.road_segments_cnt,

  a.maxspeed_avg_kph,
  a.maxspeed_p50_kph,
  a.lanes_avg,
  a.lanes_p50,
  a.oneway_share,

  a.last_load_ts
FROM SILVER.DIM_H3_R10_CELLS c
LEFT JOIN agg a
  ON a.region_code = c.region_code AND a.h3_r10 = c.h3_r10
;

/* -----------------------------------------------------------------------------
   5.4) SILVER.FEAT_H3_POI_R10
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.FEAT_H3_POI_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH pts AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(TRY_TO_GEOGRAPHY(geom_wkt_4326), 10) AS h3_r10,
    poi_class::STRING AS poi_class,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.POI_POINTS
  WHERE geom_wkt_4326 IS NOT NULL
    AND poi_class IS NOT NULL
),
areas AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt_4326)), 10) AS h3_r10,
    poi_class::STRING AS poi_class,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.POI_AREAS
  WHERE geom_wkt_4326 IS NOT NULL
    AND poi_class IS NOT NULL
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
    COUNT_IF(poi_class='amenity')  AS poi_amenity_cnt,
    COUNT_IF(poi_class='shop')     AS poi_shop_cnt,
    COUNT_IF(poi_class='tourism')  AS poi_tourism_cnt,
    COUNT_IF(poi_class='building') AS poi_building_cnt,
    COUNT_IF(poi_class='landuse')  AS poi_landuse_cnt,
    MAX(load_ts) AS last_load_ts
  FROM all_poi
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
)
SELECT
  c.region_code,
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
FROM SILVER.DIM_H3_R10_CELLS c
LEFT JOIN agg a
  ON a.region_code = c.region_code AND a.h3_r10 = c.h3_r10
;

/* -----------------------------------------------------------------------------
   5.5) SILVER.FEAT_H3_TRANSIT_R10
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.FEAT_H3_TRANSIT_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH tp AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(TRY_TO_GEOGRAPHY(geom_wkt_4326), 10) AS h3_r10,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.TRANSIT_POINTS
  WHERE geom_wkt_4326 IS NOT NULL
),
tl AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt_4326)), 10) AS h3_r10,
    ST_LENGTH(TRY_TO_GEOGRAPHY(geom_wkt_4326))::FLOAT AS len_m,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.TRANSIT_LINES
  WHERE geom_wkt_4326 IS NOT NULL
),
agg_points AS (
  SELECT region_code, h3_r10, COUNT(*) AS transit_points_cnt, MAX(load_ts) AS last_load_ts
  FROM tp
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
),
agg_lines AS (
  SELECT region_code, h3_r10, SUM(len_m) AS transit_lines_len_m_sum, MAX(load_ts) AS last_load_ts
  FROM tl
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
)
SELECT
  c.region_code,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  COALESCE(p.transit_points_cnt, 0) AS transit_points_cnt,
  COALESCE(l.transit_lines_len_m_sum, 0) AS transit_lines_len_m_sum,

  GREATEST(
    COALESCE(p.last_load_ts, '1970-01-01'::TIMESTAMP_NTZ),
    COALESCE(l.last_load_ts, '1970-01-01'::TIMESTAMP_NTZ)
  ) AS last_load_ts
FROM SILVER.DIM_H3_R10_CELLS c
LEFT JOIN agg_points p ON p.region_code = c.region_code AND p.h3_r10 = c.h3_r10
LEFT JOIN agg_lines  l ON l.region_code = c.region_code AND l.h3_r10 = c.h3_r10
;

/* -----------------------------------------------------------------------------
   5.6) SILVER.FEAT_H3_ACTIVITY_PLACES_R10
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.FEAT_H3_ACTIVITY_PLACES_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH src AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(TRY_TO_GEOGRAPHY(geom_wkt_4326)), 10) AS h3_r10,
    LOWER(activity_class)::STRING AS activity_class,
    LOWER(activity_type)::STRING  AS activity_type,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.ACTIVITY_PLACES
  WHERE geom_wkt_4326 IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    COUNT(*) AS places_cnt,

    COUNT_IF(activity_class='amenity') AS places_amenity_cnt,
    COUNT_IF(activity_class='shop')    AS places_shop_cnt,
    COUNT_IF(activity_class='tourism') AS places_tourism_cnt,
    COUNT_IF(activity_class='office')  AS places_office_cnt,
    COUNT_IF(activity_class='leisure') AS places_leisure_cnt,
    COUNT_IF(activity_class='sport')   AS places_sport_cnt,

    COUNT_IF(activity_type IN ('parking','fuel','charging_station')) AS places_mobility_cnt,
    COUNT_IF(activity_type IN ('supermarket','mall'))                AS places_retail_big_cnt,
    COUNT_IF(activity_type IN ('restaurant','fast_food','cafe'))     AS places_food_cnt,
    COUNT_IF(activity_type IN ('hotel','guest_house','hostel'))      AS places_stay_cnt,

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
FROM SILVER.DIM_H3_R10_CELLS c
LEFT JOIN agg a
  ON a.region_code = c.region_code AND a.h3_r10 = c.h3_r10
;

/* -----------------------------------------------------------------------------
   5.7) SILVER.FEAT_H3_DEGURBA_R10
   Logic : assign LAU degurba to each H3 cell by point-in-polygon (cell center)
----------------------------------------------------------------------------- */
CREATE OR REPLACE DYNAMIC TABLE SILVER.FEAT_H3_DEGURBA_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH h AS (
  SELECT
    region_code,
    h3_r10,
    cell_center_geog
  FROM SILVER.DIM_H3_R10_CELLS
),
lau AS (
  SELECT
    region_code,
    lau_id,
    lau_name,
    degurba,
    year,
    geog,
    load_ts
  FROM SILVER.LAU_DEGURBA
  WHERE geog IS NOT NULL
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
 AND ST_CONTAINS(l.geog, h.cell_center_geog)
;

-- =============================================================================
-- 6) OPTIONAL STORAGE OPTIMIZATIONS (enable selectively)
-- =============================================================================

-- Search Optimization Service (use only if you have heavy point-lookups by region_code/h3_r10)
-- ALTER TABLE SILVER.DIM_H3_R10_CELLS ADD SEARCH OPTIMIZATION ON EQUALITY(region_code, h3_r10);
-- ALTER TABLE SILVER.FEAT_H3_POP_R10   ADD SEARCH OPTIMIZATION ON EQUALITY(region_code, h3_r10);
-- ALTER TABLE SILVER.FEAT_H3_ROADS_R10 ADD SEARCH OPTIMIZATION ON EQUALITY(region_code, h3_r10);

-- =============================================================================
-- 7) REQUIRED CHECKS (geo tables): rowcount + WKT sample
-- =============================================================================

-- ADMIN_AREAS
SELECT COUNT(*) AS rows_admin_areas FROM SILVER.ADMIN_AREAS;
SELECT feature_id, admin_level, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.ADMIN_AREAS
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- EV_CHARGERS
SELECT COUNT(*) AS rows_ev_chargers FROM SILVER.EV_CHARGERS;
SELECT feature_id, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.EV_CHARGERS
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- POI_POINTS
SELECT COUNT(*) AS rows_poi_points FROM SILVER.POI_POINTS;
SELECT feature_id, poi_class, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.POI_POINTS
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- POI_AREAS
SELECT COUNT(*) AS rows_poi_areas FROM SILVER.POI_AREAS;
SELECT feature_id, poi_class, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.POI_AREAS
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- TRANSIT_POINTS
SELECT COUNT(*) AS rows_transit_points FROM SILVER.TRANSIT_POINTS;
SELECT feature_id, poi_class, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.TRANSIT_POINTS
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- TRANSIT_LINES
SELECT COUNT(*) AS rows_transit_lines FROM SILVER.TRANSIT_LINES;
SELECT feature_id, line_class, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.TRANSIT_LINES
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- ROAD_SEGMENTS
SELECT COUNT(*) AS rows_road_segments FROM SILVER.ROAD_SEGMENTS;
SELECT feature_id, highway, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.ROAD_SEGMENTS
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- ACTIVITY_PLACES
SELECT COUNT(*) AS rows_activity_places FROM SILVER.ACTIVITY_PLACES;
SELECT feature_id, activity_class, activity_type, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.ACTIVITY_PLACES
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- BUILDING_FOOTPRINTS
SELECT COUNT(*) AS rows_building_footprints FROM SILVER.BUILDING_FOOTPRINTS;
SELECT feature_id, building_type, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.BUILDING_FOOTPRINTS
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- CENSUS_GRID_2021_ADMIN4
SELECT COUNT(*) AS rows_census_grid_admin4 FROM SILVER.CENSUS_GRID_2021_ADMIN4;
SELECT admin4_name, grid_id, LEFT(cell_wkt_4326, 160) AS wkt_prefix
FROM SILVER.CENSUS_GRID_2021_ADMIN4
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GISCO_NUTS
SELECT COUNT(*) AS rows_gisco_nuts FROM SILVER.GISCO_NUTS;
SELECT level, cntr_code, nuts_id, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.GISCO_NUTS
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- LAU_DEGURBA
SELECT COUNT(*) AS rows_lau_degurba FROM SILVER.LAU_DEGURBA;
SELECT cntr_code, lau_id, year, is_valid0, is_valid_fixed, LEFT(geom_wkt_4326, 160) AS wkt_prefix
FROM SILVER.LAU_DEGURBA
WHERE geom_wkt_4326 IS NOT NULL
LIMIT 5;

-- DIM_H3_R10_CELLS
SELECT COUNT(*) AS rows_dim_h3 FROM SILVER.DIM_H3_R10_CELLS;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 160) AS wkt_prefix
FROM SILVER.DIM_H3_R10_CELLS
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- FEAT_H3_POP_R10
SELECT COUNT(*) AS rows_feat_pop_r10 FROM SILVER.FEAT_H3_POP_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 160) AS wkt_prefix
FROM SILVER.FEAT_H3_POP_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- =============================================================================
-- 8) ADDITIONAL SANITY CHECKS (recommended)
-- =============================================================================

-- Geometry validity health (quick signals)
SELECT
  'ADMIN_AREAS' AS t,
  COUNT(*) AS total,
  COUNT_IF(geog0 IS NULL) AS geog_null,
  COUNT_IF(geog0 IS NOT NULL AND NOT ST_ISVALID(geog0)) AS invalid_geog
FROM SILVER.ADMIN_AREAS;

SELECT
  COUNT(*) AS bad_share_rows
FROM SILVER.FEAT_H3_POP_R10
WHERE (share_age_ge65 IS NOT NULL AND (share_age_ge65 < 0 OR share_age_ge65 > 1))
   OR (share_age_lt15 IS NOT NULL AND (share_age_lt15 < 0 OR share_age_lt15 > 1))
   OR (share_emp      IS NOT NULL AND (share_emp      < 0 OR share_emp      > 1));

SELECT
  COUNT(*) AS bad_negative_counts
FROM SILVER.FEAT_H3_ACTIVITY_PLACES_R10
WHERE COALESCE(places_cnt,0) < 0
   OR COALESCE(places_amenity_cnt,0) < 0
   OR COALESCE(places_shop_cnt,0) < 0
   OR COALESCE(places_food_cnt,0) < 0;

-- End of SILVER bootstrap