/* =============================================================================
GOLD (country-agnostic) — unified + optimized + idempotent bootstrap
Database : GEO_PROJECT
Schema   : GOLD
Storage  : Dynamic Tables (TARGET_LAG = 48 hours)
Warehouse: COMPUTE_WH

Principles (same style as SILVER):
- Country-agnostic keys: (region_code, h3_r10)
- Canonical H3 geometry always comes from SILVER.DIM_H3_R10_CELLS
  (cell_wkt_4326, cell_center_wkt_4326, cell_area_m2)
- Idempotent: DROP old + new
- Mandatory checks at the end: rowcount + WKT samples (geo tables)
============================================================================= */

USE DATABASE GEO_PROJECT;
CREATE SCHEMA IF NOT EXISTS GOLD;
USE SCHEMA GOLD;

-- =============================================================================
-- 0) DROP (idempotent + cleanup old/stale names)
--    Order: dense helpers -> sparse -> core
-- =============================================================================

DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS_DENSE;
DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS;
DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_POI_AREAS_R10__CENTROID_XAREA;
DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_POI_AREAS_R10;
DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_POI_POINTS_R10;

DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_TRANSIT_POINTS_R10;

DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_ROADS_R10;
DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_DEGURBA_R10;

DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_CHARGING_R10;
DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_BUILDINGS_R10;
DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_POP_R10;

-- old naming cleanup (if you still have it)
DROP DYNAMIC TABLE IF EXISTS GOLD.OLD_FEAT_H3_POP_R10;
DROP DYNAMIC TABLE IF EXISTS GOLD.FEAT_H3_POP_R10_FINAL;  -- from older draft

/* =============================================================================
1) GOLD.FEAT_H3_POP_R10
   Source: SILVER.FEAT_H3_POP_R10 (already H3-grained)
   Adds: densities (support km² vs hex km²), support_area_m2 + method tag
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_POP_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH s AS (
  SELECT
    region_code::STRING AS region_code,
    h3_r10::STRING      AS h3_r10,

    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,

    admin4_osm_id,
    admin4_name,

    pop_total,
    pop_male,
    pop_female,
    pop_age_lt15,
    pop_age_1564,
    pop_age_ge65,
    emp_total,

    share_age_ge65,
    share_age_lt15,
    share_emp,

    grid_cells_cnt,
    last_load_ts
  FROM SILVER.FEAT_H3_POP_R10
)
SELECT
  region_code,
  h3_r10,

  cell_area_m2,
  cell_wkt_4326,
  cell_center_wkt_4326,

  admin4_osm_id,
  admin4_name,

  pop_total,
  pop_male,
  pop_female,
  pop_age_lt15,
  pop_age_1564,
  pop_age_ge65,
  emp_total,

  share_age_ge65,
  share_age_lt15,
  share_emp,

  grid_cells_cnt,

  /* support: how many 1km census cells contributed (rough support area) */
  (grid_cells_cnt * 1000000)::NUMBER(38,0) AS support_area_m2,
  'census_grid_1km_to_h3_admin4'::STRING    AS pop_method,

  /* densities by GRID support (per “supported km²”) */
  pop_total / NULLIF(grid_cells_cnt, 0) AS pop_per_km2_support,
  emp_total / NULLIF(grid_cells_cnt, 0) AS emp_per_km2_support,

  /* densities by HEX area */
  pop_total / NULLIF(cell_area_m2 / 1e6, 0) AS pop_per_km2_hex,
  emp_total / NULLIF(cell_area_m2 / 1e6, 0) AS emp_per_km2_hex,

  last_load_ts
FROM s;


/* =============================================================================
2) GOLD.FEAT_H3_BUILDINGS_R10
   Source: SILVER.BUILDING_FOOTPRINTS_MODEL (H3 already computed in SILVER)
   Canonical cell geo: from SILVER.DIM_H3_R10_CELLS (no recompute)
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_BUILDINGS_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH b AS (
  SELECT
    region_code::STRING               AS region_code,
    h3_r10::STRING                    AS h3_r10,
    LOWER(building_type::STRING)      AS building_type,
    COALESCE(building_levels::INT, 1) AS building_levels,
    geog                              AS geog,
    load_ts::TIMESTAMP_NTZ            AS load_ts
  FROM SILVER.BUILDING_FOOTPRINTS_MODEL
  WHERE region_code IS NOT NULL
    AND h3_r10 IS NOT NULL
    AND geog IS NOT NULL
),
b2 AS (
  SELECT
    region_code,
    h3_r10,
    building_levels,
    ST_AREA(geog)::FLOAT AS footprint_area_m2,
    CASE
      WHEN building_type IN ('house','detached','apartments','residential','semidetached_house','terrace','bungalow','dormitory')
        THEN 'residential'
      WHEN building_type IN ('retail','commercial','office','industrial','manufacture','warehouse','service',
                             'school','kindergarten','university','hospital','fire_station','government',
                             'supermarket','hotel','train_station','church','chapel')
        THEN 'nonresidential'
      WHEN building_type = 'yes' THEN 'unknown'
      ELSE 'other'
    END AS building_group,
    load_ts
  FROM b
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    COUNT(*) AS buildings_cnt,
    COUNT_IF(building_group='residential')    AS res_buildings_cnt,
    COUNT_IF(building_group='nonresidential') AS nonres_buildings_cnt,
    COUNT_IF(building_group='unknown')        AS unknown_buildings_cnt,

    SUM(footprint_area_m2) AS footprint_area_m2_sum,
    SUM(footprint_area_m2 * building_levels) AS floor_area_m2_est_sum,

    AVG(building_levels)::FLOAT              AS levels_avg,
    APPROX_PERCENTILE(building_levels, 0.5)  AS levels_p50,

    APPROX_PERCENTILE(footprint_area_m2, 0.5) AS footprint_area_p50_m2,
    APPROX_PERCENTILE(footprint_area_m2, 0.9) AS footprint_area_p90_m2,

    MAX(load_ts) AS last_load_ts
  FROM b2
  GROUP BY 1,2
),
cell AS (
  SELECT
    region_code::STRING AS region_code,
    h3_r10::STRING      AS h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  FROM SILVER.DIM_H3_R10_CELLS
)
SELECT
  a.region_code,
  a.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.buildings_cnt,
  a.res_buildings_cnt,
  a.nonres_buildings_cnt,
  a.unknown_buildings_cnt,

  a.footprint_area_m2_sum,
  a.floor_area_m2_est_sum,

  a.levels_avg,
  a.levels_p50,
  a.footprint_area_p50_m2,
  a.footprint_area_p90_m2,

  /* densities per km² */
  a.buildings_cnt / NULLIF(c.cell_area_m2 / 1e6, 0)         AS buildings_per_km2,
  a.footprint_area_m2_sum / NULLIF(c.cell_area_m2 / 1e6, 0) AS footprint_m2_per_km2,
  a.floor_area_m2_est_sum / NULLIF(c.cell_area_m2 / 1e6, 0) AS floor_area_m2_per_km2,

  /* built-up share (may exceed 1 if footprints overlap; usually <= 1 in sane data) */
  a.footprint_area_m2_sum / NULLIF(c.cell_area_m2, 0)       AS built_up_share,

  a.last_load_ts
FROM agg a
JOIN cell c
  ON c.region_code = a.region_code
 AND c.h3_r10      = a.h3_r10;


/* =============================================================================
3) GOLD.FEAT_H3_CHARGING_R10
   Sources:
     - SILVER.EV_CHARGERS  (point supply)
     - GOLD.FEAT_H3_POP_R10 (demand + canonical cell geo where pop exists)
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_CHARGING_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH ch AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(geog, 10)::STRING AS h3_r10,

    COALESCE(total_sockets_cnt, 0)::INT AS total_sockets_cnt,
    IFF(has_dc = TRUE, 1, 0) AS is_dc,
    IFF(has_ac = TRUE, 1, 0) AS is_ac,

    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.EV_CHARGERS
  WHERE geog IS NOT NULL
    AND region_code IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    COUNT(*) AS chargers_cnt,
    SUM(total_sockets_cnt) AS sockets_cnt_sum,
    SUM(is_dc) AS chargers_dc_cnt,
    SUM(is_ac) AS chargers_ac_cnt,

    IFF(COUNT(*) > 0, SUM(is_dc) / COUNT(*), NULL) AS dc_share,

    MAX(load_ts) AS last_load_ts
  FROM ch
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
),
pop AS (
  SELECT
    region_code,
    h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,
    pop_total
  FROM GOLD.FEAT_H3_POP_R10
)
SELECT
  /* key */
  p.region_code,
  p.h3_r10,

  /* geo */
  p.cell_area_m2,
  p.cell_wkt_4326,
  p.cell_center_wkt_4326,

  /* supply */
  COALESCE(a.chargers_cnt, 0)      AS chargers_cnt,
  COALESCE(a.sockets_cnt_sum, 0)   AS sockets_cnt_sum,
  COALESCE(a.chargers_dc_cnt, 0)   AS chargers_dc_cnt,
  COALESCE(a.chargers_ac_cnt, 0)   AS chargers_ac_cnt,
  a.dc_share,

  /* demand */
  p.pop_total,

  /* supply per demand */
  IFF(p.pop_total > 0, COALESCE(a.chargers_cnt,0)     * 10000.0 / p.pop_total, NULL) AS chargers_per_10k_pop,
  IFF(p.pop_total > 0, COALESCE(a.sockets_cnt_sum,0)  * 10000.0 / p.pop_total, NULL) AS sockets_per_10k_pop,

  /* supply per area */
  COALESCE(a.chargers_cnt,0)     / NULLIF(p.cell_area_m2/1e6,0) AS chargers_per_km2,
  COALESCE(a.sockets_cnt_sum,0)  / NULLIF(p.cell_area_m2/1e6,0) AS sockets_per_km2,

  a.last_load_ts
FROM pop p
LEFT JOIN agg a
  ON a.region_code = p.region_code
 AND a.h3_r10      = p.h3_r10;


/* =============================================================================
4) GOLD.FEAT_H3_DEGURBA_R10
   Source: SILVER.FEAT_H3_DEGURBA_R10 (sparse assignment),
           SILVER.DIM_H3_R10_CELLS (dense cell geometry)
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_DEGURBA_R10
  TARGET_LAG='48 hours'
  WAREHOUSE=COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH s AS (
  SELECT
    region_code,
    h3_r10,
    TRY_TO_NUMBER(year) AS year,
    degurba,
    lau_id,
    lau_name,
    last_load_ts
  FROM SILVER.FEAT_H3_DEGURBA_R10
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY region_code, h3_r10
    ORDER BY TRY_TO_NUMBER(year) DESC NULLS LAST, last_load_ts DESC NULLS LAST
  ) = 1
),
cells AS (
  SELECT
    region_code,
    h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  FROM SILVER.DIM_H3_R10_CELLS
)
SELECT
  c.region_code,
  c.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  s.year,
  s.lau_id,
  s.lau_name,
  s.degurba,

  /* one-hot for ML */
  IFF(s.degurba = 1, 1, 0) AS degurba_1_city,
  IFF(s.degurba = 2, 1, 0) AS degurba_2_towns_suburbs,
  IFF(s.degurba = 3, 1, 0) AS degurba_3_rural,

  s.last_load_ts
FROM cells c
LEFT JOIN s
  ON s.region_code = c.region_code
 AND s.h3_r10      = c.h3_r10;


/* =============================================================================
5) GOLD.FEAT_H3_ROADS_R10
   Source: SILVER.ROAD_SEGMENTS (line features)
   Canonical cell geo: SILVER.DIM_H3_R10_CELLS
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_ROADS_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH r AS (
  SELECT
    region_code::STRING        AS region_code,
    osm_id::STRING             AS osm_id,
    highway::STRING            AS highway,
    NULLIF(service::STRING,'') AS service,
    oneway::BOOLEAN            AS oneway,
    lanes::INT                 AS lanes,
    maxspeed_kph::NUMBER(10,2) AS maxspeed_kph,
    lit::BOOLEAN               AS lit,
    bridge::BOOLEAN            AS bridge,
    tunnel::BOOLEAN            AS tunnel,
    geog                       AS geog,
    load_ts::TIMESTAMP_NTZ     AS load_ts
  FROM SILVER.ROAD_SEGMENTS
  WHERE geog IS NOT NULL
    AND region_code IS NOT NULL
    AND osm_id IS NOT NULL
),
typed AS (
  SELECT
    r.*,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(geog), 10)::STRING AS h3_r10,
    ST_LENGTH(geog)::FLOAT                                AS length_m,
    CASE
      WHEN LOWER(highway) IN ('motorway','motorway_link') THEN 'motorway'
      WHEN LOWER(highway) IN ('trunk','trunk_link')       THEN 'trunk'
      WHEN LOWER(highway) IN ('primary','primary_link')   THEN 'primary'
      WHEN LOWER(highway) IN ('secondary','secondary_link') THEN 'secondary'
      WHEN LOWER(highway) IN ('tertiary','tertiary_link') THEN 'tertiary'
      WHEN LOWER(highway) IN ('residential','living_street') THEN 'residential'
      WHEN LOWER(highway) = 'service' THEN 'service'
      ELSE 'other'
    END AS road_class
  FROM r
  WHERE H3_POINT_TO_CELL_STRING(ST_CENTROID(geog), 10) IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    COUNT(*)          AS road_segments_cnt,
    SUM(length_m)     AS road_length_m_sum,

    SUM(IFF(road_class='motorway',    length_m, 0)) AS motorway_length_m_sum,
    SUM(IFF(road_class='trunk',       length_m, 0)) AS trunk_length_m_sum,
    SUM(IFF(road_class='primary',     length_m, 0)) AS primary_length_m_sum,
    SUM(IFF(road_class='secondary',   length_m, 0)) AS secondary_length_m_sum,
    SUM(IFF(road_class='tertiary',    length_m, 0)) AS tertiary_length_m_sum,
    SUM(IFF(road_class='residential', length_m, 0)) AS residential_length_m_sum,
    SUM(IFF(road_class='service',     length_m, 0)) AS service_length_m_sum,

    AVG(lanes)::FLOAT                    AS lanes_avg,
    APPROX_PERCENTILE(lanes, 0.5)        AS lanes_p50,

    AVG(maxspeed_kph)::FLOAT             AS maxspeed_kph_avg,
    APPROX_PERCENTILE(maxspeed_kph, 0.5) AS maxspeed_kph_p50,
    APPROX_PERCENTILE(maxspeed_kph, 0.9) AS maxspeed_kph_p90,

    AVG(IFF(oneway,1,0))::FLOAT          AS oneway_share,
    AVG(IFF(lit,1,0))::FLOAT             AS lit_share,

    COUNT_IF(bridge)                     AS bridge_cnt,
    COUNT_IF(tunnel)                     AS tunnel_cnt,

    MAX(load_ts)                         AS last_load_ts
  FROM typed
  GROUP BY 1,2
),
cell AS (
  SELECT
    region_code,
    h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  FROM SILVER.DIM_H3_R10_CELLS
)
SELECT
  a.region_code,
  a.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.road_segments_cnt,
  a.road_length_m_sum,

  a.motorway_length_m_sum,
  a.trunk_length_m_sum,
  a.primary_length_m_sum,
  a.secondary_length_m_sum,
  a.tertiary_length_m_sum,
  a.residential_length_m_sum,
  a.service_length_m_sum,

  a.lanes_avg,
  a.lanes_p50,

  a.maxspeed_kph_avg,
  a.maxspeed_kph_p50,
  a.maxspeed_kph_p90,

  a.oneway_share,
  a.lit_share,

  a.bridge_cnt,
  a.tunnel_cnt,

  /* densities per km² */
  a.road_segments_cnt / NULLIF(c.cell_area_m2 / 1e6, 0) AS road_segments_per_km2,
  a.road_length_m_sum / NULLIF(c.cell_area_m2 / 1e6, 0) AS road_length_m_per_km2,

  a.last_load_ts
FROM agg a
JOIN cell c
  ON c.region_code = a.region_code
 AND c.h3_r10      = a.h3_r10;


/* =============================================================================
6) GOLD.FEAT_H3_POI_POINTS_R10
   Source: SILVER.POI_POINTS (points)
   Canonical cell geo: SILVER.DIM_H3_R10_CELLS
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_POI_POINTS_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH p AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(geog, 10)::STRING AS h3_r10,
    poi_class::STRING AS poi_class,
    poi_type::STRING  AS poi_type,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.POI_POINTS
  WHERE region_code IS NOT NULL
    AND geog IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    COUNT(*)                  AS poi_points_cnt,
    COUNT(DISTINCT poi_class) AS poi_classes_cnt,
    COUNT(DISTINCT poi_type)  AS poi_types_cnt,

    COUNT_IF(poi_class='amenity')           AS amenity_cnt,
    COUNT_IF(poi_class='shop')             AS shop_cnt,
    COUNT_IF(poi_class='tourism')          AS tourism_cnt,
    COUNT_IF(poi_class='leisure')          AS leisure_cnt,
    COUNT_IF(poi_class='office')           AS office_cnt,
    COUNT_IF(poi_class='craft')            AS craft_cnt,
    COUNT_IF(poi_class='man_made')         AS man_made_cnt,
    COUNT_IF(poi_class='emergency')        AS emergency_cnt,
    COUNT_IF(poi_class='public_transport') AS public_transport_cnt,
    COUNT_IF(poi_class='railway')          AS railway_cnt,
    COUNT_IF(poi_class='highway')          AS highway_cnt,
    COUNT_IF(poi_class='place')            AS place_cnt,

    /* EV-relevant poi_type buckets (heuristics) */
    COUNT_IF(LOWER(poi_type) IN ('parking','parking_entrance','bicycle_parking')) AS parking_cnt,
    COUNT_IF(LOWER(poi_type) IN ('fuel','charging_station','car_wash','car_rental','car_sharing','parking_space')) AS mobility_services_cnt,
    COUNT_IF(LOWER(poi_type) IN ('supermarket','convenience','mall','department_store','hardware','doityourself')) AS retail_core_cnt,
    COUNT_IF(LOWER(poi_type) IN ('restaurant','fast_food','cafe','bar','pub')) AS food_cnt,
    COUNT_IF(LOWER(poi_type) IN ('hotel','motel','hostel','guest_house','apartments')) AS lodging_cnt,
    COUNT_IF(LOWER(poi_type) IN ('hospital','clinic','doctors','pharmacy')) AS health_cnt,

    MAX(load_ts) AS last_load_ts
  FROM p
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
),
cell AS (
  SELECT
    region_code::STRING AS region_code,
    h3_r10::STRING      AS h3_r10,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  FROM SILVER.DIM_H3_R10_CELLS
)
SELECT
  a.region_code,
  a.h3_r10,

  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.poi_points_cnt,
  a.poi_classes_cnt,
  a.poi_types_cnt,

  a.amenity_cnt,
  a.shop_cnt,
  a.tourism_cnt,
  a.leisure_cnt,
  a.office_cnt,
  a.craft_cnt,
  a.man_made_cnt,
  a.emergency_cnt,
  a.public_transport_cnt,
  a.railway_cnt,
  a.highway_cnt,
  a.place_cnt,

  a.parking_cnt,
  a.mobility_services_cnt,
  a.retail_core_cnt,
  a.food_cnt,
  a.lodging_cnt,
  a.health_cnt,

  /* densities per km² */
  a.poi_points_cnt / NULLIF(c.cell_area_m2 / 1e6, 0) AS poi_points_per_km2,
  a.amenity_cnt   / NULLIF(c.cell_area_m2 / 1e6, 0) AS amenity_per_km2,
  a.shop_cnt      / NULLIF(c.cell_area_m2 / 1e6, 0) AS shop_per_km2,
  a.parking_cnt   / NULLIF(c.cell_area_m2 / 1e6, 0) AS parking_per_km2,

  a.last_load_ts
FROM agg a
JOIN cell c
  ON c.region_code = a.region_code
 AND c.h3_r10      = a.h3_r10;


/* =============================================================================
7) GOLD.FEAT_H3_POI_AREAS_R10 (fast sparse: centroid -> cell, counts only)
   Source: SILVER.POI_AREAS (polygons)
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_POI_AREAS_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH base AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(geog), 10)::STRING AS h3_r10,
    LOWER(poi_class)::STRING AS poi_class,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.POI_AREAS
  WHERE geog IS NOT NULL
    AND region_code IS NOT NULL
    AND poi_class IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,
    COUNT(*) AS poi_areas_cnt,
    COUNT_IF(poi_class='amenity')  AS amenity_areas_cnt,
    COUNT_IF(poi_class='shop')     AS shop_areas_cnt,
    COUNT_IF(poi_class='tourism')  AS tourism_areas_cnt,
    COUNT_IF(poi_class='office')   AS office_areas_cnt,
    COUNT_IF(poi_class='leisure')  AS leisure_areas_cnt,
    COUNT_IF(poi_class='sport')    AS sport_areas_cnt,
    COUNT_IF(poi_class='building') AS building_areas_cnt,
    COUNT_IF(poi_class='landuse')  AS landuse_areas_cnt,
    MAX(load_ts) AS last_load_ts
  FROM base
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
),
cell AS (
  SELECT region_code, h3_r10, cell_area_m2, cell_wkt_4326, cell_center_wkt_4326
  FROM SILVER.DIM_H3_R10_CELLS
)
SELECT
  a.region_code,
  a.h3_r10,
  c.cell_area_m2,
  c.cell_wkt_4326,
  c.cell_center_wkt_4326,

  a.poi_areas_cnt,
  a.amenity_areas_cnt,
  a.shop_areas_cnt,
  a.tourism_areas_cnt,
  a.office_areas_cnt,
  a.leisure_areas_cnt,
  a.sport_areas_cnt,
  a.building_areas_cnt,
  a.landuse_areas_cnt,

  /* densities per km² */
  a.poi_areas_cnt      / NULLIF(c.cell_area_m2 / 1e6, 0) AS poi_areas_per_km2,
  a.amenity_areas_cnt  / NULLIF(c.cell_area_m2 / 1e6, 0) AS amenity_areas_per_km2,
  a.shop_areas_cnt     / NULLIF(c.cell_area_m2 / 1e6, 0) AS shop_areas_per_km2,
  a.tourism_areas_cnt  / NULLIF(c.cell_area_m2 / 1e6, 0) AS tourism_areas_per_km2,
  a.office_areas_cnt   / NULLIF(c.cell_area_m2 / 1e6, 0) AS office_areas_per_km2,
  a.leisure_areas_cnt  / NULLIF(c.cell_area_m2 / 1e6, 0) AS leisure_areas_per_km2,
  a.sport_areas_cnt    / NULLIF(c.cell_area_m2 / 1e6, 0) AS sport_areas_per_km2,

  a.last_load_ts
FROM agg a
JOIN cell c
  ON c.region_code = a.region_code
 AND c.h3_r10      = a.h3_r10;


/* =============================================================================
8) GOLD.FEAT_H3_POI_AREAS_R10__CENTROID_XAREA (centroid candidate + exact intersection area)
   NOTE: not polyfill; cheap-ish approximation but uses ST_INTERSECTION (heavier).
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_POI_AREAS_R10__CENTROID_XAREA
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH poi AS (
  SELECT
    region_code::STRING AS region_code,
    feature_id::STRING  AS feature_id,
    poi_class::STRING   AS poi_class,
    poi_type::STRING    AS poi_type,
    geog                AS poi_geog,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(geog), 10)::STRING AS h3_r10,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.POI_AREAS
  WHERE geog IS NOT NULL
    AND region_code IS NOT NULL
    AND feature_id IS NOT NULL
    AND poi_class IS NOT NULL
    AND poi_type  IS NOT NULL
),
cells AS (
  SELECT
    region_code::STRING AS region_code,
    h3_r10::STRING      AS h3_r10,
    cell_geog           AS cell_geog,
    cell_area_m2::FLOAT AS cell_area_m2,
    cell_wkt_4326::STRING        AS cell_wkt_4326,
    cell_center_wkt_4326::STRING AS cell_center_wkt_4326
  FROM SILVER.DIM_H3_R10_CELLS
  WHERE cell_geog IS NOT NULL
    AND cell_area_m2 IS NOT NULL
    AND cell_area_m2 > 0
),
joined AS (
  SELECT
    c.region_code,
    c.h3_r10,
    c.cell_geog,
    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,
    p.feature_id,
    p.poi_class,
    p.poi_type,
    p.poi_geog,
    p.load_ts
  FROM poi p
  JOIN cells c
    ON c.region_code = p.region_code
   AND c.h3_r10      = p.h3_r10
  WHERE ST_INTERSECTS(p.poi_geog, c.cell_geog)
),
x AS (
  SELECT
    region_code,
    h3_r10,
    feature_id,
    poi_class,
    poi_type,
    ST_AREA(ST_INTERSECTION(poi_geog, cell_geog))::FLOAT AS poi_area_m2,
    load_ts,
    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326
  FROM joined
  WHERE poi_geog IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    ANY_VALUE(cell_area_m2)         AS cell_area_m2,
    ANY_VALUE(cell_wkt_4326)        AS cell_wkt_4326,
    ANY_VALUE(cell_center_wkt_4326) AS cell_center_wkt_4326,

    COUNT(DISTINCT feature_id) AS poi_areas_cnt,

    SUM(poi_area_m2) AS poi_area_m2_sum,
    SUM(poi_area_m2) / NULLIF(ANY_VALUE(cell_area_m2), 0) AS poi_area_share,

    COUNT(DISTINCT IFF(poi_class='amenity',  feature_id, NULL)) AS amenity_areas_cnt,
    COUNT(DISTINCT IFF(poi_class='shop',     feature_id, NULL)) AS shop_areas_cnt,
    COUNT(DISTINCT IFF(poi_class='tourism',  feature_id, NULL)) AS tourism_areas_cnt,
    COUNT(DISTINCT IFF(poi_class='office',   feature_id, NULL)) AS office_areas_cnt,
    COUNT(DISTINCT IFF(poi_class='leisure',  feature_id, NULL)) AS leisure_areas_cnt,
    COUNT(DISTINCT IFF(poi_class='sport',    feature_id, NULL)) AS sport_areas_cnt,
    COUNT(DISTINCT IFF(poi_class='building', feature_id, NULL)) AS building_areas_cnt,
    COUNT(DISTINCT IFF(poi_class='landuse',  feature_id, NULL)) AS landuse_areas_cnt,

    COUNT(DISTINCT feature_id) / NULLIF(ANY_VALUE(cell_area_m2) / 1e6, 0) AS poi_areas_per_km2,
    SUM(poi_area_m2)          / NULLIF(ANY_VALUE(cell_area_m2) / 1e6, 0) AS poi_area_m2_per_km2,

    MAX(load_ts) AS last_load_ts
  FROM x
  WHERE poi_area_m2 IS NOT NULL
    AND poi_area_m2 > 0
  GROUP BY 1,2
)
SELECT * FROM agg;


/* =============================================================================
9) GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS (sparse with class xarea sums/shares)
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH base AS (
  SELECT
    p.region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(ST_CENTROID(p.geog), 10)::STRING AS h3_r10,
    LOWER(p.poi_class)::STRING AS poi_class,
    p.load_ts::TIMESTAMP_NTZ AS load_ts,
    p.geog AS poi_geog
  FROM SILVER.POI_AREAS p
  WHERE p.geog IS NOT NULL
    AND p.region_code IS NOT NULL
    AND p.poi_class IS NOT NULL
),
x AS (
  SELECT
    b.region_code,
    b.h3_r10,
    b.poi_class,
    ST_AREA(ST_INTERSECTION(b.poi_geog, d.cell_geog))::FLOAT AS poi_xarea_m2,
    b.load_ts
  FROM base b
  JOIN SILVER.DIM_H3_R10_CELLS d
    ON d.region_code = b.region_code
   AND d.h3_r10      = b.h3_r10
  WHERE ST_INTERSECTS(b.poi_geog, d.cell_geog)
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    COUNT(*) AS poi_areas_cnt,
    SUM(poi_xarea_m2) AS poi_xarea_m2_sum,

    /* counts by class */
    SUM(IFF(poi_class='amenity', 1, 0)) AS poi_amenity_cnt,
    SUM(IFF(poi_class='shop',    1, 0)) AS poi_shop_cnt,
    SUM(IFF(poi_class='tourism', 1, 0)) AS poi_tourism_cnt,
    SUM(IFF(poi_class='office',  1, 0)) AS poi_office_cnt,
    SUM(IFF(poi_class='leisure', 1, 0)) AS poi_leisure_cnt,
    SUM(IFF(poi_class='sport',   1, 0)) AS poi_sport_cnt,
    SUM(IFF(poi_class='building',1, 0)) AS poi_building_cnt,
    SUM(IFF(poi_class='landuse', 1, 0)) AS poi_landuse_cnt,

    /* xarea sums by class */
    SUM(IFF(poi_class='amenity',  poi_xarea_m2, 0)) AS poi_amenity_xarea_m2_sum,
    SUM(IFF(poi_class='shop',     poi_xarea_m2, 0)) AS poi_shop_xarea_m2_sum,
    SUM(IFF(poi_class='tourism',  poi_xarea_m2, 0)) AS poi_tourism_xarea_m2_sum,
    SUM(IFF(poi_class='office',   poi_xarea_m2, 0)) AS poi_office_xarea_m2_sum,
    SUM(IFF(poi_class='leisure',  poi_xarea_m2, 0)) AS poi_leisure_xarea_m2_sum,
    SUM(IFF(poi_class='sport',    poi_xarea_m2, 0)) AS poi_sport_xarea_m2_sum,
    SUM(IFF(poi_class='building', poi_xarea_m2, 0)) AS poi_building_xarea_m2_sum,
    SUM(IFF(poi_class='landuse',  poi_xarea_m2, 0)) AS poi_landuse_xarea_m2_sum,

    MAX(load_ts) AS last_load_ts
  FROM x
  GROUP BY 1,2
),
final AS (
  SELECT
    a.region_code,
    a.h3_r10,

    d.cell_area_m2,
    d.cell_wkt_4326,
    d.cell_center_wkt_4326,

    TRUE AS has_poi_areas,

    a.poi_areas_cnt,
    a.poi_xarea_m2_sum,
    a.poi_xarea_m2_sum / NULLIF(d.cell_area_m2, 0) AS poi_xarea_share,

    a.poi_areas_cnt / NULLIF(d.cell_area_m2 / 1e6, 0) AS poi_areas_per_km2,
    a.poi_xarea_m2_sum / NULLIF(d.cell_area_m2 / 1e6, 0) AS poi_xarea_m2_per_km2,

    a.poi_amenity_cnt, a.poi_shop_cnt, a.poi_tourism_cnt, a.poi_office_cnt,
    a.poi_leisure_cnt, a.poi_sport_cnt, a.poi_building_cnt, a.poi_landuse_cnt,

    a.poi_amenity_cnt / NULLIF(a.poi_areas_cnt, 0) AS poi_amenity_share,
    a.poi_shop_cnt    / NULLIF(a.poi_areas_cnt, 0) AS poi_shop_share,
    a.poi_tourism_cnt / NULLIF(a.poi_areas_cnt, 0) AS poi_tourism_share,
    a.poi_office_cnt  / NULLIF(a.poi_areas_cnt, 0) AS poi_office_share,
    a.poi_leisure_cnt / NULLIF(a.poi_areas_cnt, 0) AS poi_leisure_share,
    a.poi_sport_cnt   / NULLIF(a.poi_areas_cnt, 0) AS poi_sport_share,
    a.poi_building_cnt/ NULLIF(a.poi_areas_cnt, 0) AS poi_building_share,
    a.poi_landuse_cnt / NULLIF(a.poi_areas_cnt, 0) AS poi_landuse_share,

    a.poi_amenity_xarea_m2_sum, a.poi_shop_xarea_m2_sum, a.poi_tourism_xarea_m2_sum, a.poi_office_xarea_m2_sum,
    a.poi_leisure_xarea_m2_sum, a.poi_sport_xarea_m2_sum, a.poi_building_xarea_m2_sum, a.poi_landuse_xarea_m2_sum,

    a.poi_amenity_xarea_m2_sum / NULLIF(d.cell_area_m2, 0) AS poi_amenity_xarea_share,
    a.poi_shop_xarea_m2_sum    / NULLIF(d.cell_area_m2, 0) AS poi_shop_xarea_share,
    a.poi_tourism_xarea_m2_sum / NULLIF(d.cell_area_m2, 0) AS poi_tourism_xarea_share,
    a.poi_office_xarea_m2_sum  / NULLIF(d.cell_area_m2, 0) AS poi_office_xarea_share,
    a.poi_leisure_xarea_m2_sum / NULLIF(d.cell_area_m2, 0) AS poi_leisure_xarea_share,
    a.poi_sport_xarea_m2_sum   / NULLIF(d.cell_area_m2, 0) AS poi_sport_xarea_share,
    a.poi_building_xarea_m2_sum/ NULLIF(d.cell_area_m2, 0) AS poi_building_xarea_share,
    a.poi_landuse_xarea_m2_sum / NULLIF(d.cell_area_m2, 0) AS poi_landuse_xarea_share,

    a.last_load_ts
  FROM agg a
  JOIN SILVER.DIM_H3_R10_CELLS d
    ON d.region_code = a.region_code
   AND d.h3_r10      = a.h3_r10
)
SELECT * FROM final;


/* =============================================================================
10) GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS_DENSE (dense ML-ready)
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS_DENSE
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
SELECT
  d.region_code,
  d.h3_r10,
  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

  IFF(s.region_code IS NULL, FALSE, TRUE) AS has_poi_areas,

  COALESCE(s.poi_areas_cnt, 0)      AS poi_areas_cnt,
  COALESCE(s.poi_xarea_m2_sum, 0)   AS poi_xarea_m2_sum,
  COALESCE(s.poi_xarea_share, 0)    AS poi_xarea_share,

  COALESCE(s.poi_areas_per_km2, 0)      AS poi_areas_per_km2,
  COALESCE(s.poi_xarea_m2_per_km2, 0)   AS poi_xarea_m2_per_km2,

  COALESCE(s.poi_amenity_cnt, 0)  AS poi_amenity_cnt,
  COALESCE(s.poi_shop_cnt, 0)     AS poi_shop_cnt,
  COALESCE(s.poi_tourism_cnt, 0)  AS poi_tourism_cnt,
  COALESCE(s.poi_office_cnt, 0)   AS poi_office_cnt,
  COALESCE(s.poi_leisure_cnt, 0)  AS poi_leisure_cnt,
  COALESCE(s.poi_sport_cnt, 0)    AS poi_sport_cnt,
  COALESCE(s.poi_building_cnt, 0) AS poi_building_cnt,
  COALESCE(s.poi_landuse_cnt, 0)  AS poi_landuse_cnt,

  COALESCE(s.poi_amenity_share, 0)  AS poi_amenity_share,
  COALESCE(s.poi_shop_share, 0)     AS poi_shop_share,
  COALESCE(s.poi_tourism_share, 0)  AS poi_tourism_share,
  COALESCE(s.poi_office_share, 0)   AS poi_office_share,
  COALESCE(s.poi_leisure_share, 0)  AS poi_leisure_share,
  COALESCE(s.poi_sport_share, 0)    AS poi_sport_share,
  COALESCE(s.poi_building_share, 0) AS poi_building_share,
  COALESCE(s.poi_landuse_share, 0)  AS poi_landuse_share,

  COALESCE(s.poi_amenity_xarea_m2_sum, 0)  AS poi_amenity_xarea_m2_sum,
  COALESCE(s.poi_shop_xarea_m2_sum, 0)     AS poi_shop_xarea_m2_sum,
  COALESCE(s.poi_tourism_xarea_m2_sum, 0)  AS poi_tourism_xarea_m2_sum,
  COALESCE(s.poi_office_xarea_m2_sum, 0)   AS poi_office_xarea_m2_sum,
  COALESCE(s.poi_leisure_xarea_m2_sum, 0)  AS poi_leisure_xarea_m2_sum,
  COALESCE(s.poi_sport_xarea_m2_sum, 0)    AS poi_sport_xarea_m2_sum,
  COALESCE(s.poi_building_xarea_m2_sum, 0) AS poi_building_xarea_m2_sum,
  COALESCE(s.poi_landuse_xarea_m2_sum, 0)  AS poi_landuse_xarea_m2_sum,

  COALESCE(s.poi_amenity_xarea_share, 0)  AS poi_amenity_xarea_share,
  COALESCE(s.poi_shop_xarea_share, 0)     AS poi_shop_xarea_share,
  COALESCE(s.poi_tourism_xarea_share, 0)  AS poi_tourism_xarea_share,
  COALESCE(s.poi_office_xarea_share, 0)   AS poi_office_xarea_share,
  COALESCE(s.poi_leisure_xarea_share, 0)  AS poi_leisure_xarea_share,
  COALESCE(s.poi_sport_xarea_share, 0)    AS poi_sport_xarea_share,
  COALESCE(s.poi_building_xarea_share, 0) AS poi_building_xarea_share,
  COALESCE(s.poi_landuse_xarea_share, 0)  AS poi_landuse_xarea_share,

  /* last_load_ts intentionally nullable for empty cells */
  s.last_load_ts
FROM SILVER.DIM_H3_R10_CELLS d
LEFT JOIN GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS s
  ON s.region_code = d.region_code
 AND s.h3_r10      = d.h3_r10;


/* =============================================================================
11) GOLD.FEAT_H3_TRANSIT_POINTS_R10
   Source: SILVER.TRANSIT_POINTS (points)
============================================================================= */

CREATE OR REPLACE DYNAMIC TABLE GOLD.FEAT_H3_TRANSIT_POINTS_R10
  TARGET_LAG = '48 hours'
  WAREHOUSE  = COMPUTE_WH
  CLUSTER BY (region_code, h3_r10)
AS
WITH pts AS (
  SELECT
    region_code::STRING AS region_code,
    H3_POINT_TO_CELL_STRING(geog, 10)::STRING AS h3_r10,
    poi_class::STRING AS poi_class,
    poi_type::STRING  AS poi_type,
    load_ts::TIMESTAMP_NTZ AS load_ts
  FROM SILVER.TRANSIT_POINTS
  WHERE geog IS NOT NULL
    AND region_code IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    COUNT(*) AS transit_points_cnt,
    SUM(IFF(poi_class='transport', 1, 0)) AS transport_points_cnt,
    SUM(IFF(poi_class='amenity',   1, 0)) AS amenity_points_cnt,
    SUM(IFF(poi_class='emergency', 1, 0)) AS emergency_points_cnt,

    SUM(IFF(LOWER(poi_type) IN ('station','halt','tram_stop','subway_entrance'), 1, 0)) AS station_like_cnt,
    SUM(IFF(LOWER(poi_type) IN ('bus_stop','platform'), 1, 0)) AS stop_like_cnt,

    MAX(load_ts) AS last_load_ts
  FROM pts
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2
)
SELECT
  d.region_code,
  d.h3_r10,
  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

  COALESCE(a.transit_points_cnt, 0) AS transit_points_cnt,
  COALESCE(a.transport_points_cnt, 0) AS transport_points_cnt,
  COALESCE(a.amenity_points_cnt, 0) AS amenity_points_cnt,
  COALESCE(a.emergency_points_cnt, 0) AS emergency_points_cnt,
  COALESCE(a.station_like_cnt, 0) AS station_like_cnt,
  COALESCE(a.stop_like_cnt, 0) AS stop_like_cnt,

  IFF(COALESCE(a.transit_points_cnt,0)=0, NULL, a.transport_points_cnt / NULLIF(a.transit_points_cnt,0))::FLOAT AS transport_points_share,
  IFF(COALESCE(a.transit_points_cnt,0)=0, NULL, a.emergency_points_cnt / NULLIF(a.transit_points_cnt,0))::FLOAT AS emergency_points_share,

  (COALESCE(a.transit_points_cnt,0)   * 1e6 / NULLIF(d.cell_area_m2,0))::FLOAT AS transit_points_per_km2,
  (COALESCE(a.transport_points_cnt,0) * 1e6 / NULLIF(d.cell_area_m2,0))::FLOAT AS transport_points_per_km2,

  a.last_load_ts,
  IFF(COALESCE(a.transit_points_cnt, 0) > 0, TRUE, FALSE) AS has_transit_points
FROM SILVER.DIM_H3_R10_CELLS d
LEFT JOIN agg a
  ON a.region_code = d.region_code
 AND a.h3_r10      = d.h3_r10;


-- =============================================================================
-- 12) REQUIRED CHECKS (mandatory): rowcount + WKT samples (geo tables)
-- =============================================================================

-- GOLD.FEAT_H3_POP_R10
SELECT COUNT(*) AS rows_gold_feat_h3_pop_r10 FROM GOLD.FEAT_H3_POP_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_POP_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_BUILDINGS_R10
SELECT COUNT(*) AS rows_gold_feat_h3_buildings_r10 FROM GOLD.FEAT_H3_BUILDINGS_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_BUILDINGS_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_CHARGING_R10
SELECT COUNT(*) AS rows_gold_feat_h3_charging_r10 FROM GOLD.FEAT_H3_CHARGING_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_CHARGING_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_DEGURBA_R10
SELECT COUNT(*) AS rows_gold_feat_h3_degurba_r10 FROM GOLD.FEAT_H3_DEGURBA_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_DEGURBA_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_ROADS_R10
SELECT COUNT(*) AS rows_gold_feat_h3_roads_r10 FROM GOLD.FEAT_H3_ROADS_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_ROADS_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_POI_POINTS_R10
SELECT COUNT(*) AS rows_gold_feat_h3_poi_points_r10 FROM GOLD.FEAT_H3_POI_POINTS_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_POI_POINTS_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_POI_AREAS_R10
SELECT COUNT(*) AS rows_gold_feat_h3_poi_areas_r10 FROM GOLD.FEAT_H3_POI_AREAS_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_POI_AREAS_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_POI_AREAS_R10__CENTROID_XAREA
SELECT COUNT(*) AS rows_gold_feat_h3_poi_areas_centroid_xarea FROM GOLD.FEAT_H3_POI_AREAS_R10__CENTROID_XAREA;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_POI_AREAS_R10__CENTROID_XAREA
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS
SELECT COUNT(*) AS rows_gold_feat_h3_poi_areas_xarea_classbuckets FROM GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS_DENSE
SELECT COUNT(*) AS rows_gold_feat_h3_poi_areas_xarea_classbuckets_dense FROM GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS_DENSE;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS_DENSE
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;

-- GOLD.FEAT_H3_TRANSIT_POINTS_R10
SELECT COUNT(*) AS rows_gold_feat_h3_transit_points_r10 FROM GOLD.FEAT_H3_TRANSIT_POINTS_R10;
SELECT region_code, h3_r10, LEFT(cell_wkt_4326, 200) AS cell_wkt_prefix, cell_center_wkt_4326
FROM GOLD.FEAT_H3_TRANSIT_POINTS_R10
WHERE cell_wkt_4326 IS NOT NULL
LIMIT 5;