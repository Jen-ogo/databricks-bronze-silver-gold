-- =============================================================================
-- GEO_PROJECT / Databricks GOLD (Delta)
-- Style: Snowflake-like headings + bootstrap + checks + optimizations
-- =============================================================================

USE CATALOG geo_databricks_sub;
CREATE SCHEMA IF NOT EXISTS gold;
USE SCHEMA gold;

-- -----------------------------------------------------------------------------
-- 0) DROP (idempotent)
-- -----------------------------------------------------------------------------


-- DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_charging_r10; 
/* =============================================================================
GOLD: FEAT_H3_BUILDINGS_R10
Source   : geo_databricks_sub.silver.building_footprints_model
Grain    : (region_code, region, h3_r10)
H3       : r10 from BUILDING centroid (same idea as Snowflake “h3_r10 provided”)
Cell     : computed in GOLD (boundary/center/area) like Snowflake cell CTE
Area m2  : computed in meters using EPSG:3035 (so densities/share are sane)
============================================================================= */


DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_buildings_r10;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_buildings_r10
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH b AS (
  SELECT
    CAST(country AS STRING) AS region_code,
    CAST(region  AS STRING) AS region,

    LOWER(CAST(building_type AS STRING))        AS building_type,
    COALESCE(CAST(building_levels AS INT), 1)   AS building_levels,

    /* building geometry (EPSG:4326) */
    CASE
      WHEN geom_wkb IS NOT NULL THEN ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326)
      ELSE ST_SetSRID(ST_GeomFromWKT(geom_wkt_4326), 4326)
    END AS geom_4326,

    /* H3 r10 from centroid (string) */
    h3_pointash3string(
      ST_AsText(
        ST_Centroid(
          CASE
            WHEN geom_wkb IS NOT NULL THEN ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326)
            ELSE ST_SetSRID(ST_GeomFromWKT(geom_wkt_4326), 4326)
          END
        )
      ),
      10
    ) AS h3_r10,

    CAST(load_ts AS TIMESTAMP) AS load_ts
  FROM geo_databricks_sub.silver.building_footprints_model
  WHERE (geom_wkb IS NOT NULL OR geom_wkt_4326 IS NOT NULL)
),

cell AS (
  SELECT DISTINCT
    region_code,
    region,
    h3_r10,

    /* Snowflake-like: boundary + center as WKT */
    h3_boundaryaswkt(h3_stringtoh3(h3_r10)) AS cell_wkt_4326,
    h3_centeraswkt(h3_stringtoh3(h3_r10))   AS cell_center_wkt_4326,

    /* IMPORTANT: area in m2 (EPSG:3035) */
    CAST(
      ST_Area(
        ST_Transform(
          ST_SetSRID(ST_GeomFromWKT(h3_boundaryaswkt(h3_stringtoh3(h3_r10))), 4326),
          3035
        )
      ) AS DOUBLE
    ) AS cell_area_m2
  FROM b
  WHERE h3_r10 IS NOT NULL
),

b2 AS (
  SELECT
    region_code,
    region,
    h3_r10,
    building_levels,

    /* IMPORTANT: footprint area in m2 (EPSG:3035) */
    CAST(
      ST_Area(
        ST_Transform(geom_4326, 3035)
      ) AS DOUBLE
    ) AS footprint_area_m2,

    CASE
      WHEN building_type IN (
        'house','detached','apartments','residential','semidetached_house','terrace',
        'bungalow','dormitory'
      ) THEN 'residential'

      WHEN building_type IN (
        'retail','commercial','office','industrial','manufacture','warehouse','service',
        'school','kindergarten','university','hospital','fire_station','government',
        'supermarket','hotel','train_station','church','chapel'
      ) THEN 'nonresidential'

      WHEN building_type = 'yes' THEN 'unknown'
      ELSE 'other'
    END AS building_group,

    load_ts
  FROM b
  WHERE h3_r10 IS NOT NULL
    AND geom_4326 IS NOT NULL
),

agg AS (
  SELECT
    region_code,
    region,
    h3_r10,

    COUNT(1) AS buildings_cnt,
    SUM(CASE WHEN building_group = 'residential'    THEN 1 ELSE 0 END) AS res_buildings_cnt,
    SUM(CASE WHEN building_group = 'nonresidential' THEN 1 ELSE 0 END) AS nonres_buildings_cnt,
    SUM(CASE WHEN building_group = 'unknown'        THEN 1 ELSE 0 END) AS unknown_buildings_cnt,

    SUM(footprint_area_m2) AS footprint_area_m2_sum,
    SUM(footprint_area_m2 * CAST(building_levels AS DOUBLE)) AS floor_area_m2_est_sum,

    AVG(CAST(building_levels AS DOUBLE)) AS levels_avg,
    percentile_approx(building_levels, 0.5) AS levels_p50,

    percentile_approx(footprint_area_m2, 0.5) AS footprint_area_p50_m2,
    percentile_approx(footprint_area_m2, 0.9) AS footprint_area_p90_m2,

    MAX(load_ts) AS last_load_ts
  FROM b2
  GROUP BY 1,2,3
)

SELECT
  c.region_code,
  c.region,
  c.h3_r10,

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

  /* densities (same as Snowflake) */
  CAST(a.buildings_cnt AS DOUBLE) / NULLIF(c.cell_area_m2 / 1e6, 0)         AS buildings_per_km2,
  a.footprint_area_m2_sum          / NULLIF(c.cell_area_m2 / 1e6, 0)         AS footprint_m2_per_km2,
  a.floor_area_m2_est_sum          / NULLIF(c.cell_area_m2 / 1e6, 0)         AS floor_area_m2_per_km2,

  /* built-up share */
  a.footprint_area_m2_sum          / NULLIF(c.cell_area_m2, 0)               AS built_up_share,

  a.last_load_ts
FROM cell c
JOIN agg a
  ON a.region_code = c.region_code
 AND a.region      = c.region
 AND a.h3_r10       = c.h3_r10
;

/* =============================================================================
Checks (commented)
============================================================================= */

-- rowcount
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.gold.feat_h3_buildings_r10;

-- nulls
-- SELECT
--   SUM(CASE WHEN region_code IS NULL THEN 1 ELSE 0 END) AS region_code_nulls,
--   SUM(CASE WHEN region      IS NULL THEN 1 ELSE 0 END) AS region_nulls,
--   SUM(CASE WHEN h3_r10      IS NULL THEN 1 ELSE 0 END) AS h3_nulls,
--   SUM(CASE WHEN cell_area_m2 IS NULL OR cell_area_m2<=0 THEN 1 ELSE 0 END) AS bad_area_cnt,
--   SUM(CASE WHEN cell_wkt_4326 IS NULL THEN 1 ELSE 0 END) AS cell_wkt_nulls
-- FROM geo_databricks_sub.gold.feat_h3_buildings_r10;

-- WKT prefixes
-- SELECT region_code, region, h3_r10, substring(cell_wkt_4326,1,120) AS wkt_prefix
-- FROM geo_databricks_sub.gold.feat_h3_buildings_r10
-- WHERE cell_wkt_4326 IS NOT NULL
-- LIMIT 20;

-- sanity: built_up_share should be [0..1] (allow tiny numeric eps)
-- SELECT COUNT(*) AS bad_cnt
-- FROM geo_databricks_sub.gold.feat_h3_buildings_r10
-- WHERE built_up_share IS NOT NULL AND (built_up_share < -1e-9 OR built_up_share > 1.000000001);

--   select
--   region_code,
--   h3_r10,
--   cell_area_m2,
--   buildings_cnt,
--   buildings_per_km2,
--   footprint_m2_per_km2,
--   floor_area_m2_per_km2,
--   built_up_share,
--   levels_avg,
--   levels_p50,

--   case
--     when cell_area_m2 is null or cell_area_m2 <= 0 then 'cell_area_m2_null_or_le0'
--     when buildings_cnt is null or buildings_cnt < 0 then 'buildings_cnt_null_or_lt0'

--     when buildings_per_km2 is not null and buildings_per_km2 < 0 then 'buildings_per_km2_lt0'
--     when footprint_m2_per_km2 is not null and footprint_m2_per_km2 < 0 then 'footprint_m2_per_km2_lt0'
--     when floor_area_m2_per_km2 is not null and floor_area_m2_per_km2 < 0 then 'floor_area_m2_per_km2_lt0'

--     when built_up_share is not null and built_up_share < 0 then 'built_up_share_lt0'
--     when built_up_share is not null and built_up_share > 5 then 'built_up_share_gt5'

--     when levels_avg is not null and levels_avg < 0 then 'levels_avg_lt0'
--     when levels_p50 is not null and levels_p50 < 0 then 'levels_p50_lt0'
--     else 'unknown'
--   end as fail_reason

-- from GOLD.FEAT_H3_BUILDINGS_R10
-- where
--   cell_area_m2 is null or cell_area_m2 <= 0
--   or buildings_cnt is null or buildings_cnt < 0
--   or (buildings_per_km2 is not null and buildings_per_km2 < 0)
--   or (footprint_m2_per_km2 is not null and footprint_m2_per_km2 < 0)
--   or (floor_area_m2_per_km2 is not null and floor_area_m2_per_km2 < 0)
--   or (built_up_share is not null and (built_up_share < 0 or built_up_share > 5))
--   or (levels_avg is not null and levels_avg < 0)
--   or (levels_p50 is not null and levels_p50 < 0);



-- sanity: cell area stats (H3 r10 should be stable-ish)
-- SELECT
--   min(cell_area_m2) AS min_area_m2,
--   avg(cell_area_m2) AS avg_area_m2,
--   max(cell_area_m2) AS max_area_m2
-- FROM geo_databricks_sub.gold.feat_h3_buildings_r10;

/* =============================================================================
Optimize / Z-Order / Analyze
NOTE: do NOT ZORDER by partition columns; ZORDER by h3_r10 is fine.
============================================================================= */

OPTIMIZE geo_databricks_sub.gold.feat_h3_buildings_r10 ZORDER BY (h3_r10);

ANALYZE TABLE geo_databricks_sub.gold.feat_h3_buildings_r10
COMPUTE STATISTICS FOR COLUMNS region_code, region, h3_r10;


/* =============================================================================
GOLD: FEAT_H3_POP_R10_FINAL
Source   : geo_databricks_sub.silver.feat_h3_pop_r10
Target   : geo_databricks_sub.gold.feat_h3_pop_r10_final
Grain    : (region_code, h3_r10)
Geo      : cell_wkt_4326 / cell_center_wkt_4326 (WKT, EPSG:4326)
Compute  :
  - cell_area_m2 = ST_Area(ST_GeogFromWKT(cell_wkt_4326))   -- meters² (geography)
  - support_area_m2 = grid_cells_cnt * 1,000,000
  - densities like Snowflake
============================================================================= */


DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_pop_r10_final;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_pop_r10_final
USING DELTA
PARTITIONED BY (region_code)
AS
WITH s AS (
  SELECT *
  FROM geo_databricks_sub.silver.feat_h3_pop_r10
)
SELECT
  region_code,
  h3_r10,                              

  /* geo computed from H3 directly (DIM) */
  h3_boundaryaswkt(h3_r10) AS cell_wkt_4326,
  h3_centeraswkt(h3_r10)   AS cell_center_wkt_4326,

  /* area in m2 (equal-area projection) */
  ST_Area(
    ST_Transform(
      ST_SetSRID(ST_GeomFromWKT(h3_boundaryaswkt(h3_r10)), 4326),
      3035
    )
  ) AS cell_area_m2,

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

  /* support area */
  CAST(grid_cells_cnt * 1000000 AS DOUBLE) AS support_area_m2,
  CAST('census_grid_1km_to_h3_admin4' AS STRING) AS pop_method,

  /* densities by GRID support (km2) */
  CAST(pop_total AS DOUBLE) / NULLIF(CAST(grid_cells_cnt AS DOUBLE), 0.0) AS pop_per_km2_support,
  CAST(emp_total AS DOUBLE) / NULLIF(CAST(grid_cells_cnt AS DOUBLE), 0.0) AS emp_per_km2_support,

  /* densities by HEX area (km2) */
  CAST(pop_total AS DOUBLE) / NULLIF(cell_area_m2 / 1e6, 0.0) AS pop_per_km2_hex,
  CAST(emp_total AS DOUBLE) / NULLIF(cell_area_m2 / 1e6, 0.0) AS emp_per_km2_hex,

  last_load_ts
FROM s;

/* =============================================================================
Checks (commented)
============================================================================= */

-- rowcount
-- SELECT COUNT(*) AS rows_cnt
-- FROM geo_databricks_sub.gold.feat_h3_pop_r10_final;

-- select pop_per_km2_support, emp_per_km2_support, pop_per_km2_hex, emp_per_km2_hex from GOLD.FEAT_H3_POP_R10_FINAL order by pop_per_km2_support desc limit 100;

-- area sanity (m²): H3 r10 ~ ~0.014 km² => ~14044 m² 
-- SELECT
--   MIN(cell_area_m2) AS min_m2,
--   percentile_approx(cell_area_m2, 0.5) AS p50_m2,
--   MAX(cell_area_m2) AS max_m2
-- FROM geo_databricks_sub.gold.feat_h3_pop_r10_final;

-- -- shares in [0..1]
-- SELECT COUNT(*) AS bad_cnt
-- FROM geo_databricks_sub.gold.feat_h3_pop_r10_final
-- WHERE (share_age_ge65 IS NOT NULL AND (share_age_ge65 < 0 OR share_age_ge65 > 1))
--    OR (share_age_lt15 IS NOT NULL AND (share_age_lt15 < 0 OR share_age_lt15 > 1))
--    OR (share_emp      IS NOT NULL AND (share_emp      < 0 OR share_emp      > 1));

/* =============================================================================
Optimize / Z-Order / Analyze
Note: ZORDER not on partition col (region_code) — use h3_r10.
============================================================================= */

OPTIMIZE geo_databricks_sub.gold.feat_h3_pop_r10_final
ZORDER BY (h3_r10);

ANALYZE TABLE geo_databricks_sub.gold.feat_h3_pop_r10_final
COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10;




-- =============================================================================
-- GOLD.FEAT_H3_CHARGING_R10
-- Source:
--   - SILVER.EV_CHARGERS (points)
--   - SILVER.FEAT_H3_POP_R10 (pop by H3 BIGINT from grid polyfill)
--   - SILVER.DIM_H3_R10_CELLS (canonical H3 STRING + cell geo)
-- Key  : (region_code, h3_r10) where h3_r10 is STRING
-- =============================================================================
DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_charging_r10;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_charging_r10
USING DELTA
PARTITIONED BY (region_code)
AS
WITH ch AS (
  SELECT
    CAST(country AS STRING) AS region_code,

    /* get H3 as STRING then convert safely to BIGINT */
    h3_stringtoh3(
      h3_pointash3string(
        CASE WHEN geom_wkb IS NOT NULL THEN geom_wkb ELSE geom_wkt_4326 END,
        10
      )
    ) AS h3_r10,                        -- BIGINT

    CAST(COALESCE(total_sockets_cnt, 0) AS INT) AS total_sockets_cnt,
    CASE WHEN has_dc = TRUE THEN 1 ELSE 0 END AS is_dc,
    CASE WHEN has_ac = TRUE THEN 1 ELSE 0 END AS is_ac,
    CAST(load_ts AS TIMESTAMP) AS load_ts
  FROM geo_databricks_sub.silver.ev_chargers
  WHERE geom_wkb IS NOT NULL OR geom_wkt_4326 IS NOT NULL
),
agg AS (
  SELECT
    region_code,
    h3_r10,

    COUNT(*) AS chargers_cnt,
    SUM(total_sockets_cnt) AS sockets_cnt_sum,
    SUM(is_dc) AS chargers_dc_cnt,
    SUM(is_ac) AS chargers_ac_cnt,
    CASE WHEN COUNT(*) > 0 THEN SUM(is_dc) / COUNT(*) END AS dc_share,

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
  FROM geo_databricks_sub.gold.feat_h3_pop_r10_final
)
SELECT
  p.region_code,
  p.h3_r10,

  p.cell_area_m2,
  p.cell_wkt_4326,
  p.cell_center_wkt_4326,

  COALESCE(a.chargers_cnt, 0)    AS chargers_cnt,
  COALESCE(a.sockets_cnt_sum, 0) AS sockets_cnt_sum,
  COALESCE(a.chargers_dc_cnt, 0) AS chargers_dc_cnt,
  COALESCE(a.chargers_ac_cnt, 0) AS chargers_ac_cnt,
  a.dc_share,

  p.pop_total,

  CASE WHEN p.pop_total > 0 THEN COALESCE(a.chargers_cnt,0)    * 10000.0 / p.pop_total END AS chargers_per_10k_pop,
  CASE WHEN p.pop_total > 0 THEN COALESCE(a.sockets_cnt_sum,0) * 10000.0 / p.pop_total END AS sockets_per_10k_pop,

  COALESCE(a.chargers_cnt,0)    / NULLIF(p.cell_area_m2/1e6, 0.0) AS chargers_per_km2,
  COALESCE(a.sockets_cnt_sum,0) / NULLIF(p.cell_area_m2/1e6, 0.0) AS sockets_per_km2,

  a.last_load_ts
FROM pop p
LEFT JOIN agg a
  ON a.region_code = p.region_code
 AND a.h3_r10      = p.h3_r10
;

/* -----------------------------------------------------------------------------
Checks (commented)
----------------------------------------------------------------------------- */
-- SELECT COUNT(*) AS cnt FROM geo_databricks_sub.gold.feat_h3_charging_r10;

-- SELECT region_code, h3_r10, cell_center_wkt_4326, cell_wkt_4326
-- FROM geo_databricks_sub.gold.feat_h3_charging_r10
-- LIMIT 10;

-- SELECT
--   SUM(CASE WHEN h3_r10 IS NULL THEN 1 ELSE 0 END)              AS h3_null,
--   SUM(CASE WHEN cell_wkt_4326 IS NULL THEN 1 ELSE 0 END)       AS cell_wkt_null,
--   SUM(CASE WHEN pop_total IS NULL THEN 1 ELSE 0 END)           AS pop_null,
--   SUM(CASE WHEN chargers_cnt < 0 OR sockets_cnt_sum < 0 THEN 1 ELSE 0 END) AS neg_supply,
--   SUM(CASE WHEN chargers_per_10k_pop < 0 OR sockets_per_10k_pop < 0 THEN 1 ELSE 0 END) AS neg_rates
-- FROM geo_databricks_sub.gold.feat_h3_charging_r10;

-- SELECT * FROM geo_databricks_sub.gold.feat_h3_charging_r10 ORDER BY chargers_cnt DESC LIMIT 50;

/* -----------------------------------------------------------------------------
Optimize / Z-Order / Analyze
Notes:
- Do NOT ZORDER by partition column (region_code). ZORDER by h3_r10 for pruning.
----------------------------------------------------------------------------- */
OPTIMIZE geo_databricks_sub.gold.feat_h3_charging_r10 ZORDER BY (h3_r10);

ANALYZE TABLE geo_databricks_sub.gold.feat_h3_charging_r10 COMPUTE STATISTICS;
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_charging_r10 COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10;



/* =============================================================================
GOLD: FEAT_H3_DEGURBA_R10
Source   : SILVER.FEAT_H3_DEGURBA_R10  (region_code, h3_r10, year, degurba, lau_id, lau_name, last_load_ts)
Cells    : SILVER.DIM_H3_R10_CELLS     (region_code, h3_r10, cell_area_m2, cell_wkt_4326, cell_center_wkt_4326)
Target   : GOLD.FEAT_H3_DEGURBA_R10 (Delta)
Logic:
	•	for each cell, take one record: the most recent year, then (if needed) the most recent last_load_ts
	•	join to the full set of cells to keep all cells (even if degurba is NULL)
H3 types :
  - в silver.dim_h3_r10_cells h3_r10 = STRING
  - в silver.feat_h3_degurba_r10 h3_r10 = BIGINT 
  => join через h3_stringtoh3(c.h3_r10) = s.h3_r10
============================================================================= */

DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_degurba_r10;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_degurba_r10
USING DELTA
PARTITIONED BY (region_code)
AS
WITH s0 AS (
  SELECT
    CAST(region_code AS STRING)         AS region_code,
    CAST(h3_r10 AS STRING)              AS h3_r10,
    TRY_CAST(year AS INT)               AS year,
    CAST(degurba AS INT)                AS degurba,
    CAST(lau_id AS STRING)              AS lau_id,
    CAST(lau_name AS STRING)            AS lau_name,
    CAST(last_load_ts AS TIMESTAMP)     AS last_load_ts
  FROM geo_databricks_sub.silver.feat_h3_degurba_r10
  WHERE region_code IS NOT NULL
    AND h3_r10 IS NOT NULL
),

s AS (
  SELECT *
  FROM (
    SELECT
      s0.*,
      ROW_NUMBER() OVER (
        PARTITION BY region_code, h3_r10
        ORDER BY year DESC NULLS LAST, last_load_ts DESC NULLS LAST
      ) AS rn
    FROM s0
  ) t
  WHERE rn = 1
),

cells AS (
  SELECT
    CAST(region_code AS STRING)         AS region_code,
    CAST(h3_r10 AS STRING)              AS h3_r10,
    CAST(cell_area_m2 AS DOUBLE)        AS cell_area_m2,
    CAST(cell_wkt_4326 AS STRING)       AS cell_wkt_4326,
    CAST(cell_center_wkt_4326 AS STRING)AS cell_center_wkt_4326
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
  WHERE region_code IS NOT NULL
    AND h3_r10 IS NOT NULL
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

  /* one-hot */
  CASE WHEN s.degurba = 1 THEN 1 ELSE 0 END AS degurba_1_city,
  CASE WHEN s.degurba = 2 THEN 1 ELSE 0 END AS degurba_2_towns_suburbs,
  CASE WHEN s.degurba = 3 THEN 1 ELSE 0 END AS degurba_3_rural,

  s.last_load_ts
FROM cells c
LEFT JOIN s
  ON s.region_code = c.region_code
 AND s.h3_r10      = c.h3_r10
;

-- -----------------------------------------------------------------------------
-- Checks (commented)
-- -----------------------------------------------------------------------------
-- SELECT COUNT(*) AS rows_cnt
-- FROM geo_databricks_sub.gold.feat_h3_degurba_r10;

-- SELECT
--   COUNT(*) AS rows_cnt,
--   SUM(CASE WHEN degurba IS NULL THEN 1 ELSE 0 END) AS degurba_null_cnt
-- FROM geo_databricks_sub.gold.feat_h3_degurba_r10;

-- SELECT region_code,
--        COUNT(*) AS cnt,
--        SUM(CASE WHEN degurba IS NULL THEN 1 ELSE 0 END) AS nulls
-- FROM geo_databricks_sub.gold.feat_h3_degurba_r10
-- GROUP BY 1
-- ORDER BY cnt DESC;

-- sample: cells with missing degurba
-- SELECT c.region_code, c.h3_r10 AS h3_r10, c.cell_center_wkt_4326
-- FROM geo_databricks_sub.silver.dim_h3_r10_cells c
-- LEFT JOIN geo_databricks_sub.silver.feat_h3_degurba_r10 d
--   ON d.region_code = c.region_code
--  AND d.h3_r10 = h3_stringtoh3(c.h3_r10)
-- WHERE d.degurba IS NULL
-- LIMIT 50;

-- -----------------------------------------------------------------------------
-- Optimize / Z-Order / Analyze
-- -----------------------------------------------------------------------------
OPTIMIZE geo_databricks_sub.gold.feat_h3_degurba_r10 ZORDER BY (h3_r10);

ANALYZE TABLE geo_databricks_sub.gold.feat_h3_degurba_r10 COMPUTE STATISTICS;
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_degurba_r10 COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10, degurba;




/* =============================================================================
GOLD: FEAT_H3_ROADS_R10
Source   : SILVER.ROAD_SEGMENTS (region_code/country, osm_id, highway, oneway, lanes, maxspeed_kph, geom_wkt_4326, load_ts, ...)
Target   : GOLD.FEAT_H3_ROADS_R10 (Delta)
Key      : (region_code, h3_r10)
H3       : STRING (8a1f....) computed from centroid(point) of each segment
Geo      : computed in GOLD from H3 (cell_wkt_4326, cell_center_wkt_4326, cell_area_m2)
Notes    :
 - length_m computed from GEOGRAPHY distance (ST_GeogFromWKT) OR projected metric SRID
 - no dependency on SILVER.DIM_H3_R10_CELLS for area (Snowflake-like behavior)
============================================================================= */

DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_roads_r10;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_roads_r10
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH r AS (
  SELECT
    country as region_code,
    region,
    osm_id,
    highway,
    service,
    oneway,
    lanes,
    maxspeed_kph,
    other_tags_raw,
    geom_wkb,
    /* geom only for centroid/h3 */
    ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326) AS geom_4326,
    load_ts
  FROM geo_databricks_sub.silver.road_segments
  WHERE geom_wkb IS NOT NULL
    AND country IS NOT NULL
    AND osm_id IS NOT NULL
),

typed AS (
  SELECT
    r.*,

    /* H3 r10 from centroid (STRING) */
    h3_pointash3string(ST_AsText(ST_Centroid(geom_4326)), 10) AS h3_r10,

    /* length in meters via GEOGRAPHY */
    ST_Length(ST_GeogFromWKB(geom_wkb)) AS length_m,

    /* parse flags from other_tags_raw */
    CASE
      WHEN other_tags_raw RLIKE '\\"lit\\"=>\\"(yes|true|1)\\"' THEN true
      WHEN other_tags_raw RLIKE '\\"lit\\"=>\\"(no|false|0)\\"' THEN false
      ELSE NULL
    END AS lit,

    CASE
      WHEN other_tags_raw RLIKE '\\"bridge\\"=>\\"(yes|true|1)\\"' THEN true
      WHEN other_tags_raw RLIKE '\\"bridge\\"=>\\"(no|false|0)\\"' THEN false
      ELSE NULL
    END AS bridge,

    CASE
      WHEN other_tags_raw RLIKE '\\"tunnel\\"=>\\"(yes|true|1)\\"' THEN true
      WHEN other_tags_raw RLIKE '\\"tunnel\\"=>\\"(no|false|0)\\"' THEN false
      ELSE NULL
    END AS tunnel,

    CASE
      WHEN lower(highway) IN ('motorway','motorway_link') THEN 'motorway'
      WHEN lower(highway) IN ('trunk','trunk_link') THEN 'trunk'
      WHEN lower(highway) IN ('primary','primary_link') THEN 'primary'
      WHEN lower(highway) IN ('secondary','secondary_link') THEN 'secondary'
      WHEN lower(highway) IN ('tertiary','tertiary_link') THEN 'tertiary'
      WHEN lower(highway) IN ('residential','living_street') THEN 'residential'
      WHEN lower(highway) = 'service' THEN 'service'
      ELSE 'other'
    END AS road_class
  FROM r
  WHERE h3_pointash3string(ST_AsText(ST_Centroid(geom_4326)), 10) IS NOT NULL
),

agg AS (
  SELECT
    region_code,
    region,
    h3_r10,

    COUNT(*) AS road_segments_cnt,
    SUM(length_m) AS road_length_m_sum,

    SUM(CASE WHEN road_class='motorway'    THEN length_m ELSE 0 END) AS motorway_length_m_sum,
    SUM(CASE WHEN road_class='trunk'       THEN length_m ELSE 0 END) AS trunk_length_m_sum,
    SUM(CASE WHEN road_class='primary'     THEN length_m ELSE 0 END) AS primary_length_m_sum,
    SUM(CASE WHEN road_class='secondary'   THEN length_m ELSE 0 END) AS secondary_length_m_sum,
    SUM(CASE WHEN road_class='tertiary'    THEN length_m ELSE 0 END) AS tertiary_length_m_sum,
    SUM(CASE WHEN road_class='residential' THEN length_m ELSE 0 END) AS residential_length_m_sum,
    SUM(CASE WHEN road_class='service'     THEN length_m ELSE 0 END) AS service_length_m_sum,

    AVG(CAST(lanes AS DOUBLE)) AS lanes_avg,
    percentile_approx(lanes, 0.5) AS lanes_p50,

    AVG(CAST(maxspeed_kph AS DOUBLE)) AS maxspeed_kph_avg,
    percentile_approx(maxspeed_kph, 0.5) AS maxspeed_kph_p50,
    percentile_approx(maxspeed_kph, 0.9) AS maxspeed_kph_p90,

    AVG(CASE WHEN oneway IS NULL THEN NULL WHEN oneway THEN 1.0 ELSE 0.0 END) AS oneway_share,
    AVG(CASE WHEN lit   IS NULL THEN NULL WHEN lit   THEN 1.0 ELSE 0.0 END) AS lit_share,

    SUM(CASE WHEN bridge THEN 1 ELSE 0 END) AS bridge_cnt,
    SUM(CASE WHEN tunnel THEN 1 ELSE 0 END) AS tunnel_cnt,

    MAX(load_ts) AS last_load_ts
  FROM typed
  GROUP BY 1,2,3
),

cells AS (
  SELECT
    region_code,
    h3_r10,
    cell_wkt_4326,
    cell_center_wkt_4326,

    /* GEOGRAPHY */
    ST_Area(ST_GeogFromText(cell_wkt_4326)) AS cell_area_m2
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
)

SELECT
  a.region_code,
  a.region,
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

  a.road_segments_cnt / NULLIF(c.cell_area_m2 / 1e6, 0) AS road_segments_per_km2,
  a.road_length_m_sum / NULLIF(c.cell_area_m2 / 1e6, 0) AS road_length_m_per_km2,

  a.last_load_ts
FROM agg a
JOIN cells c
  ON c.region_code = a.region_code
 AND c.h3_r10      = a.h3_r10
;


-- 1) non-empty
-- SELECT CASE WHEN COUNT(*) > 0 THEN 0 ELSE 1 END AS fail
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10;

-- 2) duplicates
-- SELECT region_code, h3_r10, COUNT(*) AS cnt
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10
-- GROUP BY 1,2
-- HAVING COUNT(*) > 1;

-- 3) nulls in key geo fields
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10
-- WHERE region_code IS NULL
--    OR h3_r10 IS NULL
--    OR cell_wkt_4326 IS NULL
--    OR cell_center_wkt_4326 IS NULL
--    OR cell_area_m2 IS NULL;

-- 4) h3 hex string format
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10
-- WHERE NOT regexp_like(h3_r10, '^[0-9a-f]+$');

-- 5) non-negative sanity
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10
-- WHERE cell_area_m2 <= 0
--    OR road_segments_cnt < 0
--    OR road_length_m_sum < 0;

-- 6) class sum <= total length (tolerance)
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10
-- WHERE (motorway_length_m_sum
--      + trunk_length_m_sum
--      + primary_length_m_sum
--      + secondary_length_m_sum
--      + tertiary_length_m_sum
--      + residential_length_m_sum
--      + service_length_m_sum) > road_length_m_sum * 1.001;

-- 7) shares in [0,1]
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10
-- WHERE (oneway_share IS NOT NULL AND (oneway_share < 0 OR oneway_share > 1))
--    OR (lit_share    IS NOT NULL AND (lit_share    < 0 OR lit_share    > 1));

-- 8) lanes non-negative
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10
-- WHERE lanes_avg < 0 OR lanes_p50 < 0;

-- 9) top density
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_roads_r10
-- ORDER BY road_length_m_per_km2 DESC
-- LIMIT 10000;

-- DESCRIBE TABLE geo_databricks_sub.gold.feat_h3_roads_r10;

OPTIMIZE geo_databricks_sub.gold.feat_h3_roads_r10 ZORDER BY (h3_r10);

ANALYZE TABLE geo_databricks_sub.gold.feat_h3_roads_r10 COMPUTE STATISTICS;
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_roads_r10
  COMPUTE STATISTICS FOR COLUMNS region_code, h3_r10, road_segments_cnt, road_length_m_sum;



-- =============================================================================
-- GOLD.FEAT_H3_POI_POINTS_R10  (Databricks / Delta)
--
-- Source:
--   SILVER.POI_POINTS         (POI nodes)
--   SILVER.DIM_H3_R10_CELLS   (cell_wkt_4326 + cell_center_wkt_4326)
--
-- Grain: (region_code, region, h3_r10) — 1 row per H3 cell per region
--
-- Notes:
-- - compute cell_area_m2 in GOLD using GEOGRAPHY derived from cell_wkt_4326,
--   to avoid the “area ~ 1e-6” bug we had in roads.
-- =============================================================================

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_poi_points_r10
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH p AS (
  SELECT
    CAST(country AS STRING) AS region_code,
    CAST(region  AS STRING) AS region,

    /* H3 r10 from point geometry (centroid==point) */
    h3_pointash3string(
      ST_AsText(ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326)),
      10
    ) AS h3_r10,

    LOWER(CAST(poi_class AS STRING)) AS poi_class,
    LOWER(CAST(poi_type  AS STRING)) AS poi_type
  FROM geo_databricks_sub.silver.poi_points
  WHERE country IS NOT NULL
    AND region  IS NOT NULL
    AND geom_wkb IS NOT NULL
),

agg AS (
  SELECT
    region_code,
    region,
    h3_r10,

    /* totals */
    COUNT(*)                                  AS poi_points_cnt,
    COUNT(DISTINCT poi_class)                 AS poi_classes_cnt,
    COUNT(DISTINCT poi_type)                  AS poi_types_cnt,

    /* class breakdown */
    SUM(CASE WHEN poi_class = 'amenity'           THEN 1 ELSE 0 END) AS amenity_cnt,
    SUM(CASE WHEN poi_class = 'shop'              THEN 1 ELSE 0 END) AS shop_cnt,
    SUM(CASE WHEN poi_class = 'tourism'           THEN 1 ELSE 0 END) AS tourism_cnt,
    SUM(CASE WHEN poi_class = 'leisure'           THEN 1 ELSE 0 END) AS leisure_cnt,
    SUM(CASE WHEN poi_class = 'office'            THEN 1 ELSE 0 END) AS office_cnt,
    SUM(CASE WHEN poi_class = 'craft'             THEN 1 ELSE 0 END) AS craft_cnt,
    SUM(CASE WHEN poi_class = 'man_made'          THEN 1 ELSE 0 END) AS man_made_cnt,
    SUM(CASE WHEN poi_class = 'emergency'         THEN 1 ELSE 0 END) AS emergency_cnt,
    SUM(CASE WHEN poi_class = 'public_transport'  THEN 1 ELSE 0 END) AS public_transport_cnt,
    SUM(CASE WHEN poi_class = 'railway'           THEN 1 ELSE 0 END) AS railway_cnt,
    SUM(CASE WHEN poi_class = 'highway'           THEN 1 ELSE 0 END) AS highway_cnt,
    SUM(CASE WHEN poi_class = 'place'             THEN 1 ELSE 0 END) AS place_cnt,

    /* EV-relevant poi_type buckets */
    SUM(CASE WHEN poi_type IN ('parking','parking_entrance','bicycle_parking') THEN 1 ELSE 0 END) AS parking_cnt,

    SUM(CASE WHEN poi_type IN ('fuel','charging_station','car_wash','car_rental','car_sharing','parking_space')
             THEN 1 ELSE 0 END) AS mobility_services_cnt,

    SUM(CASE WHEN poi_type IN ('supermarket','convenience','mall','department_store','hardware','doityourself')
             THEN 1 ELSE 0 END) AS retail_core_cnt,

    SUM(CASE WHEN poi_type IN ('restaurant','fast_food','cafe','bar','pub')
             THEN 1 ELSE 0 END) AS food_cnt,

    SUM(CASE WHEN poi_type IN ('hotel','motel','hostel','guest_house','apartments')
             THEN 1 ELSE 0 END) AS lodging_cnt,

    SUM(CASE WHEN poi_type IN ('hospital','clinic','doctors','pharmacy')
             THEN 1 ELSE 0 END) AS health_cnt

  FROM p
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2,3
),

cells AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region      AS STRING) AS region,
    CAST(h3_r10       AS STRING) AS h3_r10,
    cell_wkt_4326,
    cell_center_wkt_4326,

    
    ST_Area(ST_GeogFromText(cell_wkt_4326)) AS cell_area_m2
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
  WHERE cell_wkt_4326 IS NOT NULL
)

SELECT
  a.region_code,
  a.region,
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

  /* densities per km2 (by hex area) */
  a.poi_points_cnt / NULLIF(c.cell_area_m2 / 1e6, 0) AS poi_points_per_km2,
  a.amenity_cnt    / NULLIF(c.cell_area_m2 / 1e6, 0) AS amenity_per_km2,
  a.shop_cnt       / NULLIF(c.cell_area_m2 / 1e6, 0) AS shop_per_km2,
  a.parking_cnt    / NULLIF(c.cell_area_m2 / 1e6, 0) AS parking_per_km2

FROM agg a
JOIN cells c
  ON c.region_code = a.region_code
 AND c.region      = a.region
 AND c.h3_r10       = a.h3_r10
;


-- -- 1) rowcount
-- SELECT COUNT(*) AS rows_cnt FROM geo_databricks_sub.gold.feat_h3_poi_points_r10;

-- -- 2) sanity check cell_area_m2
-- SELECT
--   min(cell_area_m2) AS min_m2,
--   approx_percentile(cell_area_m2, 0.5) AS p50_m2,
--   max(cell_area_m2) AS max_m2
-- FROM geo_databricks_sub.gold.feat_h3_poi_points_r10;

-- -- 3) WKT debug
-- SELECT region_code, region, h3_r10, cell_center_wkt_4326, cell_wkt_4326
-- FROM geo_databricks_sub.gold.feat_h3_poi_points_r10
-- LIMIT 10;

-- select * from geo_databricks_sub.gold.feat_h3_poi_points_r10 order by poi_points_per_km2 desc
-- limit 100;


OPTIMIZE geo_databricks_sub.gold.feat_h3_poi_points_r10 ZORDER BY (h3_r10);

ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_points_r10 COMPUTE STATISTICS;
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_points_r10 COMPUTE STATISTICS FOR COLUMNS region_code, region, h3_r10;





-- =============================================================================
-- GOLD.FEAT_H3_POI_AREAS_R10  (Databricks / Delta)
--
-- Source:
--   SILVER.POI_AREAS         (geom_wkb, poi_class, region_code, region, load_ts)
--   SILVER.DIM_H3_R10_CELLS  (cell_wkt_4326, cell_center_wkt_4326, h3_r10, region_code, region)
--
-- Method (fast, Snowflake-like):
--   - centroid -> h3 r10
--   - aggregate counts per (region_code, region, h3_r10)
--   - take cell_wkt/center from DIM
--   - recompute TRUE cell_area_m2 in GOLD via GEOGRAPHY from WKT
--
-- Grain: (region_code, region, h3_r10)
-- =============================================================================

DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_poi_areas_r10;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH base AS (
  SELECT
    CAST(country AS STRING) AS region_code,
    CAST(region      AS STRING) AS region,

    h3_pointash3string(
      ST_AsText(ST_Centroid(ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326))),
      10
    ) AS h3_r10,

    LOWER(CAST(poi_class AS STRING)) AS poi_class,
    CAST(load_ts AS TIMESTAMP)       AS load_ts
  FROM geo_databricks_sub.silver.poi_areas
  WHERE geom_wkb IS NOT NULL
    AND country IS NOT NULL
    AND region IS NOT NULL
    AND poi_class IS NOT NULL
),

agg AS (
  SELECT
    region_code,
    region,
    h3_r10,

    COUNT(*) AS poi_areas_cnt,
    SUM(CASE WHEN poi_class='amenity'  THEN 1 ELSE 0 END) AS amenity_areas_cnt,
    SUM(CASE WHEN poi_class='shop'     THEN 1 ELSE 0 END) AS shop_areas_cnt,
    SUM(CASE WHEN poi_class='tourism'  THEN 1 ELSE 0 END) AS tourism_areas_cnt,
    SUM(CASE WHEN poi_class='office'   THEN 1 ELSE 0 END) AS office_areas_cnt,
    SUM(CASE WHEN poi_class='leisure'  THEN 1 ELSE 0 END) AS leisure_areas_cnt,
    SUM(CASE WHEN poi_class='sport'    THEN 1 ELSE 0 END) AS sport_areas_cnt,
    SUM(CASE WHEN poi_class='building' THEN 1 ELSE 0 END) AS building_areas_cnt,
    SUM(CASE WHEN poi_class='landuse'  THEN 1 ELSE 0 END) AS landuse_areas_cnt,

    MAX(load_ts) AS last_load_ts
  FROM base
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2,3
),

cell AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region      AS STRING) AS region,
    CAST(h3_r10       AS STRING) AS h3_r10,
    cell_wkt_4326,
    cell_center_wkt_4326,


    ST_Area(ST_GeogFromText(cell_wkt_4326)) AS cell_area_m2
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
)

SELECT
  a.region_code,
  a.region,
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
  a.poi_areas_cnt     / NULLIF(c.cell_area_m2 / 1e6, 0) AS poi_areas_per_km2,
  a.amenity_areas_cnt / NULLIF(c.cell_area_m2 / 1e6, 0) AS amenity_areas_per_km2,
  a.shop_areas_cnt    / NULLIF(c.cell_area_m2 / 1e6, 0) AS shop_areas_per_km2,
  a.tourism_areas_cnt / NULLIF(c.cell_area_m2 / 1e6, 0) AS tourism_areas_per_km2,
  a.office_areas_cnt  / NULLIF(c.cell_area_m2 / 1e6, 0) AS office_areas_per_km2,
  a.leisure_areas_cnt / NULLIF(c.cell_area_m2 / 1e6, 0) AS leisure_areas_per_km2,
  a.sport_areas_cnt   / NULLIF(c.cell_area_m2 / 1e6, 0) AS sport_areas_per_km2,

  a.last_load_ts
FROM agg a
JOIN cell c
  ON c.region_code=a.region_code
 AND c.region=a.region
 AND c.h3_r10=a.h3_r10
;

-- --QA
-- -- 1) rowcount
-- select count(*) as rows_cnt
-- from GOLD.FEAT_H3_POI_AREAS_R10;

-- -- 2) key uniqueness
-- select region_code, h3_r10, count(*) c
-- from GOLD.FEAT_H3_POI_AREAS_R10
-- group by 1,2
-- having count(*) > 1;

-- -- 3) H3 format (простая эвристика: hex string)
-- select *
-- from GOLD.FEAT_H3_POI_AREAS_R10
-- where h3_r10 is null
--    or regexp_like(h3_r10, '^[0-9a-f]+$') = false
-- limit 100;

-- -- 4) areas > 0
-- select *
-- from GOLD.FEAT_H3_POI_AREAS_R10
-- where cell_area_m2 is null or cell_area_m2 <= 0
-- limit 100;

-- -- 5) densities non-negative
-- select *
-- from GOLD.FEAT_H3_POI_AREAS_R10
-- where poi_areas_per_km2 < 0
-- limit 100;

-- -- 6) top densest cells (QA)
-- select region_code, h3_r10, poi_areas_cnt, poi_areas_per_km2
-- from GOLD.FEAT_H3_POI_AREAS_R10
-- order by poi_areas_per_km2 desc
-- limit 50;


-- -- 1) how many hex in dim_h3_r10_cells
-- select count(*) as dim_h3_cnt
-- from SILVER.DIM_H3_R10_CELLS;

-- -- 2) how many unique hex from poi_areas
-- select count(distinct h3_pointash3string(ST_AsText(ST_Centroid(geom_wkb)), 10)) as poi_areas_h3_cnt
-- from SILVER.POI_AREAS
-- where geom_wkb is not null;

-- -- 3) coverage %
-- with a as (
--   select distinct country, h3_point_to_cell_string(st_centroid(geom_wkb), 10) as h3_r10
--   from SILVER.POI_AREAS
--   where geom_wkb is not null and country is not null
-- ),
-- d as (
--   select region_code, h3_r10
--   from SILVER.DIM_H3_R10_CELLS
-- )
-- select
--   count(*) as covered_cells,
--   (select count(*) from d) as dim_cells,
--   100.0 * count(*) / nullif((select count(*) from d),0) as coverage_pct
-- from a
-- join d using (region_code, h3_r10);

-- select
--   count(*) as total_dim,
--   sum(case when p.h3_r10 is null then 1 else 0 end) as no_poi_cells
-- from SILVER.DIM_H3_R10_CELLS d
-- left join GOLD.FEAT_H3_POI_AREAS_R10 p
--   on p.region_code = d.region_code and p.h3_r10 = d.h3_r10;

--   select
--   approx_percentile(poi_areas_cnt, 0.5) as p50,
--   approx_percentile(poi_areas_cnt, 0.9) as p90,
--   approx_percentile(poi_areas_cnt, 0.99) as p99,
--   max(poi_areas_cnt) as maxv
-- from GOLD.FEAT_H3_POI_AREAS_R10;

-- select * from GOLD.FEAT_H3_POI_AREAS_R10
-- order by poi_areas_per_km2 desc
-- limit 100;

OPTIMIZE geo_databricks_sub.gold.feat_h3_poi_areas_r10 ZORDER BY (h3_r10);
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10 COMPUTE STATISTICS;
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10 COMPUTE STATISTICS FOR COLUMNS region_code, region, h3_r10;


-- =============================================================================
-- GOLD.FEAT_H3_POI_AREAS_R10__CENTROID_XAREA  (Databricks / Delta)
--
-- Source:
--   geo_databricks_sub.silver.poi_areas        (geom_wkb, feature_id, poi_class, poi_type, region_code, region, load_ts)
--   geo_databricks_sub.silver.dim_h3_r10_cells (h3_r10, cell_wkt_4326, cell_center_wkt_4326, region_code, region)
--
-- Method:
--   - 1 candidate cell per POI by centroid -> h3 r10
--   - join to dim on (region_code, region, h3_r10)
--   - exact intersection area in m²:
--       inter_geom = ST_Intersection(poi_geom, cell_geom)   -- GEOMETRY
--       poi_area_m2 = ST_Area(ST_GeogFromWKB(ST_AsBinary(inter_geom)))  -- m²
--
-- Grain: (region_code, region, h3_r10)
-- Notes:
--   - not using polyfill: very large polygons that span multiple cells will be under-covered
--   - poi_area_share can be > 1 due to overlaps (this is OK; clamp if you want)
-- =============================================================================

DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea;

DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH poi AS (
  SELECT
    CAST(country AS STRING)   AS region_code,
    CAST(region  AS STRING)   AS region,
    CAST(feature_id AS STRING) AS feature_id,
    LOWER(CAST(poi_class AS STRING)) AS poi_class,
    LOWER(CAST(poi_type  AS STRING)) AS poi_type,
    CAST(load_ts AS TIMESTAMP) AS load_ts,

    ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326) AS poi_geom_raw
  FROM geo_databricks_sub.silver.poi_areas
  WHERE geom_wkb IS NOT NULL
    AND country IS NOT NULL AND region IS NOT NULL
    AND feature_id IS NOT NULL AND poi_class IS NOT NULL AND poi_type IS NOT NULL
),

poi_fix AS (
  SELECT
    *,
    /* fix invalid geometries */
    CASE
      WHEN ST_IsValid(poi_geom_raw) THEN poi_geom_raw
      ELSE ST_Buffer(poi_geom_raw, 0)
    END AS poi_geom
  FROM poi
),

poi_h3 AS (
  SELECT
    region_code, region, feature_id, poi_class, poi_type, load_ts, poi_geom,

    /* safe 2D input for H3 (no NaN-Z) */
    h3_pointash3string(
      concat(
        'POINT(',
        cast(ST_X(ST_Centroid(poi_geom)) as string), ' ',
        cast(ST_Y(ST_Centroid(poi_geom)) as string),
        ')'
      ),
      10
    ) AS h3_r10
  FROM poi_fix
  WHERE poi_geom IS NOT NULL
    AND ST_IsValid(poi_geom)
    AND NOT ST_IsEmpty(poi_geom)
),

cells AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region      AS STRING) AS region,
    CAST(h3_r10       AS STRING) AS h3_r10,
    CAST(cell_wkt_4326 AS STRING)        AS cell_wkt_4326,
    CAST(cell_center_wkt_4326 AS STRING) AS cell_center_wkt_4326,

    ST_SetSRID(ST_GeomFromText(cell_wkt_4326), 4326) AS cell_geom,
    ST_Area(ST_GeogFromText(cell_wkt_4326))          AS cell_area_m2
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
  WHERE region_code IS NOT NULL
    AND region IS NOT NULL
    AND h3_r10 IS NOT NULL
    AND cell_wkt_4326 IS NOT NULL
),

joined AS (
  SELECT
    c.region_code,
    c.region,
    c.h3_r10,

    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,

    p.feature_id,
    p.poi_class,
    p.poi_type,
    p.poi_geom,
    c.cell_geom,
    p.load_ts
  FROM poi_h3 p
  JOIN cells c
    ON c.region_code = p.region_code
   AND c.region      = p.region
   AND c.h3_r10      = p.h3_r10
  WHERE p.h3_r10 IS NOT NULL
    AND ST_Intersects(p.poi_geom, c.cell_geom)
),

xarea AS (
  SELECT
    region_code, region, h3_r10,
    feature_id, poi_class, poi_type,
    load_ts,

    cell_area_m2, cell_wkt_4326, cell_center_wkt_4326,

    /* intersection GEOMETRY -> GEOGRAPHY -> m² */
    ST_Area(
      ST_GeogFromWKB(
        ST_AsBinary(ST_Intersection(poi_geom, cell_geom))
      )
    ) AS poi_area_m2
  FROM joined
)

SELECT
  region_code,
  region,
  h3_r10,

  any_value(cell_area_m2)         AS cell_area_m2,
  any_value(cell_wkt_4326)        AS cell_wkt_4326,
  any_value(cell_center_wkt_4326) AS cell_center_wkt_4326,

  COUNT(DISTINCT feature_id) AS poi_areas_cnt,

  SUM(poi_area_m2) AS poi_area_m2_sum,
  SUM(poi_area_m2) / NULLIF(any_value(cell_area_m2), 0) AS poi_area_share,

  COUNT(DISTINCT CASE WHEN poi_class='amenity'  THEN feature_id END) AS amenity_areas_cnt,
  COUNT(DISTINCT CASE WHEN poi_class='shop'     THEN feature_id END) AS shop_areas_cnt,
  COUNT(DISTINCT CASE WHEN poi_class='tourism'  THEN feature_id END) AS tourism_areas_cnt,
  COUNT(DISTINCT CASE WHEN poi_class='office'   THEN feature_id END) AS office_areas_cnt,
  COUNT(DISTINCT CASE WHEN poi_class='leisure'  THEN feature_id END) AS leisure_areas_cnt,
  COUNT(DISTINCT CASE WHEN poi_class='sport'    THEN feature_id END) AS sport_areas_cnt,
  COUNT(DISTINCT CASE WHEN poi_class='building' THEN feature_id END) AS building_areas_cnt,
  COUNT(DISTINCT CASE WHEN poi_class='landuse'  THEN feature_id END) AS landuse_areas_cnt,

  SUM(CASE WHEN poi_class='amenity'  THEN poi_area_m2 ELSE 0 END) AS amenity_area_m2_sum,
  SUM(CASE WHEN poi_class='shop'     THEN poi_area_m2 ELSE 0 END) AS shop_area_m2_sum,
  SUM(CASE WHEN poi_class='tourism'  THEN poi_area_m2 ELSE 0 END) AS tourism_area_m2_sum,
  SUM(CASE WHEN poi_class='office'   THEN poi_area_m2 ELSE 0 END) AS office_area_m2_sum,
  SUM(CASE WHEN poi_class='leisure'  THEN poi_area_m2 ELSE 0 END) AS leisure_area_m2_sum,
  SUM(CASE WHEN poi_class='sport'    THEN poi_area_m2 ELSE 0 END) AS sport_area_m2_sum,
  SUM(CASE WHEN poi_class='building' THEN poi_area_m2 ELSE 0 END) AS building_area_m2_sum,
  SUM(CASE WHEN poi_class='landuse'  THEN poi_area_m2 ELSE 0 END) AS landuse_area_m2_sum,

  COUNT(DISTINCT feature_id) / NULLIF(any_value(cell_area_m2) / 1e6, 0) AS poi_areas_per_km2,
  SUM(poi_area_m2)           / NULLIF(any_value(cell_area_m2) / 1e6, 0) AS poi_area_m2_per_km2,

  MAX(load_ts) AS last_load_ts
FROM xarea
WHERE poi_area_m2 IS NOT NULL AND poi_area_m2 > 0
GROUP BY 1,2,3
;




-- --QA 

-- SELECT * 
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea
-- ORDER BY poi_areas_per_km2 LIMIT 50;

-- SELECT COUNT(*) AS rows_cnt
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea;

-- SELECT region_code, region, h3_r10, COUNT(*) c
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea
-- GROUP BY 1,2,3
-- HAVING COUNT(*) > 1;

-- SELECT region_code, region, h3_r10, cell_center_wkt_4326, cell_wkt_4326
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea
-- LIMIT 20;

-- -- non-negative metrics
-- select *
-- from GOLD.FEAT_H3_POI_AREAS_R10__CENTROID_XAREA
-- where poi_areas_cnt < 0
--    or poi_area_m2_sum < 0
--    or poi_area_share < 0
--    or poi_areas_per_km2 < 0
--    or poi_area_m2_per_km2 < 0
-- limit 50;


-- SELECT
--   approx_percentile(cell_area_m2, 0.50) AS p50_area_m2,
--   approx_percentile(cell_area_m2, 0.99) AS p99_area_m2,
--   min(cell_area_m2) AS min_area_m2,
--   max(cell_area_m2) AS max_area_m2
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea;

-- SELECT
--   max(poi_area_share) AS max_share,
--   approx_percentile(poi_area_share, 0.99) AS p99_share,
--   approx_percentile(poi_area_share, 0.999) AS p999_share
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea;


OPTIMIZE geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea ZORDER BY (h3_r10);
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10__centroid_xarea
COMPUTE STATISTICS FOR COLUMNS region_code, region, h3_r10;


-- =============================================================================
-- GOLD.FEAT_H3_TRANSIT_POINTS_R10  (Databricks / Delta)
--
-- Source:
--   geo_databricks_sub.silver.transit_points    (point POIs, geom_wkb)
--   geo_databricks_sub.silver.dim_h3_r10_cells  (cell_wkt_4326, cell_center_wkt_4326, region_code, region, h3_r10)
--
-- Method:
--   - h3_r10 from point (safe WKT POINT(lon lat)) -> h3_pointash3string(..., 10)
--   - aggregate counts per (region_code, region, h3_r10)
--   - LEFT JOIN from DIM to keep full grid coverage
--   - compute densities using TRUE cell area (m²) from cell_wkt_4326
--
-- Grain: (region_code, region, h3_r10)
-- =============================================================================

DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_transit_points_r10;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_transit_points_r10
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH dim AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region      AS STRING) AS region,
    CAST(h3_r10       AS STRING) AS h3_r10,
    CAST(cell_wkt_4326        AS STRING) AS cell_wkt_4326,
    CAST(cell_center_wkt_4326 AS STRING) AS cell_center_wkt_4326,

    /* TRUE area in m² from WKT */
    ST_Area(ST_GeogFromText(cell_wkt_4326)) AS cell_area_m2
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
  WHERE region_code IS NOT NULL
    AND region IS NOT NULL
    AND h3_r10 IS NOT NULL
    AND cell_wkt_4326 IS NOT NULL
),

pts AS (
  SELECT
    CAST(country AS STRING) AS region_code,
    CAST(region  AS STRING) AS region,
    LOWER(CAST(transit_class AS STRING)) AS poi_class,
    LOWER(CAST(transit_type  AS STRING)) AS poi_type,
    CAST(load_ts AS TIMESTAMP) AS load_ts,

    /* GEOMETRY 4326 from WKB */
    ST_SetSRID(ST_GeomFromWKB(geom_wkb), 4326) AS pt_geom
  FROM geo_databricks_sub.silver.transit_points
  WHERE geom_wkb IS NOT NULL
    AND country IS NOT NULL
    AND region IS NOT NULL
),

pts_h3 AS (
  SELECT
    region_code,
    region,
    poi_class,
    poi_type,
    load_ts,

    /* SAFE 2D input to H3: POINT(lon lat) */
    h3_pointash3string(
      concat(
        'POINT(',
        cast(ST_X(pt_geom) as string), ' ',
        cast(ST_Y(pt_geom) as string),
        ')'
      ),
      10
    ) AS h3_r10
  FROM pts
  WHERE pt_geom IS NOT NULL
),

agg AS (
  SELECT
    region_code,
    region,
    h3_r10,

    COUNT(*) AS transit_points_cnt,
    SUM(CASE WHEN poi_class = 'transport' THEN 1 ELSE 0 END) AS transport_points_cnt,
    SUM(CASE WHEN poi_class = 'amenity'   THEN 1 ELSE 0 END) AS amenity_points_cnt,
    SUM(CASE WHEN poi_class = 'emergency' THEN 1 ELSE 0 END) AS emergency_points_cnt,

    /* station-like / stop-like buckets */
    SUM(CASE WHEN poi_type IN ('station','halt','tram_stop','subway_entrance') THEN 1 ELSE 0 END) AS station_like_cnt,
    SUM(CASE WHEN poi_type IN ('bus_stop','platform') THEN 1 ELSE 0 END) AS stop_like_cnt,

    MAX(load_ts) AS last_load_ts
  FROM pts_h3
  WHERE h3_r10 IS NOT NULL
  GROUP BY 1,2,3
)

SELECT
  d.region_code,
  d.region,
  d.h3_r10,

  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

  COALESCE(a.transit_points_cnt, 0)   AS transit_points_cnt,
  COALESCE(a.transport_points_cnt, 0) AS transport_points_cnt,
  COALESCE(a.amenity_points_cnt, 0)   AS amenity_points_cnt,
  COALESCE(a.emergency_points_cnt, 0) AS emergency_points_cnt,
  COALESCE(a.station_like_cnt, 0)     AS station_like_cnt,
  COALESCE(a.stop_like_cnt, 0)        AS stop_like_cnt,

  /* shares */
  CASE WHEN COALESCE(a.transit_points_cnt,0)=0 THEN NULL
       ELSE a.transport_points_cnt / NULLIF(a.transit_points_cnt,0) END AS transport_points_share,
  CASE WHEN COALESCE(a.transit_points_cnt,0)=0 THEN NULL
       ELSE a.emergency_points_cnt / NULLIF(a.transit_points_cnt,0) END AS emergency_points_share,

  /* densities per km²: cnt * 1e6 / area_m2 */
  (COALESCE(a.transit_points_cnt,0)   * 1e6 / NULLIF(d.cell_area_m2,0)) AS transit_points_per_km2,
  (COALESCE(a.transport_points_cnt,0) * 1e6 / NULLIF(d.cell_area_m2,0)) AS transport_points_per_km2,

  a.last_load_ts,
  (COALESCE(a.transit_points_cnt,0) > 0) AS has_transit_points
FROM dim d
LEFT JOIN agg a
  ON a.region_code = d.region_code
 AND a.region      = d.region
 AND a.h3_r10      = d.h3_r10
;



-- -- rowcount
-- SELECT count(*) AS rows_cnt
-- FROM geo_databricks_sub.gold.feat_h3_transit_points_r10;

-- -- key uniqueness
-- SELECT region_code, region, h3_r10, count(*) c
-- FROM geo_databricks_sub.gold.feat_h3_transit_points_r10
-- GROUP BY 1,2,3
-- HAVING count(*) > 1;

-- -- geometry debug must exist for all dim cells
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_transit_points_r10
-- WHERE cell_wkt_4326 IS NULL OR cell_center_wkt_4326 IS NULL OR cell_area_m2 IS NULL OR cell_area_m2 <= 0
-- LIMIT 50;

-- -- density non-negative
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_transit_points_r10
-- WHERE transit_points_per_km2 < 0 OR transport_points_per_km2 < 0
-- LIMIT 50;

-- -- tops
-- SELECT region_code, region, h3_r10, transit_points_cnt, transit_points_per_km2
-- FROM geo_databricks_sub.gold.feat_h3_transit_points_r10
-- ORDER BY transit_points_per_km2 DESC
-- LIMIT 50;

-- SELECT
--   approx_percentile(cell_area_m2, 0.5) AS area_p50_m2,
--   approx_percentile(cell_area_m2, 0.1) AS area_p10_m2,
--   approx_percentile(cell_area_m2, 0.9) AS area_p90_m2,
--   min(cell_area_m2) AS area_min_m2,
--   max(cell_area_m2) AS area_max_m2
-- FROM geo_databricks_sub.gold.feat_h3_transit_points_r10;

-- recommended physical optimization
OPTIMIZE geo_databricks_sub.gold.feat_h3_transit_points_r10 ZORDER BY (h3_r10, last_load_ts);

ANALYZE TABLE geo_databricks_sub.gold.feat_h3_transit_points_r10 COMPUTE STATISTICS;
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_transit_points_r10 COMPUTE STATISTICS FOR COLUMNS h3_r10;



-- =============================================================================
-- GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS  (Databricks / Delta)
--
-- Source:
--   geo_databricks_sub.silver.poi_areas        (geom_wkb, feature_id, poi_class, poi_type, country, region, load_ts)
--   geo_databricks_sub.silver.dim_h3_r10_cells (region_code, region, h3_r10, cell_wkt_4326, cell_center_wkt_4326)
--
-- Method:
--   - 1 candidate H3 cell per POI polygon from centroid (H3 r10)
--   - join to DIM on (region_code, region, h3_r10)
--   - exact intersection area (m²):
--       inter_geom      = ST_Intersection(poi_geom_4326, cell_geom_4326)           -- GEOMETRY
--       poi_xarea_m2    = ST_Area(ST_GeogFromWKB(ST_AsBinary(inter_geom)))         -- m²
--   - aggregate totals + 8 class buckets: counts + xarea sums + shares
--
-- Grain: (region_code, region, h3_r10)  -- sparse (only covered cells)
-- =============================================================================

DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH poi_raw AS (
  SELECT
    CAST(country AS STRING) AS region_code,
    CAST(region  AS STRING) AS region,
    CAST(feature_id AS STRING) AS feature_id,
    LOWER(CAST(poi_class AS STRING)) AS poi_class,
    LOWER(CAST(poi_type  AS STRING)) AS poi_type,
    CAST(load_ts AS TIMESTAMP) AS load_ts,

    -- raw geometry (no SRID yet)
    ST_GeomFromWKB(geom_wkb) AS poi_geom_raw
  FROM geo_databricks_sub.silver.poi_areas
  WHERE geom_wkb IS NOT NULL
    AND country  IS NOT NULL
    AND region   IS NOT NULL
    AND feature_id IS NOT NULL
    AND poi_class IS NOT NULL
),

poi_fixed AS (
  SELECT
    region_code, region, feature_id, poi_class, poi_type, load_ts,

    -- set SRID + fix invalid via buffer(0)
    ST_SetSRID(
      CASE
        WHEN poi_geom_raw IS NULL THEN NULL
        WHEN ST_IsEmpty(poi_geom_raw) THEN NULL
        WHEN NOT ST_IsValid(poi_geom_raw) THEN ST_Buffer(poi_geom_raw, 0.0)
        ELSE poi_geom_raw
      END,
      4326
    ) AS poi_geom_4326,

    -- validation flags (QA)
    CASE WHEN poi_geom_raw IS NULL THEN 1 ELSE 0 END AS poi_geom_null,
    CASE WHEN poi_geom_raw IS NOT NULL AND ST_IsEmpty(poi_geom_raw) THEN 1 ELSE 0 END AS poi_geom_empty,
    CASE WHEN poi_geom_raw IS NOT NULL AND NOT ST_IsEmpty(poi_geom_raw) AND NOT ST_IsValid(poi_geom_raw) THEN 1 ELSE 0 END AS poi_geom_invalid_before,
    CASE
      WHEN poi_geom_raw IS NOT NULL
       AND NOT ST_IsEmpty(poi_geom_raw)
       AND NOT ST_IsValid(poi_geom_raw)
       AND ST_IsValid(ST_Buffer(poi_geom_raw, 0.0))
      THEN 1 ELSE 0
    END AS poi_fixed_by_buffer0
  FROM poi_raw
),

poi AS (
  SELECT
    region_code, region, feature_id, poi_class, poi_type, load_ts,
    poi_geom_4326,

    -- candidate H3 from centroid (как у тебя было)
    h3_pointash3string(ST_AsText(ST_Centroid(poi_geom_4326)), 10) AS h3_r10
  FROM poi_fixed
  WHERE poi_geom_4326 IS NOT NULL
    AND ST_IsValid(poi_geom_4326)
),

cells AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region      AS STRING) AS region,
    CAST(h3_r10      AS STRING) AS h3_r10,

    CAST(cell_wkt_4326 AS STRING)        AS cell_wkt_4326,
    CAST(cell_center_wkt_4326 AS STRING) AS cell_center_wkt_4326,

    -- Cell as GEOMETRY(4326)
    ST_SetSRID(ST_GeomFromText(cell_wkt_4326), 4326) AS cell_geom_4326,

    -- True cell area in m² (GEOGRAPHY)
    ST_Area(ST_GeogFromText(cell_wkt_4326)) AS cell_area_m2
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
  WHERE region_code IS NOT NULL
    AND region IS NOT NULL
    AND h3_r10 IS NOT NULL
    AND cell_wkt_4326 IS NOT NULL
),

joined AS (
  SELECT
    c.region_code,
    c.region,
    c.h3_r10,

    c.cell_area_m2,
    c.cell_wkt_4326,
    c.cell_center_wkt_4326,

    p.feature_id,
    p.poi_class,
    p.poi_type,
    p.load_ts,

    p.poi_geom_4326,
    c.cell_geom_4326
  FROM poi p
  JOIN cells c
    ON c.region_code = p.region_code
   AND c.region      = p.region
   AND c.h3_r10      = p.h3_r10
  WHERE p.h3_r10 IS NOT NULL
    AND ST_Intersects(p.poi_geom_4326, c.cell_geom_4326)
),

metrics AS (
  SELECT
    region_code,
    region,
    h3_r10,
    feature_id,
    poi_class,
    poi_type,
    load_ts,

    cell_area_m2,
    cell_wkt_4326,
    cell_center_wkt_4326,

    -- intersection area in m² (GEOGRAPHY)
    ST_Area(
      ST_GeogFromWKB(
        ST_AsBinary(
          ST_Intersection(poi_geom_4326, cell_geom_4326)
        )
      )
    ) AS poi_xarea_m2
  FROM joined
),

agg AS (
  SELECT
    region_code,
    region,
    h3_r10,

    ANY_VALUE(cell_area_m2)          AS cell_area_m2,
    ANY_VALUE(cell_wkt_4326)         AS cell_wkt_4326,
    ANY_VALUE(cell_center_wkt_4326)  AS cell_center_wkt_4326,

    TRUE AS has_poi_areas,

    COUNT(DISTINCT feature_id) AS poi_areas_cnt,
    SUM(poi_xarea_m2)          AS poi_xarea_m2_sum,
    SUM(poi_xarea_m2) / NULLIF(ANY_VALUE(cell_area_m2), 0) AS poi_xarea_share,

    COUNT(DISTINCT feature_id) / NULLIF(ANY_VALUE(cell_area_m2) / 1e6, 0) AS poi_areas_per_km2,
    SUM(poi_xarea_m2)          / NULLIF(ANY_VALUE(cell_area_m2) / 1e6, 0) AS poi_xarea_m2_per_km2,

    COUNT(DISTINCT CASE WHEN poi_class='amenity'  THEN feature_id END) AS poi_amenity_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='shop'     THEN feature_id END) AS poi_shop_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='tourism'  THEN feature_id END) AS poi_tourism_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='office'   THEN feature_id END) AS poi_office_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='leisure'  THEN feature_id END) AS poi_leisure_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='sport'    THEN feature_id END) AS poi_sport_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='building' THEN feature_id END) AS poi_building_cnt,
    COUNT(DISTINCT CASE WHEN poi_class='landuse'  THEN feature_id END) AS poi_landuse_cnt,

    COUNT(DISTINCT CASE WHEN poi_class='amenity'  THEN feature_id END) / NULLIF(COUNT(DISTINCT feature_id),0) AS poi_amenity_share,
    COUNT(DISTINCT CASE WHEN poi_class='shop'     THEN feature_id END) / NULLIF(COUNT(DISTINCT feature_id),0) AS poi_shop_share,
    COUNT(DISTINCT CASE WHEN poi_class='tourism'  THEN feature_id END) / NULLIF(COUNT(DISTINCT feature_id),0) AS poi_tourism_share,
    COUNT(DISTINCT CASE WHEN poi_class='office'   THEN feature_id END) / NULLIF(COUNT(DISTINCT feature_id),0) AS poi_office_share,
    COUNT(DISTINCT CASE WHEN poi_class='leisure'  THEN feature_id END) / NULLIF(COUNT(DISTINCT feature_id),0) AS poi_leisure_share,
    COUNT(DISTINCT CASE WHEN poi_class='sport'    THEN feature_id END) / NULLIF(COUNT(DISTINCT feature_id),0) AS poi_sport_share,
    COUNT(DISTINCT CASE WHEN poi_class='building' THEN feature_id END) / NULLIF(COUNT(DISTINCT feature_id),0) AS poi_building_share,
    COUNT(DISTINCT CASE WHEN poi_class='landuse'  THEN feature_id END) / NULLIF(COUNT(DISTINCT feature_id),0) AS poi_landuse_share,

    SUM(CASE WHEN poi_class='amenity'  THEN poi_xarea_m2 ELSE 0 END) AS poi_amenity_xarea_m2_sum,
    SUM(CASE WHEN poi_class='shop'     THEN poi_xarea_m2 ELSE 0 END) AS poi_shop_xarea_m2_sum,
    SUM(CASE WHEN poi_class='tourism'  THEN poi_xarea_m2 ELSE 0 END) AS poi_tourism_xarea_m2_sum,
    SUM(CASE WHEN poi_class='office'   THEN poi_xarea_m2 ELSE 0 END) AS poi_office_xarea_m2_sum,
    SUM(CASE WHEN poi_class='leisure'  THEN poi_xarea_m2 ELSE 0 END) AS poi_leisure_xarea_m2_sum,
    SUM(CASE WHEN poi_class='sport'    THEN poi_xarea_m2 ELSE 0 END) AS poi_sport_xarea_m2_sum,
    SUM(CASE WHEN poi_class='building' THEN poi_xarea_m2 ELSE 0 END) AS poi_building_xarea_m2_sum,
    SUM(CASE WHEN poi_class='landuse'  THEN poi_xarea_m2 ELSE 0 END) AS poi_landuse_xarea_m2_sum,

    SUM(CASE WHEN poi_class='amenity'  THEN poi_xarea_m2 ELSE 0 END) / NULLIF(ANY_VALUE(cell_area_m2),0) AS poi_amenity_xarea_share,
    SUM(CASE WHEN poi_class='shop'     THEN poi_xarea_m2 ELSE 0 END) / NULLIF(ANY_VALUE(cell_area_m2),0) AS poi_shop_xarea_share,
    SUM(CASE WHEN poi_class='tourism'  THEN poi_xarea_m2 ELSE 0 END) / NULLIF(ANY_VALUE(cell_area_m2),0) AS poi_tourism_xarea_share,
    SUM(CASE WHEN poi_class='office'   THEN poi_xarea_m2 ELSE 0 END) / NULLIF(ANY_VALUE(cell_area_m2),0) AS poi_office_xarea_share,
    SUM(CASE WHEN poi_class='leisure'  THEN poi_xarea_m2 ELSE 0 END) / NULLIF(ANY_VALUE(cell_area_m2),0) AS poi_leisure_xarea_share,
    SUM(CASE WHEN poi_class='sport'    THEN poi_xarea_m2 ELSE 0 END) / NULLIF(ANY_VALUE(cell_area_m2),0) AS poi_sport_xarea_share,
    SUM(CASE WHEN poi_class='building' THEN poi_xarea_m2 ELSE 0 END) / NULLIF(ANY_VALUE(cell_area_m2),0) AS poi_building_xarea_share,
    SUM(CASE WHEN poi_class='landuse'  THEN poi_xarea_m2 ELSE 0 END) / NULLIF(ANY_VALUE(cell_area_m2),0) AS poi_landuse_xarea_share,

    MAX(load_ts) AS last_load_ts
  FROM metrics
  WHERE poi_xarea_m2 IS NOT NULL AND poi_xarea_m2 > 0
  GROUP BY 1,2,3
)

SELECT * FROM agg
;


---QA / validation queries
-- WITH s AS (
--   SELECT ST_GeomFromWKB(geom_wkb) AS g
--   FROM geo_databricks_sub.silver.poi_areas
--   WHERE geom_wkb IS NOT NULL
-- )
-- SELECT
--   COUNT(*) AS total,
--   SUM(CASE WHEN g IS NULL THEN 1 ELSE 0 END) AS geom_null_cnt,
--   SUM(CASE WHEN g IS NOT NULL AND ST_IsEmpty(g) THEN 1 ELSE 0 END) AS geom_empty_cnt,
--   SUM(CASE WHEN g IS NOT NULL AND NOT ST_IsEmpty(g) AND NOT ST_IsValid(g) THEN 1 ELSE 0 END) AS geom_invalid_cnt,
--   SUM(CASE
--         WHEN g IS NOT NULL AND NOT ST_IsEmpty(g) AND NOT ST_IsValid(g)
--          AND ST_IsValid(ST_Buffer(g, 0.0))
--         THEN 1 ELSE 0
--       END) AS fixed_by_buffer0
-- FROM s;

-- SELECT
--   COUNT(*) AS rows_cnt,
--   SUM(CASE WHEN cell_area_m2 <= 0 OR cell_area_m2 IS NULL THEN 1 ELSE 0 END) AS bad_cell_area_cnt,
--   SUM(CASE WHEN poi_xarea_m2_sum < 0 THEN 1 ELSE 0 END) AS neg_xarea_cnt,
--   SUM(CASE WHEN poi_xarea_share < 0 THEN 1 ELSE 0 END) AS neg_share_cnt,
--   MAX(poi_xarea_share) AS max_share,
--   approx_percentile(poi_xarea_share, 0.99) AS p99_share,
--   approx_percentile(poi_xarea_share, 0.999) AS p999_share
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets;


-- SELECT region_code, region, h3_r10, cell_area_m2, poi_xarea_m2_sum, poi_xarea_share
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets
-- ORDER BY poi_xarea_share DESC
-- LIMIT 50;

-- SELECT COUNT(*) AS rows_cnt
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets;

-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets
-- WHERE cell_wkt_4326 IS NULL OR TRIM(cell_wkt_4326) = ''
--    OR cell_center_wkt_4326 IS NULL OR TRIM(cell_center_wkt_4326) = ''
-- LIMIT 50;

-- -----------------------------------------------------------------------------
-- OPTIMIZE / STATS
-- NOTE: region_code, region are partition columns -> do NOT ZORDER by them
-- -----------------------------------------------------------------------------
OPTIMIZE geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets
ZORDER BY (h3_r10);

ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets COMPUTE STATISTICS;
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets COMPUTE STATISTICS FOR COLUMNS h3_r10;



-- =============================================================================
-- GOLD.FEAT_H3_POI_AREAS_R10_XAREA_CLASSBUCKETS_DENSE (Databricks / Delta)
--
-- Dense ML-ready view:
--   dim_h3_r10_cells LEFT JOIN sparse poi_xarea features -> 1 row per H3 cell.
--
-- Sources:
--   geo_databricks_sub.silver.dim_h3_r10_cells
--     - (region_code, region, h3_r10, cell_wkt_4326, cell_center_wkt_4326)
--   geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets  (sparse)
--     - (region_code, region, h3_r10, counts/sums/shares, last_load_ts)
--
-- Semantics:
--   - counts/sums are zero-filled if cell has no POI polygons
--   - has_poi_areas flag marks coverage
--   - shares/densities are NULL for non-covered cells (ML-safe)
--
-- Grain:
--   (region_code, region, h3_r10)
-- =============================================================================

DROP TABLE IF EXISTS geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense;

CREATE OR REPLACE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense
USING DELTA
PARTITIONED BY (region_code, region)
AS
WITH d AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region      AS STRING) AS region,
    CAST(h3_r10       AS STRING) AS h3_r10,
    CAST(cell_wkt_4326 AS STRING)        AS cell_wkt_4326,
    CAST(cell_center_wkt_4326 AS STRING) AS cell_center_wkt_4326,
    -- TRUE area in m² from WKT by Geography
    ST_Area(ST_GeogFromText(cell_wkt_4326)) AS cell_area_m2
  FROM geo_databricks_sub.silver.dim_h3_r10_cells
  WHERE region_code IS NOT NULL
    AND region IS NOT NULL
    AND h3_r10 IS NOT NULL
    AND cell_wkt_4326 IS NOT NULL
),
s AS (
  SELECT
    CAST(region_code AS STRING) AS region_code,
    CAST(region      AS STRING) AS region,
    CAST(h3_r10      AS STRING) AS h3_r10,

    poi_areas_cnt,
    poi_xarea_m2_sum,

    poi_amenity_cnt,
    poi_shop_cnt,
    poi_tourism_cnt,
    poi_office_cnt,
    poi_leisure_cnt,
    poi_sport_cnt,
    poi_building_cnt,
    poi_landuse_cnt,

    poi_amenity_xarea_m2_sum,
    poi_shop_xarea_m2_sum,
    poi_tourism_xarea_m2_sum,
    poi_office_xarea_m2_sum,
    poi_leisure_xarea_m2_sum,
    poi_sport_xarea_m2_sum,
    poi_building_xarea_m2_sum,
    poi_landuse_xarea_m2_sum,

    last_load_ts
  FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets
)
SELECT
  d.region_code,
  d.region,
  d.h3_r10,
  d.cell_area_m2,
  d.cell_wkt_4326,
  d.cell_center_wkt_4326,

  CASE WHEN s.h3_r10 IS NULL THEN false ELSE true END AS has_poi_areas,

  /* counts/sums: zero-fill */
  COALESCE(s.poi_areas_cnt, 0)     AS poi_areas_cnt,
  COALESCE(s.poi_xarea_m2_sum, 0)  AS poi_xarea_m2_sum,

  COALESCE(s.poi_amenity_cnt, 0)   AS poi_amenity_cnt,
  COALESCE(s.poi_shop_cnt, 0)      AS poi_shop_cnt,
  COALESCE(s.poi_tourism_cnt, 0)   AS poi_tourism_cnt,
  COALESCE(s.poi_office_cnt, 0)    AS poi_office_cnt,
  COALESCE(s.poi_leisure_cnt, 0)   AS poi_leisure_cnt,
  COALESCE(s.poi_sport_cnt, 0)     AS poi_sport_cnt,
  COALESCE(s.poi_building_cnt, 0)  AS poi_building_cnt,
  COALESCE(s.poi_landuse_cnt, 0)   AS poi_landuse_cnt,

  COALESCE(s.poi_amenity_xarea_m2_sum, 0)  AS poi_amenity_xarea_m2_sum,
  COALESCE(s.poi_shop_xarea_m2_sum, 0)     AS poi_shop_xarea_m2_sum,
  COALESCE(s.poi_tourism_xarea_m2_sum, 0)  AS poi_tourism_xarea_m2_sum,
  COALESCE(s.poi_office_xarea_m2_sum, 0)   AS poi_office_xarea_m2_sum,
  COALESCE(s.poi_leisure_xarea_m2_sum, 0)  AS poi_leisure_xarea_m2_sum,
  COALESCE(s.poi_sport_xarea_m2_sum, 0)    AS poi_sport_xarea_m2_sum,
  COALESCE(s.poi_building_xarea_m2_sum, 0) AS poi_building_xarea_m2_sum,
  COALESCE(s.poi_landuse_xarea_m2_sum, 0)  AS poi_landuse_xarea_m2_sum,

  /* shares + densities: NULL if no coverage or area invalid */
  CASE
    WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL
    ELSE s.poi_xarea_m2_sum / d.cell_area_m2
  END AS poi_xarea_share,

  CASE
    WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL
    ELSE s.poi_areas_cnt / (d.cell_area_m2 / 1e6)
  END AS poi_areas_per_km2,

  CASE
    WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL
    ELSE s.poi_xarea_m2_sum / (d.cell_area_m2 / 1e6)
  END AS poi_xarea_m2_per_km2,

  /* class shares by count: NULL if poi_areas_cnt=0 */
  CASE WHEN s.h3_r10 IS NULL OR COALESCE(s.poi_areas_cnt,0)=0 THEN NULL ELSE s.poi_amenity_cnt / s.poi_areas_cnt END AS poi_amenity_share,
  CASE WHEN s.h3_r10 IS NULL OR COALESCE(s.poi_areas_cnt,0)=0 THEN NULL ELSE s.poi_shop_cnt    / s.poi_areas_cnt END AS poi_shop_share,
  CASE WHEN s.h3_r10 IS NULL OR COALESCE(s.poi_areas_cnt,0)=0 THEN NULL ELSE s.poi_tourism_cnt / s.poi_areas_cnt END AS poi_tourism_share,
  CASE WHEN s.h3_r10 IS NULL OR COALESCE(s.poi_areas_cnt,0)=0 THEN NULL ELSE s.poi_office_cnt  / s.poi_areas_cnt END AS poi_office_share,
  CASE WHEN s.h3_r10 IS NULL OR COALESCE(s.poi_areas_cnt,0)=0 THEN NULL ELSE s.poi_leisure_cnt / s.poi_areas_cnt END AS poi_leisure_share,
  CASE WHEN s.h3_r10 IS NULL OR COALESCE(s.poi_areas_cnt,0)=0 THEN NULL ELSE s.poi_sport_cnt   / s.poi_areas_cnt END AS poi_sport_share,
  CASE WHEN s.h3_r10 IS NULL OR COALESCE(s.poi_areas_cnt,0)=0 THEN NULL ELSE s.poi_building_cnt/ s.poi_areas_cnt END AS poi_building_share,
  CASE WHEN s.h3_r10 IS NULL OR COALESCE(s.poi_areas_cnt,0)=0 THEN NULL ELSE s.poi_landuse_cnt / s.poi_areas_cnt END AS poi_landuse_share,

  /* class xarea shares: NULL if no coverage or area invalid */
  CASE WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL ELSE s.poi_amenity_xarea_m2_sum  / d.cell_area_m2 END AS poi_amenity_xarea_share,
  CASE WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL ELSE s.poi_shop_xarea_m2_sum     / d.cell_area_m2 END AS poi_shop_xarea_share,
  CASE WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL ELSE s.poi_tourism_xarea_m2_sum  / d.cell_area_m2 END AS poi_tourism_xarea_share,
  CASE WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL ELSE s.poi_office_xarea_m2_sum   / d.cell_area_m2 END AS poi_office_xarea_share,
  CASE WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL ELSE s.poi_leisure_xarea_m2_sum  / d.cell_area_m2 END AS poi_leisure_xarea_share,
  CASE WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL ELSE s.poi_sport_xarea_m2_sum    / d.cell_area_m2 END AS poi_sport_xarea_share,
  CASE WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL ELSE s.poi_building_xarea_m2_sum / d.cell_area_m2 END AS poi_building_xarea_share,
  CASE WHEN s.h3_r10 IS NULL OR d.cell_area_m2 IS NULL OR d.cell_area_m2 <= 0 THEN NULL ELSE s.poi_landuse_xarea_m2_sum  / d.cell_area_m2 END AS poi_landuse_xarea_share,

  /* last_load_ts: NULL if no coverage */
  s.last_load_ts
FROM d
LEFT JOIN s
  ON s.region_code = d.region_code
 AND s.region      = d.region
 AND s.h3_r10      = d.h3_r10
;

-- SELECT
--   (SELECT COUNT(*) FROM geo_databricks_sub.silver.dim_h3_r10_cells) AS dim_cnt,
--   (SELECT COUNT(*) FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense) AS dense_cnt;

--   SELECT
--   COUNT(*) AS rows_total,
--   SUM(CASE WHEN has_poi_areas=false AND last_load_ts IS NULL THEN 1 ELSE 0 END) AS ok_empty,
--   SUM(CASE WHEN has_poi_areas=true  AND last_load_ts IS NOT NULL THEN 1 ELSE 0 END) AS ok_covered,
--   SUM(CASE WHEN (has_poi_areas=false AND last_load_ts IS NOT NULL)
--          OR (has_poi_areas=true  AND last_load_ts IS NULL) THEN 1 ELSE 0 END) AS bad_rows
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense;

-- SELECT
--   MAX(poi_xarea_share) AS max_share,
--   percentile_approx(poi_xarea_share, 0.99)  AS p99_share,
--   percentile_approx(poi_xarea_share, 0.999) AS p999_share
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense;

-- -- top 50 by count
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense
-- ORDER BY poi_areas_cnt DESC
-- LIMIT 50;

-- -- top 50 by intersection area
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense
-- ORDER BY poi_xarea_m2_sum DESC
-- LIMIT 50;

-- -- top 50 by share (only covered)
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense
-- WHERE poi_xarea_share IS NOT NULL
-- ORDER BY poi_xarea_share DESC
-- LIMIT 50;

-- -- top 50 by density
-- SELECT *
-- FROM geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense
-- WHERE poi_areas_per_km2 IS NOT NULL
-- ORDER BY poi_areas_per_km2 DESC
-- LIMIT 50;

-- OPTIMIZE / ZORDER (не по partition columns!)
OPTIMIZE geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense ZORDER BY (h3_r10);
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense COMPUTE STATISTICS;
ANALYZE TABLE geo_databricks_sub.gold.feat_h3_poi_areas_r10_xarea_classbuckets_dense COMPUTE STATISTICS FOR COLUMNS h3_r10;
