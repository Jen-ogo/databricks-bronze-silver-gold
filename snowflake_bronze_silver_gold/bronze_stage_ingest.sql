/* =============================================================================
   GEO_PROJECT — Canonical bootstrap + BRONZE ingest (OSM + EUROSTAT + GISCO)
   Pattern: 1 STAGE -> 1 DIR STREAM -> 1 APPLY procedure -> 2 TASKS:
            (1) stage refresh, (2) call APPLY after refresh when stream has data
   Notes:
   - OSM uses *_sf.parquet (WKT) because Snowflake can't read WKB geometry parquet reliably.
   - We parse partitions from RELATIVE_PATH to match Databricks Bronze structure.
   - We do "idempotent by file": delete rows where source_file IN files_to_process, then insert.
   ============================================================================= */

-- =============================================================================
-- 0) CONTEXT
-- =============================================================================
USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS GEO_PROJECT;
USE DATABASE GEO_PROJECT;

CREATE SCHEMA IF NOT EXISTS TASKS;
CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;

-- =============================================================================
-- 1) STORAGE INTEGRATION + FILE FORMAT + STAGES + DIRECTORY + STREAMS
-- =============================================================================
USE SCHEMA TASKS;

-- ---- 1.1 STORAGE INTEGRATION (idempotent)
-- You must set correct AZURE_TENANT_ID + allowed locations for your account.
CREATE OR REPLACE STORAGE INTEGRATION ADLS_GEO_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '3600a0f2-48df-4c6f-9503-7eded884a513'
  STORAGE_ALLOWED_LOCATIONS = ('azure://stgeodbxuc.blob.core.windows.net/uc-root');

DESC STORAGE INTEGRATION ADLS_GEO_INT;

-- ---- 1.2 FILE FORMAT
CREATE OR REPLACE FILE FORMAT TASKS.FF_PARQUET
  TYPE = PARQUET
  COMPRESSION = AUTO;

-- ---- 1.3 ROOT STAGE (optional)
CREATE OR REPLACE STAGE TASKS.GEO_ADLS_STAGE
  URL = 'azure://stgeodbxuc.blob.core.windows.net/uc-root'
  STORAGE_INTEGRATION = ADLS_GEO_INT
  FILE_FORMAT = TASKS.FF_PARQUET;

-- ---- 1.4 OSM RAW STAGE
CREATE OR REPLACE STAGE TASKS.OSM_RAW_STAGE
  URL = 'azure://stgeodbxuc.blob.core.windows.net/uc-root/raw/osm/'
  STORAGE_INTEGRATION = ADLS_GEO_INT
  FILE_FORMAT = TASKS.FF_PARQUET;

ALTER STAGE TASKS.OSM_RAW_STAGE
  SET DIRECTORY = ( ENABLE = TRUE AUTO_REFRESH = FALSE );

CREATE OR REPLACE STREAM TASKS.OSM_RAW_STAGE_DIR_S
  ON STAGE TASKS.OSM_RAW_STAGE;

-- ---- 1.5 EUROSTAT RAW STAGE
CREATE OR REPLACE STAGE TASKS.EUROSTAT_RAW_STAGE
  URL = 'azure://stgeodbxuc.blob.core.windows.net/uc-root/raw/eurostat/'
  STORAGE_INTEGRATION = ADLS_GEO_INT
  FILE_FORMAT = TASKS.FF_PARQUET;

ALTER STAGE TASKS.EUROSTAT_RAW_STAGE
  SET DIRECTORY = ( ENABLE = TRUE AUTO_REFRESH = FALSE );

CREATE OR REPLACE STREAM TASKS.EUROSTAT_RAW_STAGE_DIR_S
  ON STAGE TASKS.EUROSTAT_RAW_STAGE;

-- ---- 1.6 GISCO RAW STAGE
CREATE OR REPLACE STAGE TASKS.GISCO_RAW_STAGE
  URL = 'azure://stgeodbxuc.blob.core.windows.net/uc-root/raw/gisco/'
  STORAGE_INTEGRATION = ADLS_GEO_INT
  FILE_FORMAT = TASKS.FF_PARQUET;

ALTER STAGE TASKS.GISCO_RAW_STAGE
  SET DIRECTORY = ( ENABLE = TRUE AUTO_REFRESH = FALSE );

CREATE OR REPLACE STREAM TASKS.GISCO_RAW_STAGE_DIR_S
  ON STAGE TASKS.GISCO_RAW_STAGE;

  
DESC STORAGE INTEGRATION ADLS_GEO_INT;
LIST @TASKS.GEO_ADLS_STAGE;

-- =============================================================================
-- 2) BRONZE TABLES (match Databricks semantics)
-- =============================================================================
USE SCHEMA BRONZE;
USE ROLE SYSADMIN;
-- -----------------------------------------------------------------------------
-- 2.A) OSM tables: add COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
-- -----------------------------------------------------------------------------

CREATE OR REPLACE TABLE BRONZE.ADMIN (
  OSM_ID      STRING,
  NAME        STRING,
  ADMIN_LEVEL INTEGER,
  BOUNDARY    STRING,
  TYPE        STRING,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  COUNTRY     STRING,
  REGION      STRING,
  DT          DATE,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY, REGION, DT);

CREATE OR REPLACE TABLE BRONZE.ROADS (
  OSM_ID      STRING,
  NAME        STRING,
  HIGHWAY     STRING,
  BARRIER     STRING,
  MAN_MADE    STRING,
  RAILWAY     STRING,
  WATERWAY    STRING,
  Z_ORDER     INTEGER,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  COUNTRY     STRING,
  REGION      STRING,
  DT          DATE,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY, REGION, DT);

CREATE OR REPLACE TABLE BRONZE.CHARGING (
  OSM_ID      STRING,
  NAME        STRING,
  REF         STRING,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  COUNTRY     STRING,
  REGION      STRING,
  DT          DATE,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY, REGION, DT);

CREATE OR REPLACE TABLE BRONZE.POI_POINTS (
  OSM_ID      STRING,
  NAME        STRING,
  BARRIER     STRING,
  HIGHWAY     STRING,
  IS_IN       STRING,
  REF         STRING,
  MAN_MADE    STRING,
  PLACE       STRING,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  COUNTRY     STRING,
  REGION      STRING,
  DT          DATE,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY, REGION, DT);

CREATE OR REPLACE TABLE BRONZE.POI_POLYGONS (
  OSM_ID      STRING,
  OSM_WAY_ID  STRING,
  NAME        STRING,
  TYPE        STRING,
  AEROWAY     STRING,
  AMENITY     STRING,
  ADMIN_LEVEL STRING,
  BARRIER     STRING,
  BOUNDARY    STRING,
  BUILDING    STRING,
  CRAFT       STRING,
  HISTORIC    STRING,
  LANDUSE     STRING,
  LEISURE     STRING,
  MAN_MADE    STRING,
  MILITARY    STRING,
  NATURAL     STRING,
  OFFICE      STRING,
  PLACE       STRING,
  SHOP        STRING,
  SPORT       STRING,
  TOURISM     STRING,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  COUNTRY     STRING,
  REGION      STRING,
  DT          DATE,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY, REGION, DT);

CREATE OR REPLACE TABLE BRONZE.PT_POINTS (
  OSM_ID      STRING,
  NAME        STRING,
  BARRIER     STRING,
  HIGHWAY     STRING,
  IS_IN       STRING,
  REF         STRING,
  MAN_MADE    STRING,
  PLACE       STRING,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  COUNTRY     STRING,
  REGION      STRING,
  DT          DATE,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY, REGION, DT);

CREATE OR REPLACE TABLE BRONZE.PT_LINES (
  OSM_ID      STRING,
  NAME        STRING,
  AERIALWAY   STRING,
  HIGHWAY     STRING,
  WATERWAY    STRING,
  BARRIER     STRING,
  MAN_MADE    STRING,
  RAILWAY     STRING,
  Z_ORDER     INTEGER,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  COUNTRY     STRING,
  REGION      STRING,
  DT          DATE,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY, REGION, DT);

CREATE OR REPLACE TABLE BRONZE.BUILDINGS_ACTIVITY (
  OSM_ID      STRING,
  OSM_WAY_ID  STRING,
  NAME        STRING,
  TYPE        STRING,
  AEROWAY     STRING,
  AMENITY     STRING,
  ADMIN_LEVEL STRING,
  BARRIER     STRING,
  BOUNDARY    STRING,
  BUILDING    STRING,
  CRAFT       STRING,
  HISTORIC    STRING,
  LANDUSE     STRING,
  LEISURE     STRING,
  MAN_MADE    STRING,
  MILITARY    STRING,
  NATURAL     STRING,
  OFFICE      STRING,
  PLACE       STRING,
  SHOP        STRING,
  SPORT       STRING,
  TOURISM     STRING,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  COUNTRY     STRING,
  REGION      STRING,
  DT          DATE,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (COUNTRY, REGION, DT);

-- -----------------------------------------------------------------------------
-- 2.B) GISCO NUTS (keep your existing cols, ensure SOURCE_FILE/LOAD_TS exist)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.GISCO_NUTS (
  NUTS_ID     STRING,
  CNTR_CODE   STRING,
  NAME_LATN   STRING,
  LEVL_CODE   NUMBER(38,0),
  GEOM_WKT    STRING,

  YEAR        STRING,
  SCALE       STRING,
  CRS         STRING,
  LEVEL       NUMBER(38,0),

  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (YEAR, SCALE, CRS, LEVEL);

-- -----------------------------------------------------------------------------
-- 2.C) EUROSTAT: DEGURBA (LAU)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.EUROSTAT_LAU_DEGURBA (
  GISCO_ID    STRING,
  CNTR_CODE   STRING,
  LAU_ID      STRING,
  LAU_NAME    STRING,
  DGURBA      NUMBER(38,0),
  FID         NUMBER(38,0),
  GEOM_WKT    STRING,

  YEAR        STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (YEAR, CNTR_CODE);

-- -----------------------------------------------------------------------------
-- 2.D) EUROSTAT: Census grid 2021 Europe
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.CENSUS_GRID_2021_EUROPE (
  GRD_ID       STRING,
  T            NUMBER(38,0),
  M            NUMBER(38,0),
  F            NUMBER(38,0),
  Y_LT15       NUMBER(38,0),
  Y_1564       NUMBER(38,0),
  Y_GE65       NUMBER(38,0),
  EMP          NUMBER(38,0),
  NAT          NUMBER(38,0),
  EU_OTH       NUMBER(38,0),
  OTH          NUMBER(38,0),
  SAME         NUMBER(38,0),
  CHG_IN       NUMBER(38,0),
  CHG_OUT      NUMBER(38,0),

  T_CI         NUMBER(38,0),
  M_CI         NUMBER(38,0),
  F_CI         NUMBER(38,0),
  Y_LT15_CI    NUMBER(38,0),
  Y_1564_CI    NUMBER(38,0),
  Y_GE65_CI    NUMBER(38,0),
  EMP_CI       NUMBER(38,0),
  NAT_CI       NUMBER(38,0),
  EU_OTH_CI    NUMBER(38,0),
  OTH_CI       NUMBER(38,0),
  SAME_CI      NUMBER(38,0),
  CHG_IN_CI    NUMBER(38,0),
  CHG_OUT_CI   NUMBER(38,0),

  LAND_SURFACE FLOAT,
  POPULATED    NUMBER(38,0),

  GEOM_WKT     STRING,
  SOURCE_FILE  STRING,
  LOAD_TS      TIMESTAMP_NTZ
)
CLUSTER BY (GRD_ID);

-- -----------------------------------------------------------------------------
-- 2.E) EUROSTAT: EV trend (tran_r_elvehst) snapshot-style
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TABLE BRONZE.EUROSTAT_TRAN_R_ELVEHST (
  SOURCE_FILE   STRING,
  SNAPSHOT      STRING,

  DATASET       STRING,
  FREQ          STRING,
  VEHICLE       STRING,
  UNIT          STRING,
  GEO           STRING,
  YEAR          NUMBER(38,0),

  VALUE         NUMBER(38,0),

  INGEST_TS_RAW NUMBER(38,0),
  INGEST_TS     TIMESTAMP_NTZ,

  LOAD_TS       TIMESTAMP_NTZ
)
CLUSTER BY (SNAPSHOT, GEO, YEAR, VEHICLE);

-- =============================================================================
-- 3) APPLY PROCEDURES (1 per STAGE)
-- =============================================================================
USE SCHEMA TASKS;
USE ROLE ACCOUNTADMIN;

-- -----------------------------------------------------------------------------
-- 3.A) APPLY_OSM_ALL_STAGE_CHANGES (loads all OSM *_sf.parquet)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_ALL_STAGE_CHANGES()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_changed_cnt NUMBER DEFAULT 0;
BEGIN
  -- capture all stage changes once
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING       AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER    AS SIZE,
    METADATA$ACTION::STRING     AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S;

  v_changed_cnt := (SELECT COUNT(*) FROM _CHANGED);

  IF (v_changed_cnt = 0) THEN
    RETURN OBJECT_CONSTRUCT('status','NOOP','changed',0);
  END IF;

  -- helper: files_to_process per dataset (only INSERT/positive size)
  -- We do per-dataset blocks to keep patterns clean.

  ----------------------------------------------------------------------------
  -- ADMIN
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_ADMIN AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT'
    AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '.*/admin/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/admin_sf[.]parquet$');

  DELETE FROM BRONZE.ADMIN t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.ADMIN t
  USING _F_ADMIN f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.ADMIN (
    OSM_ID, NAME, ADMIN_LEVEL, BOUNDARY, TYPE, OTHER_TAGS, GEOM_WKT,
    COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:name::STRING,
    s.$1:admin_level::INTEGER,
    s.$1:boundary::STRING,
    s.$1:type::STRING,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,

    SPLIT_PART(f.RELATIVE_PATH,'/',1)::STRING AS COUNTRY,
    SPLIT_PART(f.RELATIVE_PATH,'/',2)::STRING AS REGION,
    TO_DATE(REGEXP_SUBSTR(f.RELATIVE_PATH,'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})',1,1,'e',1)) AS DT,

    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*admin_sf[.]parquet$') s
  JOIN _F_ADMIN f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- ROADS
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_ROADS AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT'
    AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '.*/roads/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/roads_sf[.]parquet$');

  DELETE FROM BRONZE.ROADS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.ROADS t
  USING _F_ROADS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.ROADS (
    OSM_ID, NAME, HIGHWAY, BARRIER, MAN_MADE, RAILWAY, WATERWAY, Z_ORDER, OTHER_TAGS, GEOM_WKT,
    COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:name::STRING,
    s.$1:highway::STRING,
    s.$1:barrier::STRING,
    s.$1:man_made::STRING,
    s.$1:railway::STRING,
    s.$1:waterway::STRING,
    s.$1:z_order::INTEGER,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,

    SPLIT_PART(f.RELATIVE_PATH,'/',1)::STRING,
    SPLIT_PART(f.RELATIVE_PATH,'/',2)::STRING,
    TO_DATE(REGEXP_SUBSTR(f.RELATIVE_PATH,'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})',1,1,'e',1)),

    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*roads_sf[.]parquet$') s
  JOIN _F_ROADS f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- CHARGING
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_CHARGING AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT'
    AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '.*/charging/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/charging_sf[.]parquet$');

  DELETE FROM BRONZE.CHARGING t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.CHARGING t
  USING _F_CHARGING f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.CHARGING (
    OSM_ID, NAME, REF, OTHER_TAGS, GEOM_WKT,
    COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:name::STRING,
    s.$1:ref::STRING,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,

    SPLIT_PART(f.RELATIVE_PATH,'/',1)::STRING,
    SPLIT_PART(f.RELATIVE_PATH,'/',2)::STRING,
    TO_DATE(REGEXP_SUBSTR(f.RELATIVE_PATH,'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})',1,1,'e',1)),

    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*charging_sf[.]parquet$') s
  JOIN _F_CHARGING f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- POI_POINTS
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_POI_P AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT'
    AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '.*/poi_points/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/poi_points_sf[.]parquet$');

  DELETE FROM BRONZE.POI_POINTS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.POI_POINTS t
  USING _F_POI_P f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.POI_POINTS (
    OSM_ID, NAME, BARRIER, HIGHWAY, IS_IN, REF, MAN_MADE, PLACE, OTHER_TAGS, GEOM_WKT,
    COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:name::STRING,
    s.$1:barrier::STRING,
    s.$1:highway::STRING,
    s.$1:is_in::STRING,
    s.$1:ref::STRING,
    s.$1:man_made::STRING,
    s.$1:place::STRING,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,

    SPLIT_PART(f.RELATIVE_PATH,'/',1)::STRING,
    SPLIT_PART(f.RELATIVE_PATH,'/',2)::STRING,
    TO_DATE(REGEXP_SUBSTR(f.RELATIVE_PATH,'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})',1,1,'e',1)),

    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*poi_points_sf[.]parquet$') s
  JOIN _F_POI_P f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- POI_POLYGONS
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_POI_POLY AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT'
    AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '.*/poi_polygons/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/poi_polygons_sf[.]parquet$');

  DELETE FROM BRONZE.POI_POLYGONS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.POI_POLYGONS t
  USING _F_POI_POLY f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.POI_POLYGONS (
    OSM_ID, OSM_WAY_ID, NAME, TYPE, AEROWAY, AMENITY, ADMIN_LEVEL, BARRIER, BOUNDARY, BUILDING,
    CRAFT, HISTORIC, LANDUSE, LEISURE, MAN_MADE, MILITARY, NATURAL, OFFICE, PLACE, SHOP, SPORT,
    TOURISM, OTHER_TAGS, GEOM_WKT,
    COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:osm_way_id::STRING,
    s.$1:name::STRING,
    s.$1:type::STRING,
    s.$1:aeroway::STRING,
    s.$1:amenity::STRING,
    s.$1:admin_level::STRING,
    s.$1:barrier::STRING,
    s.$1:boundary::STRING,
    s.$1:building::STRING,
    s.$1:craft::STRING,
    s.$1:historic::STRING,
    s.$1:landuse::STRING,
    s.$1:leisure::STRING,
    s.$1:man_made::STRING,
    s.$1:military::STRING,
    s.$1:natural::STRING,
    s.$1:office::STRING,
    s.$1:place::STRING,
    s.$1:shop::STRING,
    s.$1:sport::STRING,
    s.$1:tourism::STRING,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,

    SPLIT_PART(f.RELATIVE_PATH,'/',1)::STRING,
    SPLIT_PART(f.RELATIVE_PATH,'/',2)::STRING,
    TO_DATE(REGEXP_SUBSTR(f.RELATIVE_PATH,'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})',1,1,'e',1)),

    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*poi_polygons_sf[.]parquet$') s
  JOIN _F_POI_POLY f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- PT_POINTS
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_PT_P AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT'
    AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '.*/pt_points/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/pt_points_sf[.]parquet$');

  DELETE FROM BRONZE.PT_POINTS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.PT_POINTS t
  USING _F_PT_P f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.PT_POINTS (
    OSM_ID, NAME, BARRIER, HIGHWAY, IS_IN, REF, MAN_MADE, PLACE, OTHER_TAGS, GEOM_WKT,
    COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:name::STRING,
    s.$1:barrier::STRING,
    s.$1:highway::STRING,
    s.$1:is_in::STRING,
    s.$1:ref::STRING,
    s.$1:man_made::STRING,
    s.$1:place::STRING,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,

    SPLIT_PART(f.RELATIVE_PATH,'/',1)::STRING,
    SPLIT_PART(f.RELATIVE_PATH,'/',2)::STRING,
    TO_DATE(REGEXP_SUBSTR(f.RELATIVE_PATH,'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})',1,1,'e',1)),

    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*pt_points_sf[.]parquet$') s
  JOIN _F_PT_P f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- PT_LINES
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_PT_L AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT'
    AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '.*/pt_lines/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/pt_lines_sf[.]parquet$');

  DELETE FROM BRONZE.PT_LINES t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.PT_LINES t
  USING _F_PT_L f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.PT_LINES (
    OSM_ID, NAME, AERIALWAY, HIGHWAY, WATERWAY, BARRIER, MAN_MADE, RAILWAY, Z_ORDER, OTHER_TAGS, GEOM_WKT,
    COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:name::STRING,
    s.$1:aerialway::STRING,
    s.$1:highway::STRING,
    s.$1:waterway::STRING,
    s.$1:barrier::STRING,
    s.$1:man_made::STRING,
    s.$1:railway::STRING,
    s.$1:z_order::INTEGER,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,

    SPLIT_PART(f.RELATIVE_PATH,'/',1)::STRING,
    SPLIT_PART(f.RELATIVE_PATH,'/',2)::STRING,
    TO_DATE(REGEXP_SUBSTR(f.RELATIVE_PATH,'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})',1,1,'e',1)),

    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*pt_lines_sf[.]parquet$') s
  JOIN _F_PT_L f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- BUILDINGS_ACTIVITY
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_BA AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT'
    AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '.*/buildings_activity/dt=[0-9]{4}-[0-9]{2}-[0-9]{2}/buildings_activity_sf[.]parquet$');

  DELETE FROM BRONZE.BUILDINGS_ACTIVITY t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.BUILDINGS_ACTIVITY t
  USING _F_BA f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.BUILDINGS_ACTIVITY (
    OSM_ID, OSM_WAY_ID, NAME, TYPE, AEROWAY, AMENITY, ADMIN_LEVEL, BARRIER, BOUNDARY, BUILDING,
    CRAFT, HISTORIC, LANDUSE, LEISURE, MAN_MADE, MILITARY, NATURAL, OFFICE, PLACE, SHOP, SPORT,
    TOURISM, OTHER_TAGS, GEOM_WKT,
    COUNTRY, REGION, DT, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:osm_way_id::STRING,
    s.$1:name::STRING,
    s.$1:type::STRING,
    s.$1:aeroway::STRING,
    s.$1:amenity::STRING,
    s.$1:admin_level::STRING,
    s.$1:barrier::STRING,
    s.$1:boundary::STRING,
    s.$1:building::STRING,
    s.$1:craft::STRING,
    s.$1:historic::STRING,
    s.$1:landuse::STRING,
    s.$1:leisure::STRING,
    s.$1:man_made::STRING,
    s.$1:military::STRING,
    s.$1:natural::STRING,
    s.$1:office::STRING,
    s.$1:place::STRING,
    s.$1:shop::STRING,
    s.$1:sport::STRING,
    s.$1:tourism::STRING,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,

    SPLIT_PART(f.RELATIVE_PATH,'/',1)::STRING,
    SPLIT_PART(f.RELATIVE_PATH,'/',2)::STRING,
    TO_DATE(REGEXP_SUBSTR(f.RELATIVE_PATH,'dt=([0-9]{4}-[0-9]{2}-[0-9]{2})',1,1,'e',1)),

    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*buildings_activity_sf[.]parquet$') s
  JOIN _F_BA f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  RETURN OBJECT_CONSTRUCT('status','OK','changed',v_changed_cnt);
END;
$$;

-- -----------------------------------------------------------------------------
-- 3.B) APPLY_GISCO_NUTS_STAGE_CHANGES (your mapping, cleaned)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE TASKS.APPLY_GISCO_NUTS_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING    AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING  AS ACTION
  FROM TASKS.GISCO_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(
    RELATIVE_PATH,
    '^nuts/year=[0-9]{4}/scale=[^/]+/crs=[0-9]+/level=[0-9]+/.*[.]parquet$'
  );

  IF ((SELECT COUNT(*) FROM _CHANGED) = 0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.GISCO_NUTS) = 0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.GISCO_RAW_STAGE)
      WHERE REGEXP_LIKE(
        RELATIVE_PATH,
        '^nuts/year=[0-9]{4}/scale=[^/]+/crs=[0-9]+/level=[0-9]+/.*[.]parquet$'
      )
        AND COALESCE(SIZE,0) > 0;
    ELSE
      RETURN 'NOOP: no GISCO NUTS changes';
    END IF;
  ELSE
    CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
    SELECT RELATIVE_PATH
    FROM _CHANGED
    WHERE ACTION='INSERT' AND SIZE > 0;
  END IF;

  DELETE FROM BRONZE.GISCO_NUTS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.GISCO_NUTS t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.GISCO_NUTS (
    NUTS_ID, CNTR_CODE, NAME_LATN, LEVL_CODE,
    GEOM_WKT, YEAR, SCALE, CRS, LEVEL,
    SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:"NUTS_ID"::STRING,
    s.$1:"CNTR_CODE"::STRING,
    s.$1:"NAME_LATN"::STRING,
    s.$1:"LEVL_CODE"::NUMBER(38,0),
    s.$1:"geom_wkt"::STRING AS GEOM_WKT,

    REGEXP_SUBSTR(f.RELATIVE_PATH, 'year=([0-9]{4})', 1, 1, 'e', 1)::STRING AS YEAR,
    REGEXP_SUBSTR(f.RELATIVE_PATH, 'scale=([^/]+)',   1, 1, 'e', 1)::STRING AS SCALE,
    REGEXP_SUBSTR(f.RELATIVE_PATH, 'crs=([0-9]+)',     1, 1, 'e', 1)::STRING AS CRS,
    REGEXP_SUBSTR(f.RELATIVE_PATH, 'level=([0-9]+)',   1, 1, 'e', 1)::NUMBER(38,0) AS LEVEL,

    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.GISCO_RAW_STAGE (PATTERN => '.*[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  RETURN 'OK: GISCO NUTS loaded';
END;
$$;

-- -----------------------------------------------------------------------------
-- 3.C) APPLY_EUROSTAT_ALL_STAGE_CHANGES (DEGURBA + CENSUS + TRAN_R_ELVEHST)
-- -----------------------------------------------------------------------------
CREATE OR REPLACE PROCEDURE TASKS.APPLY_EUROSTAT_ALL_STAGE_CHANGES()
RETURNS VARIANT
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_changed_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING    AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING  AS ACTION
  FROM TASKS.EUROSTAT_RAW_STAGE_DIR_S;

  v_changed_cnt := (SELECT COUNT(*) FROM _CHANGED);
  IF (v_changed_cnt = 0) THEN
    RETURN OBJECT_CONSTRUCT('status','NOOP','changed',0);
  END IF;

  ----------------------------------------------------------------------------
  -- DEGURBA (lau) *_sf.parquet
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_DEG AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT' AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '^degurba/lau/year=[0-9]{4}/.*_sf[.]parquet$');

  DELETE FROM BRONZE.EUROSTAT_LAU_DEGURBA t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.EUROSTAT_LAU_DEGURBA t
  USING _F_DEG f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.EUROSTAT_LAU_DEGURBA (
    GISCO_ID, CNTR_CODE, LAU_ID, LAU_NAME, DGURBA, FID, GEOM_WKT,
    YEAR, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:"GISCO_ID"::STRING,
    s.$1:"CNTR_CODE"::STRING,
    s.$1:"LAU_ID"::STRING,
    s.$1:"LAU_NAME"::STRING,
    s.$1:"DGURBA"::NUMBER(38,0),
    s.$1:"FID"::NUMBER(38,0),
    s.$1:"geom_wkt"::STRING,

    REGEXP_SUBSTR(f.RELATIVE_PATH, 'year=([0-9]{4})', 1, 1, 'e', 1)::STRING AS YEAR,
    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.EUROSTAT_RAW_STAGE (PATTERN => '.*_sf[.]parquet$') s
  JOIN _F_DEG f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- CENSUS GRID 2021 *_sf.parquet
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_GRID AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT' AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '^population_grid/europe/census_grid_2021/.*_sf[.]parquet$');

  DELETE FROM BRONZE.CENSUS_GRID_2021_EUROPE t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.CENSUS_GRID_2021_EUROPE t
  USING _F_GRID f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.CENSUS_GRID_2021_EUROPE (
    GRD_ID,
    T, M, F,
    Y_LT15, Y_1564, Y_GE65,
    EMP, NAT, EU_OTH, OTH,
    SAME, CHG_IN, CHG_OUT,
    T_CI, M_CI, F_CI,
    Y_LT15_CI, Y_1564_CI, Y_GE65_CI,
    EMP_CI, NAT_CI, EU_OTH_CI, OTH_CI,
    SAME_CI, CHG_IN_CI, CHG_OUT_CI,
    LAND_SURFACE, POPULATED,
    GEOM_WKT,
    SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:GRD_ID::STRING,

    NULLIF(s.$1:T::NUMBER(38,0), -9999),
    NULLIF(s.$1:M::NUMBER(38,0), -9999),
    NULLIF(s.$1:F::NUMBER(38,0), -9999),

    NULLIF(s.$1:Y_LT15::NUMBER(38,0), -9999),
    NULLIF(s.$1:Y_1564::NUMBER(38,0), -9999),
    NULLIF(s.$1:Y_GE65::NUMBER(38,0), -9999),

    NULLIF(s.$1:EMP::NUMBER(38,0), -9999),
    NULLIF(s.$1:NAT::NUMBER(38,0), -9999),
    NULLIF(s.$1:EU_OTH::NUMBER(38,0), -9999),
    NULLIF(s.$1:OTH::NUMBER(38,0), -9999),

    NULLIF(s.$1:SAME::NUMBER(38,0), -9999),
    NULLIF(s.$1:CHG_IN::NUMBER(38,0), -9999),
    NULLIF(s.$1:CHG_OUT::NUMBER(38,0), -9999),

    NULLIF(s.$1:T_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:M_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:F_CI::NUMBER(38,0), -9999),

    NULLIF(s.$1:Y_LT15_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:Y_1564_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:Y_GE65_CI::NUMBER(38,0), -9999),

    NULLIF(s.$1:EMP_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:NAT_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:EU_OTH_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:OTH_CI::NUMBER(38,0), -9999),

    NULLIF(s.$1:SAME_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:CHG_IN_CI::NUMBER(38,0), -9999),
    NULLIF(s.$1:CHG_OUT_CI::NUMBER(38,0), -9999),

    NULLIF(s.$1:LAND_SURFACE::FLOAT, -9999),
    NULLIF(s.$1:POPULATED::NUMBER(38,0), -9999),

    s.$1:geom_wkt::STRING,

    f.RELATIVE_PATH::STRING,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ
  FROM @TASKS.EUROSTAT_RAW_STAGE (PATTERN => '.*census_grid_2021/.*_sf[.]parquet$') s
  JOIN _F_GRID f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH;

  ----------------------------------------------------------------------------
  -- TRAN_R_ELVEHST tidy snapshot parquet
  ----------------------------------------------------------------------------
  CREATE OR REPLACE TEMP TABLE _F_EV AS
  SELECT DISTINCT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT' AND SIZE > 0
    AND REGEXP_LIKE(RELATIVE_PATH, '^tran_r_elvehst/snapshot=[^/]+/tran_r_elvehst_tidy[.]parquet$');

  DELETE FROM BRONZE.EUROSTAT_TRAN_R_ELVEHST t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.EUROSTAT_TRAN_R_ELVEHST t
  USING _F_EV f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.EUROSTAT_TRAN_R_ELVEHST (
    SOURCE_FILE, SNAPSHOT, DATASET, FREQ, VEHICLE, UNIT, GEO, YEAR,
    VALUE, INGEST_TS_RAW, INGEST_TS, LOAD_TS
  )
  SELECT
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    REGEXP_SUBSTR(f.RELATIVE_PATH, 'snapshot=([^/]+)', 1, 1, 'e', 1)::STRING AS SNAPSHOT,

    s.$1:dataset::TEXT  AS DATASET,
    s.$1:freq::TEXT     AS FREQ,
    s.$1:vehicle::TEXT  AS VEHICLE,
    s.$1:unit::TEXT     AS UNIT,
    s.$1:geo::TEXT      AS GEO,

    TRY_TO_NUMBER(s.$1:time::TEXT) AS YEAR,
    NULLIF(s.$1:value::FLOAT, 0)   AS VALUE,

    s.$1:ingest_ts::NUMBER(38,0)                   AS INGEST_TS_RAW,
    TO_TIMESTAMP_NTZ(s.$1:ingest_ts::NUMBER(38,0), 9) AS INGEST_TS,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ             AS LOAD_TS
  FROM @TASKS.EUROSTAT_RAW_STAGE
       (PATTERN => '.*tran_r_elvehst/snapshot=[^/]+/tran_r_elvehst_tidy[.]parquet$') s
  JOIN _F_EV f
    ON RIGHT(s.METADATA$FILENAME, LENGTH(f.RELATIVE_PATH)) = f.RELATIVE_PATH
  WHERE TRY_TO_NUMBER(s.$1:time::TEXT) IS NOT NULL
    AND NULLIF(s.$1:value::FLOAT, 0) IS NOT NULL;

  RETURN OBJECT_CONSTRUCT('status','OK','changed',v_changed_cnt);
END;
$$;

-- =============================================================================
-- 4) TASKS (2 per STAGE: refresh + apply)
-- =============================================================================
USE SCHEMA TASKS;

-- Choose one schedule for all refresh tasks (UTC). Example: 02:00 UTC daily
-- -----------------------------------------------------------------------------
-- OSM: refresh + apply
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK TASKS.T_OSM_STAGE_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 2 * * * UTC'
AS
  ALTER STAGE TASKS.OSM_RAW_STAGE REFRESH;

CREATE OR REPLACE TASK TASKS.T_OSM_APPLY_CHANGES
  WAREHOUSE = COMPUTE_WH
  AFTER TASKS.T_OSM_STAGE_REFRESH
  WHEN SYSTEM$STREAM_HAS_DATA('TASKS.OSM_RAW_STAGE_DIR_S')
AS
  CALL TASKS.APPLY_OSM_ALL_STAGE_CHANGES();

-- -----------------------------------------------------------------------------
-- EUROSTAT: refresh + apply
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK TASKS.T_EUROSTAT_STAGE_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 5 2 * * * UTC'
AS
  ALTER STAGE TASKS.EUROSTAT_RAW_STAGE REFRESH;

CREATE OR REPLACE TASK TASKS.T_EUROSTAT_APPLY_CHANGES
  WAREHOUSE = COMPUTE_WH
  AFTER TASKS.T_EUROSTAT_STAGE_REFRESH
  WHEN SYSTEM$STREAM_HAS_DATA('TASKS.EUROSTAT_RAW_STAGE_DIR_S')
AS
  CALL TASKS.APPLY_EUROSTAT_ALL_STAGE_CHANGES();

-- -----------------------------------------------------------------------------
-- GISCO: refresh + apply
-- -----------------------------------------------------------------------------
CREATE OR REPLACE TASK TASKS.T_GISCO_STAGE_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 3 * * * UTC'
AS
  ALTER STAGE TASKS.GISCO_RAW_STAGE REFRESH;

CREATE OR REPLACE TASK TASKS.T_GISCO_APPLY_CHANGES
  WAREHOUSE = COMPUTE_WH
  AFTER TASKS.T_GISCO_STAGE_REFRESH
  WHEN SYSTEM$STREAM_HAS_DATA('TASKS.GISCO_RAW_STAGE_DIR_S')
AS
  CALL TASKS.APPLY_GISCO_NUTS_STAGE_CHANGES();

-- -----------------------------------------------------------------------------
-- Enable tasks (run once when ready)
-- -----------------------------------------------------------------------------
ALTER TASK TASKS.T_OSM_APPLY_CHANGES RESUME;
ALTER TASK TASKS.T_OSM_STAGE_REFRESH RESUME;

ALTER TASK TASKS.T_EUROSTAT_APPLY_CHANGES RESUME;
ALTER TASK TASKS.T_EUROSTAT_STAGE_REFRESH RESUME;

ALTER TASK TASKS.T_GISCO_APPLY_CHANGES RESUME;
ALTER TASK TASKS.T_GISCO_STAGE_REFRESH RESUME;

-- =============================================================================
-- 5) OPTIONAL: manual one-shot run
-- =============================================================================
-- ALTER STAGE TASKS.OSM_RAW_STAGE REFRESH;
-- CALL TASKS.APPLY_OSM_ALL_STAGE_CHANGES();




-- ALTER STAGE TASKS.EUROSTAT_RAW_STAGE REFRESH;
-- CALL TASKS.APPLY_EUROSTAT_ALL_STAGE_CHANGES();

-- ALTER STAGE TASKS.GISCO_RAW_STAGE REFRESH;
-- CALL TASKS.APPLY_GISCO_NUTS_STAGE_CHANGES();