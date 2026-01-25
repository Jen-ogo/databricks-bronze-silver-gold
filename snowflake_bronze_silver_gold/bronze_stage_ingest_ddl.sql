/* =============================================================================
   GEO_PROJECT — Canonical bootstrap + ingest (OSM + EUROSTAT + GISCO)

   Goal
   ----
   Provide an idempotent Snowflake setup that ingests Parquet data from ADLS Gen2
   into BRONZE tables using:
     - External stages (separate roots for OSM, EUROSTAT, GISCO)
     - Stage DIRECTORY + DIRECTORY STREAM for change detection
     - One APPLY procedure per BRONZE table (idempotent-by-file reload)
     - Two tasks per stage: (1) REFRESH directory, (2) APPLY

   Design notes
   ------------
   - OSM, EUROSTAT, GISCO are separated at the stage level.
   - APPLY procedures are "file-snapshot" style:
       * REMOVE events => delete rows by SOURCE_FILE
       * INSERT/changed files => delete rows for those SOURCE_FILE, then reload from stage
   - Procedures implement a safe fallback:
       * If stage stream is empty but the target table is empty -> initial load from DIRECTORY()
       * Otherwise -> NOOP when no relevant changes
   - Geometry is stored as GEOM_WKT (string) in BRONZE for debug + validation.
   - Storage optimizations:
       * CLUSTER BY chosen for common filters (country/region/year/snapshot)
       * Optional SEARCH OPTIMIZATION is shown (commented) for selective predicates

   IMPORTANT (tasks):
   ------------------
   APPLY tasks are NOT guarded by WHEN SYSTEM$STREAM_HAS_DATA(...), because that would
   prevent initial-load fallback (stream empty + target empty). Procedures themselves
   NOOP safely when nothing to do.
   ============================================================================= */


/* =============================================================================
   0) CONTEXT
   ============================================================================= */

USE ROLE ACCOUNTADMIN;
USE WAREHOUSE COMPUTE_WH;

CREATE DATABASE IF NOT EXISTS GEO_PROJECT;
USE DATABASE GEO_PROJECT;

CREATE SCHEMA IF NOT EXISTS BRONZE;
CREATE SCHEMA IF NOT EXISTS TASKS;
CREATE SCHEMA IF NOT EXISTS SILVER;
CREATE SCHEMA IF NOT EXISTS GOLD;


/* =============================================================================
   1) STORAGE INTEGRATION + FILE FORMAT + STAGES + DIRECTORY + STREAMS
   ============================================================================= */

USE SCHEMA TASKS;

/* -----------------------------------------------------------------------------
   1.1 STORAGE INTEGRATION (Azure ADLS Gen2)
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE STORAGE INTEGRATION ADLS_GEO_INT
  TYPE = EXTERNAL_STAGE
  STORAGE_PROVIDER = AZURE
  ENABLED = TRUE
  AZURE_TENANT_ID = '3600a0f2-48df-4c6f-9503-7eded884a513'
  STORAGE_ALLOWED_LOCATIONS = (
    'azure://stgeodbxuc.blob.core.windows.net/uc-root'
  );

DESC STORAGE INTEGRATION ADLS_GEO_INT;

/* -----------------------------------------------------------------------------
   1.2 CANONICAL PARQUET FILE FORMAT
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE FILE FORMAT TASKS.FF_PARQUET
  TYPE = PARQUET
  COMPRESSION = AUTO;

/* -----------------------------------------------------------------------------
   1.3 STAGES
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE STAGE TASKS.GEO_ADLS_STAGE
  URL = 'azure://stgeodbxuc.blob.core.windows.net/uc-root'
  STORAGE INTEGRATION = ADLS_GEO_INT
  FILE_FORMAT = TASKS.FF_PARQUET;

CREATE OR REPLACE STAGE TASKS.OSM_RAW_STAGE
  URL = 'azure://stgeodbxuc.blob.core.windows.net/uc-root/raw/osm/'
  STORAGE_INTEGRATION = ADLS_GEO_INT
  FILE_FORMAT = TASKS.FF_PARQUET;

CREATE OR REPLACE STAGE TASKS.EUROSTAT_RAW_STAGE
  URL = 'azure://stgeodbxuc.blob.core.windows.net/uc-root/raw/eurostat/'
  STORAGE_INTEGRATION = ADLS_GEO_INT
  FILE_FORMAT = TASKS.FF_PARQUET;

CREATE OR REPLACE STAGE TASKS.GISCO_RAW_STAGE
  URL = 'azure://stgeodbxuc.blob.core.windows.net/uc-root/raw/gisco/'
  STORAGE_INTEGRATION = ADLS_GEO_INT
  FILE_FORMAT = TASKS.FF_PARQUET;

/* -----------------------------------------------------------------------------
   1.4 DIRECTORY + DIRECTORY STREAMS
   ---------------------------------------------------------------------------- */

ALTER STAGE TASKS.OSM_RAW_STAGE
  SET DIRECTORY = ( ENABLE = TRUE AUTO_REFRESH = FALSE );

ALTER STAGE TASKS.EUROSTAT_RAW_STAGE
  SET DIRECTORY = ( ENABLE = TRUE AUTO_REFRESH = FALSE );

ALTER STAGE TASKS.GISCO_RAW_STAGE
  SET DIRECTORY = ( ENABLE = TRUE AUTO_REFRESH = FALSE );

CREATE OR REPLACE STREAM TASKS.OSM_RAW_STAGE_DIR_S
  ON STAGE TASKS.OSM_RAW_STAGE;

CREATE OR REPLACE STREAM TASKS.EUROSTAT_RAW_STAGE_DIR_S
  ON STAGE TASKS.EUROSTAT_RAW_STAGE;

CREATE OR REPLACE STREAM TASKS.GISCO_RAW_STAGE_DIR_S
  ON STAGE TASKS.GISCO_RAW_STAGE;


/* =============================================================================
   2) BRONZE TABLES
   ============================================================================= */

USE SCHEMA BRONZE;

/* -----------------------------------------------------------------------------
   2.1 GISCO — NUTS
   ---------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS BRONZE.GISCO_NUTS (
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
CLUSTER BY (YEAR, LEVEL, CNTR_CODE);

/* -----------------------------------------------------------------------------
   2.2 EUROSTAT — LAU / DEGURBA
   ---------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS BRONZE.EUROSTAT_LAU_DEGURBA (
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
CLUSTER BY (CNTR_CODE, YEAR, LAU_ID);

/* -----------------------------------------------------------------------------
   2.3 EUROSTAT — Census Grid 2021 Europe
   ---------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS BRONZE.CENSUS_GRID_2021_EUROPE (
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

/* -----------------------------------------------------------------------------
   2.4 EUROSTAT — tran_r_elvehst (tidy snapshot parquet)
   ---------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS BRONZE.EUROSTAT_TRAN_R_ELVEHST (
  SOURCE_FILE   STRING,
  SNAPSHOT      STRING,

  DATASET       STRING,
  FREQ          STRING,
  VEHICLE       STRING,
  UNIT          STRING,
  GEO           STRING,
  YEAR          NUMBER(38,0),

  VALUE         FLOAT,

  INGEST_TS_RAW NUMBER(38,0),
  INGEST_TS     TIMESTAMP_NTZ,

  LOAD_TS       TIMESTAMP_NTZ
)
CLUSTER BY (SNAPSHOT, GEO, YEAR, VEHICLE);

/* -----------------------------------------------------------------------------
   2.5 OSM — BRONZE TABLES
   ---------------------------------------------------------------------------- */

CREATE TABLE IF NOT EXISTS BRONZE.ADMIN (
  OSM_ID      STRING,
  NAME        STRING,
  ADMIN_LEVEL INTEGER,
  BOUNDARY    STRING,
  TYPE        STRING,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  REGION      STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (REGION, ADMIN_LEVEL);

CREATE TABLE IF NOT EXISTS BRONZE.ROADS (
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

  REGION      STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (REGION, HIGHWAY);

CREATE TABLE IF NOT EXISTS BRONZE.CHARGING (
  OSM_ID      STRING,
  NAME        STRING,
  REF         STRING,
  OTHER_TAGS  STRING,
  GEOM_WKT    STRING,

  REGION      STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (REGION);

CREATE TABLE IF NOT EXISTS BRONZE.POI_POINTS (
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

  REGION      STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (REGION, PLACE);

CREATE TABLE IF NOT EXISTS BRONZE.POI_POLYGONS (
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

  REGION      STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (REGION, AMENITY);

CREATE TABLE IF NOT EXISTS BRONZE.PT_POINTS (
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

  REGION      STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (REGION);

CREATE TABLE IF NOT EXISTS BRONZE.PT_LINES (
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

  REGION      STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (REGION);

CREATE TABLE IF NOT EXISTS BRONZE.BUILDINGS_ACTIVITY (
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

  REGION      STRING,
  SOURCE_FILE STRING,
  LOAD_TS     TIMESTAMP_NTZ
)
CLUSTER BY (REGION);

/* -----------------------------------------------------------------------------
   Optional: Search Optimization (cost trade-off)
   ----------------------------------------------------------------------------
   ALTER TABLE BRONZE.CHARGING ADD SEARCH OPTIMIZATION ON EQUALITY(REGION);
   ALTER TABLE BRONZE.EUROSTAT_TRAN_R_ELVEHST ADD SEARCH OPTIMIZATION ON EQUALITY(GEO, VEHICLE, SNAPSHOT);
*/


/* =============================================================================
   3) APPLY PROCEDURES (one per table)
   ============================================================================= */

USE SCHEMA TASKS;

/* -----------------------------------------------------------------------------
   Helper notes for path parsing (OSM)
   - RELATIVE_PATH is relative to stage root ".../raw/osm/"
   - REGION parsed as "<country>/<region>" from first two segments of RELATIVE_PATH
   ---------------------------------------------------------------------------- */


/* -----------------------------------------------------------------------------
   3.1 GISCO — APPLY_GISCO_NUTS_STAGE_CHANGES
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_GISCO_NUTS_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_changed_cnt NUMBER DEFAULT 0;
  v_files_cnt   NUMBER DEFAULT 0;
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

  SELECT COUNT(*) INTO :v_changed_cnt FROM _CHANGED;

  IF (v_changed_cnt = 0) THEN
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
      RETURN 'NOOP: no GISCO NUTS changes in stage stream';
    END IF;
  ELSE
    CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
    SELECT RELATIVE_PATH
    FROM _CHANGED
    WHERE ACTION='INSERT' AND SIZE > 0;

    IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS) = 0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.GISCO_RAW_STAGE)
      WHERE REGEXP_LIKE(
        RELATIVE_PATH,
        '^nuts/year=[0-9]{4}/scale=[^/]+/crs=[0-9]+/level=[0-9]+/.*[.]parquet$'
      )
        AND COALESCE(SIZE,0) > 0;
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt = 0) THEN
    RETURN 'NOOP: GISCO NUTS pattern matched 0 files';
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
    ON REGEXP_SUBSTR(s.METADATA$FILENAME, '(nuts/.*[.]parquet)$') = f.RELATIVE_PATH;

  RETURN 'OK: GISCO NUTS applied into BRONZE.GISCO_NUTS';
END;
$$;


/* -----------------------------------------------------------------------------
   3.2 EUROSTAT — APPLY_EUROSTAT_DEGURBA_STAGE_CHANGES
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_EUROSTAT_DEGURBA_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_changed_cnt NUMBER DEFAULT 0;
  v_files_cnt   NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING    AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING  AS ACTION
  FROM TASKS.EUROSTAT_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '^degurba/lau/year=[0-9]{4}/.*_sf[.]parquet$');

  SELECT COUNT(*) INTO :v_changed_cnt FROM _CHANGED;

  IF (v_changed_cnt = 0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.EUROSTAT_LAU_DEGURBA) = 0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.EUROSTAT_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '^degurba/lau/year=[0-9]{4}/.*_sf[.]parquet$')
        AND COALESCE(SIZE,0) > 0;
    ELSE
      RETURN 'NOOP: no DEGURBA changes in EUROSTAT stage stream';
    END IF;
  ELSE
    CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
    SELECT RELATIVE_PATH
    FROM _CHANGED
    WHERE ACTION='INSERT' AND SIZE > 0;

    IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS) = 0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.EUROSTAT_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '^degurba/lau/year=[0-9]{4}/.*_sf[.]parquet$')
        AND COALESCE(SIZE,0) > 0;
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt = 0) THEN
    RETURN 'NOOP: DEGURBA pattern matched 0 files';
  END IF;

  DELETE FROM BRONZE.EUROSTAT_LAU_DEGURBA t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.EUROSTAT_LAU_DEGURBA t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.EUROSTAT_LAU_DEGURBA (
    GISCO_ID, CNTR_CODE, LAU_ID, LAU_NAME, DGURBA, FID,
    GEOM_WKT, YEAR, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:"GISCO_ID"::STRING,
    s.$1:"CNTR_CODE"::STRING,
    s.$1:"LAU_ID"::STRING,
    s.$1:"LAU_NAME"::STRING,
    s.$1:"DGURBA"::NUMBER(38,0),
    s.$1:"FID"::NUMBER(38,0),
    s.$1:"geom_wkt"::STRING AS GEOM_WKT,

    REGEXP_SUBSTR(f.RELATIVE_PATH, 'year=([0-9]{4})', 1, 1, 'e', 1)::STRING AS YEAR,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.EUROSTAT_RAW_STAGE (PATTERN => '.*_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON REGEXP_SUBSTR(s.METADATA$FILENAME, '(degurba/.*_sf[.]parquet)$') = f.RELATIVE_PATH;

  RETURN 'OK: Eurostat DEGURBA applied into BRONZE.EUROSTAT_LAU_DEGURBA';
END;
$$;


/* -----------------------------------------------------------------------------
   3.3 EUROSTAT — APPLY_EUROSTAT_TRAN_R_ELVEHST_STAGE_CHANGES
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_EUROSTAT_TRAN_R_ELVEHST_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_changed_cnt NUMBER DEFAULT 0;
  v_files_cnt   NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING    AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING  AS ACTION
  FROM TASKS.EUROSTAT_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(
    RELATIVE_PATH,
    '^tran_r_elvehst/snapshot=[^/]+/tran_r_elvehst_tidy[.]parquet$'
  );

  SELECT COUNT(*) INTO :v_changed_cnt FROM _CHANGED;

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT' AND SIZE > 0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS) = 0) THEN
    CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
    SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
    FROM DIRECTORY(@TASKS.EUROSTAT_RAW_STAGE)
    WHERE REGEXP_LIKE(
      RELATIVE_PATH,
      '^tran_r_elvehst/snapshot=[^/]+/tran_r_elvehst_tidy[.]parquet$'
    )
      AND COALESCE(SIZE,0) > 0;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt = 0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.EUROSTAT_TRAN_R_ELVEHST) = 0) THEN
      RETURN 'NOOP: tran_r_elvehst pattern matched 0 files (empty stage?)';
    ELSE
      RETURN 'NOOP: no tran_r_elvehst files to process';
    END IF;
  END IF;

  DELETE FROM BRONZE.EUROSTAT_TRAN_R_ELVEHST t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.EUROSTAT_TRAN_R_ELVEHST t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.EUROSTAT_TRAN_R_ELVEHST (
    SOURCE_FILE,
    SNAPSHOT,
    DATASET,
    FREQ,
    VEHICLE,
    UNIT,
    GEO,
    YEAR,
    VALUE,
    INGEST_TS_RAW,
    INGEST_TS,
    LOAD_TS
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

    NULLIF(s.$1:value::FLOAT, 0)  AS VALUE,

    s.$1:ingest_ts::NUMBER(38,0)  AS INGEST_TS_RAW,
    TO_TIMESTAMP_NTZ(s.$1:ingest_ts::NUMBER(38,0), 9) AS INGEST_TS,

    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.EUROSTAT_RAW_STAGE
       (FILE_FORMAT => TASKS.FF_PARQUET,
        PATTERN => '.*tran_r_elvehst/snapshot=[^/]+/tran_r_elvehst_tidy[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON REGEXP_SUBSTR(s.METADATA$FILENAME, '(tran_r_elvehst/.*tran_r_elvehst_tidy[.]parquet)$') = f.RELATIVE_PATH
  WHERE TRY_TO_NUMBER(s.$1:time::TEXT) IS NOT NULL
    AND NULLIF(s.$1:value::FLOAT, 0) IS NOT NULL;

  RETURN 'OK: Eurostat tran_r_elvehst applied into BRONZE.EUROSTAT_TRAN_R_ELVEHST';
END;
$$;


/* -----------------------------------------------------------------------------
   3.4 EUROSTAT — APPLY_EUROSTAT_CENSUS_GRID_2021_STAGE_CHANGES
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_EUROSTAT_CENSUS_GRID_2021_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  v_changed_cnt NUMBER DEFAULT 0;
  v_files_cnt   NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING    AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING  AS ACTION
  FROM TASKS.EUROSTAT_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(
    RELATIVE_PATH,
    '^population_grid/europe/census_grid_2021/.*_sf[.]parquet$'
  );

  SELECT COUNT(*) INTO :v_changed_cnt FROM _CHANGED;

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH
  FROM _CHANGED
  WHERE ACTION='INSERT' AND SIZE > 0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS) = 0) THEN
    CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
    SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
    FROM DIRECTORY(@TASKS.EUROSTAT_RAW_STAGE)
    WHERE REGEXP_LIKE(
      RELATIVE_PATH,
      '^population_grid/europe/census_grid_2021/.*_sf[.]parquet$'
    )
      AND COALESCE(SIZE,0) > 0;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt = 0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.CENSUS_GRID_2021_EUROPE) = 0) THEN
      RETURN 'NOOP: census_grid_2021 pattern matched 0 files (empty stage?)';
    ELSE
      RETURN 'NOOP: no census_grid_2021 files to process';
    END IF;
  END IF;

  DELETE FROM BRONZE.CENSUS_GRID_2021_EUROPE t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.CENSUS_GRID_2021_EUROPE t
  USING _FILES_TO_PROCESS f
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

    s.$1:geom_wkt::STRING AS GEOM_WKT,

    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.EUROSTAT_RAW_STAGE (PATTERN => '.*population_grid/europe/census_grid_2021/.*_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON REGEXP_SUBSTR(s.METADATA$FILENAME, '(population_grid/.*_sf[.]parquet)$') = f.RELATIVE_PATH;

  RETURN 'OK: Eurostat census_grid_2021 applied into BRONZE.CENSUS_GRID_2021_EUROPE';
END;
$$;


/* -----------------------------------------------------------------------------
   3.5 OSM — APPLY procedures (ADMIN / ROADS / CHARGING / POI / PT / BUILDINGS)
   NOTE:
   - Join is s.METADATA$FILENAME = f.RELATIVE_PATH (both relative to OSM stage root)
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_ADMIN_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE v_files_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)admin_sf[.]parquet$');

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH FROM _CHANGED WHERE ACTION='INSERT' AND SIZE>0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS)=0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.ADMIN)=0
        OR (SELECT COUNT(*) FROM _CHANGED WHERE ACTION IN ('REMOVE','DELETE'))>0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.OSM_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)admin_sf[.]parquet$')
        AND COALESCE(SIZE,0)>0;
    ELSE
      RETURN 'NOOP: no OSM ADMIN changes in stage stream';
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt=0) THEN RETURN 'NOOP: OSM ADMIN pattern matched 0 files'; END IF;

  DELETE FROM BRONZE.ADMIN t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.ADMIN t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.ADMIN (
    OSM_ID, NAME, ADMIN_LEVEL, BOUNDARY, TYPE, OTHER_TAGS, GEOM_WKT,
    REGION, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:name::STRING,
    s.$1:admin_level::INTEGER,
    s.$1:boundary::STRING,
    s.$1:type::STRING,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,
    REGEXP_SUBSTR(f.RELATIVE_PATH, '^([^/]+/[^/]+)', 1, 1, 'e', 1)::STRING AS REGION,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*admin_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON s.METADATA$FILENAME = f.RELATIVE_PATH;

  RETURN 'OK: OSM ADMIN applied into BRONZE.ADMIN';
END;
$$;


CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_ROADS_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE v_files_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)roads_sf[.]parquet$');

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH FROM _CHANGED WHERE ACTION='INSERT' AND SIZE>0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS)=0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.ROADS)=0
        OR (SELECT COUNT(*) FROM _CHANGED WHERE ACTION IN ('REMOVE','DELETE'))>0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.OSM_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)roads_sf[.]parquet$')
        AND COALESCE(SIZE,0)>0;
    ELSE
      RETURN 'NOOP: no OSM ROADS changes in stage stream';
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt=0) THEN RETURN 'NOOP: OSM ROADS pattern matched 0 files'; END IF;

  DELETE FROM BRONZE.ROADS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.ROADS t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.ROADS (
    OSM_ID, NAME, HIGHWAY, BARRIER, MAN_MADE, RAILWAY, WATERWAY, Z_ORDER,
    OTHER_TAGS, GEOM_WKT, REGION, SOURCE_FILE, LOAD_TS
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
    REGEXP_SUBSTR(f.RELATIVE_PATH, '^([^/]+/[^/]+)', 1, 1, 'e', 1)::STRING AS REGION,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*roads_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON s.METADATA$FILENAME = f.RELATIVE_PATH;

  RETURN 'OK: OSM ROADS applied into BRONZE.ROADS';
END;
$$;


CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_CHARGING_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE v_files_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)charging_sf[.]parquet$');

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH FROM _CHANGED WHERE ACTION='INSERT' AND SIZE>0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS)=0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.CHARGING)=0
        OR (SELECT COUNT(*) FROM _CHANGED WHERE ACTION IN ('REMOVE','DELETE'))>0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.OSM_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)charging_sf[.]parquet$')
        AND COALESCE(SIZE,0)>0;
    ELSE
      RETURN 'NOOP: no OSM CHARGING changes in stage stream';
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt=0) THEN RETURN 'NOOP: OSM CHARGING pattern matched 0 files'; END IF;

  DELETE FROM BRONZE.CHARGING t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.CHARGING t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.CHARGING (
    OSM_ID, NAME, REF, OTHER_TAGS, GEOM_WKT, REGION, SOURCE_FILE, LOAD_TS
  )
  SELECT
    s.$1:osm_id::STRING,
    s.$1:name::STRING,
    s.$1:ref::STRING,
    s.$1:other_tags::STRING,
    s.$1:geom_wkt::STRING,
    REGEXP_SUBSTR(f.RELATIVE_PATH, '^([^/]+/[^/]+)', 1, 1, 'e', 1)::STRING AS REGION,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*charging_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON s.METADATA$FILENAME = f.RELATIVE_PATH;

  RETURN 'OK: OSM CHARGING applied into BRONZE.CHARGING';
END;
$$;


/* -----------------------------------------------------------------------------
   3.6 OSM — POI_POINTS
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_POI_POINTS_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE v_files_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)poi_points_sf[.]parquet$');

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH FROM _CHANGED WHERE ACTION='INSERT' AND SIZE>0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS)=0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.POI_POINTS)=0
        OR (SELECT COUNT(*) FROM _CHANGED WHERE ACTION IN ('REMOVE','DELETE'))>0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.OSM_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)poi_points_sf[.]parquet$')
        AND COALESCE(SIZE,0)>0;
    ELSE
      RETURN 'NOOP: no OSM POI_POINTS changes in stage stream';
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt=0) THEN RETURN 'NOOP: OSM POI_POINTS pattern matched 0 files'; END IF;

  DELETE FROM BRONZE.POI_POINTS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.POI_POINTS t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.POI_POINTS (
    OSM_ID, NAME, BARRIER, HIGHWAY, IS_IN, REF, MAN_MADE, PLACE, OTHER_TAGS, GEOM_WKT,
    REGION, SOURCE_FILE, LOAD_TS
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
    REGEXP_SUBSTR(f.RELATIVE_PATH, '^([^/]+/[^/]+)', 1, 1, 'e', 1)::STRING AS REGION,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*poi_points_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON s.METADATA$FILENAME = f.RELATIVE_PATH;

  RETURN 'OK: OSM POI_POINTS applied into BRONZE.POI_POINTS';
END;
$$;


/* -----------------------------------------------------------------------------
   3.7 OSM — POI_POLYGONS
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_POI_POLYGONS_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE v_files_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)poi_polygons_sf[.]parquet$');

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH FROM _CHANGED WHERE ACTION='INSERT' AND SIZE>0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS)=0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.POI_POLYGONS)=0
        OR (SELECT COUNT(*) FROM _CHANGED WHERE ACTION IN ('REMOVE','DELETE'))>0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.OSM_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)poi_polygons_sf[.]parquet$')
        AND COALESCE(SIZE,0)>0;
    ELSE
      RETURN 'NOOP: no OSM POI_POLYGONS changes in stage stream';
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt=0) THEN RETURN 'NOOP: OSM POI_POLYGONS pattern matched 0 files'; END IF;

  DELETE FROM BRONZE.POI_POLYGONS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.POI_POLYGONS t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.POI_POLYGONS (
    OSM_ID, OSM_WAY_ID, NAME, TYPE,
    AEROWAY, AMENITY, ADMIN_LEVEL, BARRIER, BOUNDARY, BUILDING, CRAFT, HISTORIC,
    LANDUSE, LEISURE, MAN_MADE, MILITARY, NATURAL, OFFICE, PLACE, SHOP, SPORT, TOURISM,
    OTHER_TAGS, GEOM_WKT,
    REGION, SOURCE_FILE, LOAD_TS
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

    REGEXP_SUBSTR(f.RELATIVE_PATH, '^([^/]+/[^/]+)', 1, 1, 'e', 1)::STRING AS REGION,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*poi_polygons_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON s.METADATA$FILENAME = f.RELATIVE_PATH;

  RETURN 'OK: OSM POI_POLYGONS applied into BRONZE.POI_POLYGONS';
END;
$$;


/* -----------------------------------------------------------------------------
   3.8 OSM — PT_POINTS
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_PT_POINTS_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE v_files_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)pt_points_sf[.]parquet$');

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH FROM _CHANGED WHERE ACTION='INSERT' AND SIZE>0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS)=0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.PT_POINTS)=0
        OR (SELECT COUNT(*) FROM _CHANGED WHERE ACTION IN ('REMOVE','DELETE'))>0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.OSM_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)pt_points_sf[.]parquet$')
        AND COALESCE(SIZE,0)>0;
    ELSE
      RETURN 'NOOP: no OSM PT_POINTS changes in stage stream';
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt=0) THEN RETURN 'NOOP: OSM PT_POINTS pattern matched 0 files'; END IF;

  DELETE FROM BRONZE.PT_POINTS t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.PT_POINTS t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.PT_POINTS (
    OSM_ID, NAME, BARRIER, HIGHWAY, IS_IN, REF, MAN_MADE, PLACE, OTHER_TAGS, GEOM_WKT,
    REGION, SOURCE_FILE, LOAD_TS
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
    REGEXP_SUBSTR(f.RELATIVE_PATH, '^([^/]+/[^/]+)', 1, 1, 'e', 1)::STRING AS REGION,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*pt_points_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON s.METADATA$FILENAME = f.RELATIVE_PATH;

  RETURN 'OK: OSM PT_POINTS applied into BRONZE.PT_POINTS';
END;
$$;


/* -----------------------------------------------------------------------------
   3.9 OSM — PT_LINES
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_PT_LINES_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE v_files_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)pt_lines_sf[.]parquet$');

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH FROM _CHANGED WHERE ACTION='INSERT' AND SIZE>0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS)=0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.PT_LINES)=0
        OR (SELECT COUNT(*) FROM _CHANGED WHERE ACTION IN ('REMOVE','DELETE'))>0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.OSM_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)pt_lines_sf[.]parquet$')
        AND COALESCE(SIZE,0)>0;
    ELSE
      RETURN 'NOOP: no OSM PT_LINES changes in stage stream';
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt=0) THEN RETURN 'NOOP: OSM PT_LINES pattern matched 0 files'; END IF;

  DELETE FROM BRONZE.PT_LINES t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.PT_LINES t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.PT_LINES (
    OSM_ID, NAME, AERIALWAY, HIGHWAY, WATERWAY, BARRIER, MAN_MADE, RAILWAY, Z_ORDER,
    OTHER_TAGS, GEOM_WKT, REGION, SOURCE_FILE, LOAD_TS
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
    REGEXP_SUBSTR(f.RELATIVE_PATH, '^([^/]+/[^/]+)', 1, 1, 'e', 1)::STRING AS REGION,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*pt_lines_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON s.METADATA$FILENAME = f.RELATIVE_PATH;

  RETURN 'OK: OSM PT_LINES applied into BRONZE.PT_LINES';
END;
$$;


/* -----------------------------------------------------------------------------
   3.10 OSM — BUILDINGS_ACTIVITY
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE PROCEDURE TASKS.APPLY_OSM_BUILDINGS_ACTIVITY_STAGE_CHANGES()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE v_files_cnt NUMBER DEFAULT 0;
BEGIN
  CREATE OR REPLACE TEMP TABLE _CHANGED AS
  SELECT
    RELATIVE_PATH::STRING AS RELATIVE_PATH,
    COALESCE(SIZE,0)::NUMBER AS SIZE,
    METADATA$ACTION::STRING AS ACTION
  FROM TASKS.OSM_RAW_STAGE_DIR_S
  WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)buildings_activity_sf[.]parquet$');

  CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
  SELECT RELATIVE_PATH FROM _CHANGED WHERE ACTION='INSERT' AND SIZE>0;

  IF ((SELECT COUNT(*) FROM _FILES_TO_PROCESS)=0) THEN
    IF ((SELECT COUNT(*) FROM BRONZE.BUILDINGS_ACTIVITY)=0
        OR (SELECT COUNT(*) FROM _CHANGED WHERE ACTION IN ('REMOVE','DELETE'))>0) THEN
      CREATE OR REPLACE TEMP TABLE _FILES_TO_PROCESS AS
      SELECT RELATIVE_PATH::STRING AS RELATIVE_PATH
      FROM DIRECTORY(@TASKS.OSM_RAW_STAGE)
      WHERE REGEXP_LIKE(RELATIVE_PATH, '(^|.*/)buildings_activity_sf[.]parquet$')
        AND COALESCE(SIZE,0)>0;
    ELSE
      RETURN 'NOOP: no OSM BUILDINGS_ACTIVITY changes in stage stream';
    END IF;
  END IF;

  SELECT COUNT(*) INTO :v_files_cnt FROM _FILES_TO_PROCESS;
  IF (v_files_cnt=0) THEN RETURN 'NOOP: OSM BUILDINGS_ACTIVITY pattern matched 0 files'; END IF;

  DELETE FROM BRONZE.BUILDINGS_ACTIVITY t
  USING _CHANGED e
  WHERE e.ACTION IN ('REMOVE','DELETE')
    AND t.SOURCE_FILE = e.RELATIVE_PATH;

  DELETE FROM BRONZE.BUILDINGS_ACTIVITY t
  USING _FILES_TO_PROCESS f
  WHERE t.SOURCE_FILE = f.RELATIVE_PATH;

  INSERT INTO BRONZE.BUILDINGS_ACTIVITY (
    OSM_ID, OSM_WAY_ID, NAME, TYPE,
    AEROWAY, AMENITY, ADMIN_LEVEL, BARRIER, BOUNDARY, BUILDING, CRAFT, HISTORIC,
    LANDUSE, LEISURE, MAN_MADE, MILITARY, NATURAL, OFFICE, PLACE, SHOP, SPORT, TOURISM,
    OTHER_TAGS, GEOM_WKT,
    REGION, SOURCE_FILE, LOAD_TS
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

    REGEXP_SUBSTR(f.RELATIVE_PATH, '^([^/]+/[^/]+)', 1, 1, 'e', 1)::STRING AS REGION,
    f.RELATIVE_PATH::STRING AS SOURCE_FILE,
    CURRENT_TIMESTAMP()::TIMESTAMP_NTZ AS LOAD_TS
  FROM @TASKS.OSM_RAW_STAGE (PATTERN => '.*buildings_activity_sf[.]parquet$') s
  JOIN _FILES_TO_PROCESS f
    ON s.METADATA$FILENAME = f.RELATIVE_PATH;

  RETURN 'OK: OSM BUILDINGS_ACTIVITY applied into BRONZE.BUILDINGS_ACTIVITY';
END;
$$;


/* =============================================================================
   4) TASKS (one refresh + one apply per stage)
   ============================================================================= */

USE SCHEMA TASKS;

/* -----------------------------------------------------------------------------
   4.1 OSM stage tasks
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE TASK TASKS.T_OSM_STAGE_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 2 * * * UTC'
AS
  ALTER STAGE TASKS.OSM_RAW_STAGE REFRESH;

CREATE OR REPLACE TASK TASKS.T_OSM_APPLY_CHANGES
  WAREHOUSE = COMPUTE_WH
  AFTER TASKS.T_OSM_STAGE_REFRESH
AS
BEGIN
  CALL TASKS.APPLY_OSM_ADMIN_STAGE_CHANGES();
  CALL TASKS.APPLY_OSM_ROADS_STAGE_CHANGES();
  CALL TASKS.APPLY_OSM_CHARGING_STAGE_CHANGES();
  CALL TASKS.APPLY_OSM_POI_POINTS_STAGE_CHANGES();
  CALL TASKS.APPLY_OSM_POI_POLYGONS_STAGE_CHANGES();
  CALL TASKS.APPLY_OSM_PT_POINTS_STAGE_CHANGES();
  CALL TASKS.APPLY_OSM_PT_LINES_STAGE_CHANGES();
  CALL TASKS.APPLY_OSM_BUILDINGS_ACTIVITY_STAGE_CHANGES();
END;

/* -----------------------------------------------------------------------------
   4.2 EUROSTAT stage tasks
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE TASK TASKS.T_EUROSTAT_STAGE_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 5 2 * * * UTC'
AS
  ALTER STAGE TASKS.EUROSTAT_RAW_STAGE REFRESH;

CREATE OR REPLACE TASK TASKS.T_EUROSTAT_APPLY_CHANGES
  WAREHOUSE = COMPUTE_WH
  AFTER TASKS.T_EUROSTAT_STAGE_REFRESH
AS
BEGIN
  CALL TASKS.APPLY_EUROSTAT_DEGURBA_STAGE_CHANGES();
  CALL TASKS.APPLY_EUROSTAT_TRAN_R_ELVEHST_STAGE_CHANGES();
  CALL TASKS.APPLY_EUROSTAT_CENSUS_GRID_2021_STAGE_CHANGES();
END;

/* -----------------------------------------------------------------------------
   4.3 GISCO stage tasks
   ---------------------------------------------------------------------------- */

CREATE OR REPLACE TASK TASKS.T_GISCO_STAGE_REFRESH
  WAREHOUSE = COMPUTE_WH
  SCHEDULE  = 'USING CRON 0 3 * * * UTC'
AS
  ALTER STAGE TASKS.GISCO_RAW_STAGE REFRESH;

CREATE OR REPLACE TASK TASKS.T_GISCO_APPLY_CHANGES
  WAREHOUSE = COMPUTE_WH
  AFTER TASKS.T_GISCO_STAGE_REFRESH
AS
BEGIN
  CALL TASKS.APPLY_GISCO_NUTS_STAGE_CHANGES();
END;

/* -----------------------------------------------------------------------------
   4.4 One-time task control (admin commands)
   ----------------------------------------------------------------------------
   ALTER TASK TASKS.T_OSM_STAGE_REFRESH RESUME;
   ALTER TASK TASKS.T_OSM_APPLY_CHANGES RESUME;

   ALTER TASK TASKS.T_EUROSTAT_STAGE_REFRESH RESUME;
   ALTER TASK TASKS.T_EUROSTAT_APPLY_CHANGES RESUME;

   ALTER TASK TASKS.T_GISCO_STAGE_REFRESH RESUME;
   ALTER TASK TASKS.T_GISCO_APPLY_CHANGES RESUME;

   -- To pause:
   -- ALTER TASK ... SUSPEND;
*/


/* =============================================================================
   5) OPTIONAL VALIDATION SNIPPETS (manual checks)
   - Mandatory geo checks: rowcount + WKT presence + WKT samples
   ============================================================================= */

-- Stream status (debug)
-- SELECT SYSTEM$STREAM_HAS_DATA('TASKS.OSM_RAW_STAGE_DIR_S')      AS osm_has_data;
-- SELECT SYSTEM$STREAM_HAS_DATA('TASKS.EUROSTAT_RAW_STAGE_DIR_S') AS eurostat_has_data;
-- SELECT SYSTEM$STREAM_HAS_DATA('TASKS.GISCO_RAW_STAGE_DIR_S')    AS gisco_has_data;

-- Rowcount checks (mandatory)
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.GISCO_NUTS;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.EUROSTAT_LAU_DEGURBA;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.CENSUS_GRID_2021_EUROPE;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.EUROSTAT_TRAN_R_ELVEHST;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.ADMIN;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.ROADS;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.CHARGING;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.POI_POINTS;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.POI_POLYGONS;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.PT_POINTS;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.PT_LINES;
-- SELECT COUNT(*) AS row_cnt FROM BRONZE.BUILDINGS_ACTIVITY;

-- WKT presence checks (mandatory for geo tables)
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.GISCO_NUTS;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.EUROSTAT_LAU_DEGURBA;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.CENSUS_GRID_2021_EUROPE;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.ADMIN;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.ROADS;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.CHARGING;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.POI_POINTS;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.POI_POLYGONS;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.PT_POINTS;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.PT_LINES;
-- SELECT COUNT_IF(GEOM_WKT IS NULL OR GEOM_WKT='') AS geom_wkt_null_cnt, COUNT(*) AS total_cnt FROM BRONZE.BUILDINGS_ACTIVITY;

-- WKT samples (mandatory)
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.GISCO_NUTS LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.EUROSTAT_LAU_DEGURBA LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.CENSUS_GRID_2021_EUROPE LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.ADMIN LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.ROADS LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.CHARGING LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.POI_POINTS LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.POI_POLYGONS LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.PT_POINTS LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.PT_LINES LIMIT 10;
-- SELECT SOURCE_FILE, LEFT(GEOM_WKT, 160) AS geom_wkt_head FROM BRONZE.BUILDINGS_ACTIVITY LIMIT 10;