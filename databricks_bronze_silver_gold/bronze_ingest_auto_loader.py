# Databricks Notebook (Python)
# =============================================================================
# Bronze ingestion (Delta) via Auto Loader
#
# What this notebook does
# - Ingests OSM datasets (Parquet with WKB geometry) from ADLS into Unity Catalog Delta tables
# - Ingests selected reference datasets (Parquet exported with WKT EPSG:4326) into Bronze tables
#
# Core semantics
# - Auto Loader discovers new files
# - foreachBatch writes are idempotent:
#     * partitioned tables: overwrite only the touched partitions using replaceWhere
#     * snapshot-style tables: delete by source_file then append
# - source_file + load_ts are always preserved for traceability
# =============================================================================

from __future__ import annotations

from typing import Callable, Iterable, List, Optional, Sequence, Tuple

from pyspark.sql import DataFrame
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType, LongType, DoubleType
from delta.tables import DeltaTable


# -----------------------------------------------------------------------------
# 0) Global settings
# -----------------------------------------------------------------------------
CATALOG = "geo_databricks_sub"
SCHEMA = "bronze"

ADLS_ACCOUNT = "stgeodbxuc"
CONTAINER = "uc-root"

ROOT_OSM = f"abfss://{CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/raw/osm"
ROOT_GISCO = f"abfss://{CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/raw/gisco"
ROOT_EURO = f"abfss://{CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/raw/eurostat"

CHECKPOINT_BASE = f"abfss://{CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/_checkpoints/autoloader"
SCHEMA_BASE = f"abfss://{CONTAINER}@{ADLS_ACCOUNT}.dfs.core.windows.net/_schemas/autoloader"


def ensure_uc_context(catalog: str, schema: str) -> None:
    spark.sql(f"USE CATALOG {catalog}")
    spark.sql(f"CREATE SCHEMA IF NOT EXISTS {schema}")
    spark.sql(f"USE SCHEMA {schema}")


ensure_uc_context(CATALOG, SCHEMA)


# -----------------------------------------------------------------------------
# 1) Generic helpers
# -----------------------------------------------------------------------------

def create_delta_table_if_missing(
    table_fqn: str,
    columns_ddl: Sequence[str],
    partition_cols: Optional[Sequence[str]] = None,
) -> None:
    part = ""
    if partition_cols:
        part = f"\nPARTITIONED BY ({', '.join(partition_cols)})"

    spark.sql(
        f"""
        CREATE TABLE IF NOT EXISTS {table_fqn} (
          {', '.join(columns_ddl)}
        )
        USING DELTA
        {part}
        """
    )


def read_autoloader_parquet(pattern: str, schema_location: str) -> DataFrame:
    return (
        spark.readStream.format("cloudFiles")
        .option("cloudFiles.format", "parquet")
        .option("cloudFiles.schemaLocation", schema_location)
        .option("cloudFiles.inferColumnTypes", "true")
        .option("cloudFiles.schemaEvolutionMode", "addNewColumns")
        # Optional optimization (requires notification setup):
        # .option("cloudFiles.useNotifications", "true")
        .load(pattern)
        .withColumn("_source_file", F.input_file_name())
    )


def _filter_eq(colname: str, value):
    if value is None:
        return F.col(colname).isNull()
    return F.col(colname) == F.lit(value)


def _replace_where_expr(colname: str, value) -> str:
    # Build a SQL expression fragment for replaceWhere.
    # Assumes partition columns are STRING/DATE/INT-like.
    if value is None:
        return f"{colname} IS NULL"

    # Spark collects DATE as datetime.date -> string via isoformat.
    if hasattr(value, "isoformat"):
        return f"{colname} = DATE('{value.isoformat()}')"

    if isinstance(value, (int, float)):
        return f"{colname} = {value}"

    safe = str(value).replace("'", "''")
    return f"{colname} = '{safe}'"


def foreach_batch_overwrite_partitions(
    microbatch_df: DataFrame,
    batch_id: int,
    target_table_fqn: str,
    partition_cols: Sequence[str],
) -> None:
    if microbatch_df.rdd.isEmpty():
        return

    parts = microbatch_df.select(*partition_cols).distinct().collect()

    for p in parts:
        filters = []
        where_parts = []

        for c in partition_cols:
            v = p[c]
            filters.append(_filter_eq(c, v))
            where_parts.append(_replace_where_expr(c, v))

        part_df = microbatch_df
        for f in filters:
            part_df = part_df.filter(f)

        replace_where = " AND ".join(where_parts)

        (
            part_df.write.format("delta")
            .mode("overwrite")
            .option("replaceWhere", replace_where)
            .saveAsTable(target_table_fqn)
        )


def foreach_batch_delete_by_source_file_then_append(
    microbatch_df: DataFrame,
    batch_id: int,
    target_table_fqn: str,
) -> None:
    if microbatch_df.rdd.isEmpty():
        return

    files = [r["source_file"] for r in microbatch_df.select("source_file").distinct().collect()]
    dt = DeltaTable.forName(spark, target_table_fqn)
    dt.delete(F.col("source_file").isin(files))

    (
        microbatch_df.write.format("delta")
        .mode("append")
        .saveAsTable(target_table_fqn)
    )


def run_stream_available_now(
    df: DataFrame,
    checkpoint_location: str,
    foreach_batch_fn: Callable[[DataFrame, int], None],
) -> None:
    (
        df.writeStream.option("checkpointLocation", checkpoint_location)
        .foreachBatch(foreach_batch_fn)
        .trigger(availableNow=True)
        .start()
        .awaitTermination()
    )


# -----------------------------------------------------------------------------
# 2) OSM (WKB) Bronze ingestion
# -----------------------------------------------------------------------------


def ingest_osm_dataset(
    dataset: str,
    target_table_fqn: str,
    column_map: Sequence[Tuple[str, str, str]],
    geometry_col: str = "geometry",
    file_name: Optional[str] = None,
) -> None:
    """
    Ingest a single OSM dataset into a partitioned Delta table.

    Source path convention:
      {ROOT_OSM}/<country>/<region>/{dataset}/dt=YYYY-MM-DD/{dataset}.parquet

    Partition columns:
      country, region, dt

    Parameters
    - column_map: list of tuples (source_col, cast_type, alias)
    - geometry_col: source binary WKB column name (commonly 'geometry')
    - file_name: override default file name (defaults to '{dataset}.parquet')
    """

    file_name = file_name or f"{dataset}.parquet"

    pattern = f"{ROOT_OSM}/*/*/{dataset}/dt=*/{file_name}"
    checkpoint = f"{CHECKPOINT_BASE}/osm_{dataset}"
    schema_loc = f"{SCHEMA_BASE}/osm_{dataset}"

    # DDL
    columns_ddl = [f"{alias} {cast_type.upper()}" for (_, cast_type, alias) in column_map]
    columns_ddl += [
        "geom_wkb BINARY",
        "country STRING",
        "region STRING",
        "dt DATE",
        "source_file STRING",
        "load_ts TIMESTAMP",
    ]

    create_delta_table_if_missing(
        table_fqn=target_table_fqn,
        columns_ddl=columns_ddl,
        partition_cols=["country", "region", "dt"],
    )

    raw = read_autoloader_parquet(pattern=pattern, schema_location=schema_loc)

    # Parse partitions from path
    path = F.col("_source_file")
    country = F.regexp_extract(path, r"/raw/osm/([^/]+)/", 1)
    region = F.regexp_extract(path, rf"/raw/osm/[^/]+/([^/]+)/{dataset}/", 1)
    dt_str = F.regexp_extract(path, r"/dt=([0-9]{4}-[0-9]{2}-[0-9]{2})/", 1)

    # Select/cast
    select_exprs = [F.col(src).cast(cast_type).alias(alias) for (src, cast_type, alias) in column_map]

    df = raw.select(
        *select_exprs,
        F.col(geometry_col).cast("binary").alias("geom_wkb"),
        country.alias("country"),
        region.alias("region"),
        F.to_date(dt_str).alias("dt"),
        F.col("_source_file").alias("source_file"),
        F.current_timestamp().alias("load_ts"),
    )

    def _writer(microbatch_df: DataFrame, batch_id: int) -> None:
        foreach_batch_overwrite_partitions(
            microbatch_df=microbatch_df,
            batch_id=batch_id,
            target_table_fqn=target_table_fqn,
            partition_cols=["country", "region", "dt"],
        )

    run_stream_available_now(df=df, checkpoint_location=checkpoint, foreach_batch_fn=_writer)


# ---- OSM datasets ----

# ADMIN boundaries
ingest_osm_dataset(
    dataset="admin",
    target_table_fqn=f"{CATALOG}.{SCHEMA}.osm_admin",
    column_map=[
        ("osm_id", "string", "osm_id"),
        ("name", "string", "name"),
        ("admin_level", "int", "admin_level"),
        ("boundary", "string", "boundary"),
        ("type", "string", "type"),
        ("other_tags", "string", "other_tags"),
    ],
    geometry_col="geometry",
    file_name="admin.parquet",
)

# ROADS
ingest_osm_dataset(
    dataset="roads",
    target_table_fqn=f"{CATALOG}.{SCHEMA}.osm_roads",
    column_map=[
        ("osm_id", "string", "osm_id"),
        ("name", "string", "name"),
        ("highway", "string", "highway"),
        ("barrier", "string", "barrier"),
        ("man_made", "string", "man_made"),
        ("railway", "string", "railway"),
        ("waterway", "string", "waterway"),
        ("z_order", "int", "z_order"),
        ("other_tags", "string", "other_tags"),
    ],
)

# CHARGING
ingest_osm_dataset(
    dataset="charging",
    target_table_fqn=f"{CATALOG}.{SCHEMA}.osm_charging",
    column_map=[
        ("osm_id", "string", "osm_id"),
        ("name", "string", "name"),
        ("ref", "string", "ref"),
        ("other_tags", "string", "other_tags"),
    ],
)

# POI points
ingest_osm_dataset(
    dataset="poi_points",
    target_table_fqn=f"{CATALOG}.{SCHEMA}.osm_poi_points",
    column_map=[
        ("osm_id", "string", "osm_id"),
        ("name", "string", "name"),
        ("barrier", "string", "barrier"),
        ("highway", "string", "highway"),
        ("is_in", "string", "is_in"),
        ("ref", "string", "ref"),
        ("man_made", "string", "man_made"),
        ("place", "string", "place"),
        ("other_tags", "string", "other_tags"),
    ],
)

# POI polygons
ingest_osm_dataset(
    dataset="poi_polygons",
    target_table_fqn=f"{CATALOG}.{SCHEMA}.osm_poi_polygons",
    column_map=[
        ("osm_id", "string", "osm_id"),
        ("osm_way_id", "string", "osm_way_id"),
        ("name", "string", "name"),
        ("type", "string", "type"),
        ("aeroway", "string", "aeroway"),
        ("amenity", "string", "amenity"),
        ("admin_level", "string", "admin_level"),
        ("barrier", "string", "barrier"),
        ("boundary", "string", "boundary"),
        ("building", "string", "building"),
        ("craft", "string", "craft"),
        ("historic", "string", "historic"),
        ("landuse", "string", "landuse"),
        ("leisure", "string", "leisure"),
        ("man_made", "string", "man_made"),
        ("military", "string", "military"),
        ("natural", "string", "natural"),
        ("office", "string", "office"),
        ("place", "string", "place"),
        ("shop", "string", "shop"),
        ("sport", "string", "sport"),
        ("tourism", "string", "tourism"),
        ("other_tags", "string", "other_tags"),
    ],
)

# PT points
ingest_osm_dataset(
    dataset="pt_points",
    target_table_fqn=f"{CATALOG}.{SCHEMA}.osm_pt_points",
    column_map=[
        ("osm_id", "string", "osm_id"),
        ("name", "string", "name"),
        ("barrier", "string", "barrier"),
        ("highway", "string", "highway"),
        ("is_in", "string", "is_in"),
        ("ref", "string", "ref"),
        ("man_made", "string", "man_made"),
        ("place", "string", "place"),
        ("other_tags", "string", "other_tags"),
    ],
)

# PT lines
ingest_osm_dataset(
    dataset="pt_lines",
    target_table_fqn=f"{CATALOG}.{SCHEMA}.osm_pt_lines",
    column_map=[
        ("osm_id", "string", "osm_id"),
        ("name", "string", "name"),
        ("aerialway", "string", "aerialway"),
        ("highway", "string", "highway"),
        ("waterway", "string", "waterway"),
        ("barrier", "string", "barrier"),
        ("man_made", "string", "man_made"),
        ("railway", "string", "railway"),
        ("z_order", "int", "z_order"),
        ("other_tags", "string", "other_tags"),
    ],
)

# Buildings / activity polygons
ingest_osm_dataset(
    dataset="buildings_activity",
    target_table_fqn=f"{CATALOG}.{SCHEMA}.osm_buildings_activity",
    column_map=[
        ("osm_id", "string", "osm_id"),
        ("osm_way_id", "string", "osm_way_id"),
        ("name", "string", "name"),
        ("type", "string", "type"),
        ("aeroway", "string", "aeroway"),
        ("amenity", "string", "amenity"),
        ("admin_level", "string", "admin_level"),
        ("barrier", "string", "barrier"),
        ("boundary", "string", "boundary"),
        ("building", "string", "building"),
        ("craft", "string", "craft"),
        ("historic", "string", "historic"),
        ("landuse", "string", "landuse"),
        ("leisure", "string", "leisure"),
        ("man_made", "string", "man_made"),
        ("military", "string", "military"),
        ("natural", "string", "natural"),
        ("office", "string", "office"),
        ("place", "string", "place"),
        ("shop", "string", "shop"),
        ("sport", "string", "sport"),
        ("tourism", "string", "tourism"),
        ("other_tags", "string", "other_tags"),
    ],
)


# -----------------------------------------------------------------------------
# 3) Reference datasets (WKT EPSG:4326) Bronze ingestion
# -----------------------------------------------------------------------------
# These sources are exported as *_sf.parquet with a 'geom_wkt' column.
# Geometry stays as text in Bronze; conversion to GEOGRAPHY happens in Silver/Gold.


def null_if_neg9999(colname: str) -> F.Column:
    return F.when(F.col(colname).cast("double") == F.lit(-9999), F.lit(None)).otherwise(F.col(colname))


# 3.1 GISCO NUTS
T_GISCO_NUTS = f"{CATALOG}.{SCHEMA}.gisco_nuts"

create_delta_table_if_missing(
    table_fqn=T_GISCO_NUTS,
    columns_ddl=[
        "nuts_id STRING",
        "cntr_code STRING",
        "name_latn STRING",
        "levl_code BIGINT",
        "geom_wkt STRING",
        "year STRING",
        "scale STRING",
        "crs STRING",
        "level BIGINT",
        "source_file STRING",
        "load_ts TIMESTAMP",
    ],
    partition_cols=["year", "scale", "crs", "level"],
)

pattern_nuts = f"{ROOT_GISCO}/nuts/year=*/scale=*/crs=*/level=*/*.parquet"
raw_nuts = read_autoloader_parquet(pattern=pattern_nuts, schema_location=f"{SCHEMA_BASE}/gisco_nuts")

sf = F.col("_source_file")
year = F.regexp_extract(sf, r"/nuts/year=([0-9]{4})/", 1)
scale = F.regexp_extract(sf, r"/scale=([^/]+)/", 1)
crs = F.regexp_extract(sf, r"/crs=([0-9]+)/", 1)
level = F.regexp_extract(sf, r"/level=([0-9]+)/", 1)

nuts_df = raw_nuts.select(
    F.col("NUTS_ID").cast("string").alias("nuts_id"),
    F.col("CNTR_CODE").cast("string").alias("cntr_code"),
    F.col("NAME_LATN").cast("string").alias("name_latn"),
    F.col("LEVL_CODE").cast("long").alias("levl_code"),
    F.col("geom_wkt").cast("string").alias("geom_wkt"),
    year.cast("string").alias("year"),
    scale.cast("string").alias("scale"),
    crs.cast("string").alias("crs"),
    level.cast("long").alias("level"),
    F.col("_source_file").alias("source_file"),
    F.current_timestamp().alias("load_ts"),
)

run_stream_available_now(
    df=nuts_df,
    checkpoint_location=f"{CHECKPOINT_BASE}/gisco_nuts",
    foreach_batch_fn=lambda mb, bid: foreach_batch_overwrite_partitions(
        microbatch_df=mb,
        batch_id=bid,
        target_table_fqn=T_GISCO_NUTS,
        partition_cols=["year", "scale", "crs", "level"],
    ),
)


# 3.2 EUROSTAT LAU DEGURBA
T_DEGURBA = f"{CATALOG}.{SCHEMA}.eurostat_lau_degurba"

create_delta_table_if_missing(
    table_fqn=T_DEGURBA,
    columns_ddl=[
        "gisco_id STRING",
        "cntr_code STRING",
        "lau_id STRING",
        "lau_name STRING",
        "degurba BIGINT",
        "fid BIGINT",
        "geom_wkt STRING",
        "year STRING",
        "source_file STRING",
        "load_ts TIMESTAMP",
    ],
    partition_cols=["year"],
)

pattern_deg = f"{ROOT_EURO}/degurba/lau/year=*/*_sf.parquet"
raw_deg = read_autoloader_parquet(pattern=pattern_deg, schema_location=f"{SCHEMA_BASE}/eurostat_lau_degurba")

sf = F.col("_source_file")
year = F.regexp_extract(sf, r"/year=([0-9]{4})/", 1)

deg_df = raw_deg.select(
    F.col("GISCO_ID").cast("string").alias("gisco_id"),
    F.col("CNTR_CODE").cast("string").alias("cntr_code"),
    F.col("LAU_ID").cast("string").alias("lau_id"),
    F.col("LAU_NAME").cast("string").alias("lau_name"),
    F.col("DGURBA").cast("long").alias("degurba"),
    F.col("FID").cast("long").alias("fid"),
    F.col("geom_wkt").cast("string").alias("geom_wkt"),
    year.cast("string").alias("year"),
    F.col("_source_file").alias("source_file"),
    F.current_timestamp().alias("load_ts"),
)

run_stream_available_now(
    df=deg_df,
    checkpoint_location=f"{CHECKPOINT_BASE}/eurostat_lau_degurba",
    foreach_batch_fn=lambda mb, bid: foreach_batch_overwrite_partitions(
        microbatch_df=mb,
        batch_id=bid,
        target_table_fqn=T_DEGURBA,
        partition_cols=["year"],
    ),
)


# 3.3 EUROSTAT Census Grid 2021 (Europe)
T_GRID = f"{CATALOG}.{SCHEMA}.census_grid_2021_europe"

create_delta_table_if_missing(
    table_fqn=T_GRID,
    columns_ddl=[
        "grd_id STRING",
        "t BIGINT",
        "m BIGINT",
        "f BIGINT",
        "y_lt15 BIGINT",
        "y_1564 BIGINT",
        "y_ge65 BIGINT",
        "emp BIGINT",
        "nat BIGINT",
        "eu_oth BIGINT",
        "oth BIGINT",
        "same BIGINT",
        "chg_in BIGINT",
        "chg_out BIGINT",
        "t_ci BIGINT",
        "m_ci BIGINT",
        "f_ci BIGINT",
        "y_lt15_ci BIGINT",
        "y_1564_ci BIGINT",
        "y_ge65_ci BIGINT",
        "emp_ci BIGINT",
        "nat_ci BIGINT",
        "eu_oth_ci BIGINT",
        "oth_ci BIGINT",
        "same_ci BIGINT",
        "chg_in_ci BIGINT",
        "chg_out_ci BIGINT",
        "land_surface DOUBLE",
        "populated BIGINT",
        "geom_wkt STRING",
        "source_file STRING",
        "load_ts TIMESTAMP",
    ],
    partition_cols=None,
)

pattern_grid = f"{ROOT_EURO}/population_grid/europe/census_grid_2021/*_sf.parquet"
raw_grid = read_autoloader_parquet(pattern=pattern_grid, schema_location=f"{SCHEMA_BASE}/census_grid_2021_europe")

grid_df = raw_grid.select(
    F.col("GRD_ID").cast("string").alias("grd_id"),
    null_if_neg9999("T").cast("long").alias("t"),
    null_if_neg9999("M").cast("long").alias("m"),
    null_if_neg9999("F").cast("long").alias("f"),
    null_if_neg9999("Y_LT15").cast("long").alias("y_lt15"),
    null_if_neg9999("Y_1564").cast("long").alias("y_1564"),
    null_if_neg9999("Y_GE65").cast("long").alias("y_ge65"),
    null_if_neg9999("EMP").cast("long").alias("emp"),
    null_if_neg9999("NAT").cast("long").alias("nat"),
    null_if_neg9999("EU_OTH").cast("long").alias("eu_oth"),
    null_if_neg9999("OTH").cast("long").alias("oth"),
    null_if_neg9999("SAME").cast("long").alias("same"),
    null_if_neg9999("CHG_IN").cast("long").alias("chg_in"),
    null_if_neg9999("CHG_OUT").cast("long").alias("chg_out"),
    null_if_neg9999("T_CI").cast("long").alias("t_ci"),
    null_if_neg9999("M_CI").cast("long").alias("m_ci"),
    null_if_neg9999("F_CI").cast("long").alias("f_ci"),
    null_if_neg9999("Y_LT15_CI").cast("long").alias("y_lt15_ci"),
    null_if_neg9999("Y_1564_CI").cast("long").alias("y_1564_ci"),
    null_if_neg9999("Y_GE65_CI").cast("long").alias("y_ge65_ci"),
    null_if_neg9999("EMP_CI").cast("long").alias("emp_ci"),
    null_if_neg9999("NAT_CI").cast("long").alias("nat_ci"),
    null_if_neg9999("EU_OTH_CI").cast("long").alias("eu_oth_ci"),
    null_if_neg9999("OTH_CI").cast("long").alias("oth_ci"),
    null_if_neg9999("SAME_CI").cast("long").alias("same_ci"),
    null_if_neg9999("CHG_IN_CI").cast("long").alias("chg_in_ci"),
    null_if_neg9999("CHG_OUT_CI").cast("long").alias("chg_out_ci"),
    null_if_neg9999("LAND_SURFACE").cast("double").alias("land_surface"),
    null_if_neg9999("POPULATED").cast("long").alias("populated"),
    F.col("geom_wkt").cast("string").alias("geom_wkt"),
    F.col("_source_file").alias("source_file"),
    F.current_timestamp().alias("load_ts"),
)

run_stream_available_now(
    df=grid_df,
    checkpoint_location=f"{CHECKPOINT_BASE}/census_grid_2021_europe",
    foreach_batch_fn=lambda mb, bid: foreach_batch_delete_by_source_file_then_append(
        microbatch_df=mb,
        batch_id=bid,
        target_table_fqn=T_GRID,
    ),
)


## 3.4 EUROSTAT tran_r_elvehst (tidy snapshot)
T_EV = f"{CATALOG}.{SCHEMA}.eurostat_tran_r_elvehst"

create_delta_table_if_missing(
    table_fqn=T_EV,
    columns_ddl=[
        "source_file STRING",
        "snapshot STRING",
        "dataset STRING",
        "freq STRING",
        "vehicle STRING",
        "unit STRING",
        "geo STRING",
        "year BIGINT",
        "value DOUBLE",
        "ingest_ts_raw BIGINT",
        "ingest_ts TIMESTAMP",
        "load_ts TIMESTAMP",
    ],
    partition_cols=["snapshot"],
)

pattern_ev = f"{ROOT_EURO}/tran_r_elvehst/snapshot=*/tran_r_elvehst_tidy.parquet"
schema_loc_ev = f"{SCHEMA_BASE}/eurostat_tran_r_elvehst"

# --- harden Auto Loader: explicit schema (safe types) ---
ev_schema = StructType([
    StructField("dataset", StringType(), True),
    StructField("freq", StringType(), True),
    StructField("vehicle", StringType(), True),
    StructField("unit", StringType(), True),
    StructField("geo", StringType(), True),

    # IMPORTANT: parquet has time as BINARY -> read as STRING
    StructField("time", StringType(), True),

    StructField("value", DoubleType(), True),

    # ingest_ts remains nanos (often LONG)
    StructField("ingest_ts", LongType(), True),
])

raw_ev = (
    spark.readStream.format("cloudFiles")
      .option("cloudFiles.format", "parquet")
      .schema(ev_schema)
      .option("cloudFiles.schemaLocation", f"{SCHEMA_BASE}/eurostat_tran_r_elvehst")
      .option("cloudFiles.schemaEvolutionMode", "none")
      .load(pattern_ev)
      .withColumn("_source_file", F.input_file_name())
)

sf = F.col("_source_file")
snapshot = F.regexp_extract(sf, r"/snapshot=([^/]+)/", 1)

# parse year robustly from time string (supports '2023', '2023-01', etc.)
year_str = F.regexp_extract(F.col("time"), r"([0-9]{4})", 1)
year = F.when(F.length(year_str) == 4, year_str.cast("long")).otherwise(F.lit(None).cast("long"))

ev_df = (
    raw_ev.select(
        F.col("_source_file").alias("source_file"),
        snapshot.alias("snapshot"),

        F.col("dataset").cast("string").alias("dataset"),
        F.col("freq").cast("string").alias("freq"),
        F.col("vehicle").cast("string").alias("vehicle"),
        F.col("unit").cast("string").alias("unit"),
        F.col("geo").cast("string").alias("geo"),

        year.alias("year"),

        F.when(F.col("value").cast("double") == F.lit(0.0), F.lit(None))
         .otherwise(F.col("value").cast("double"))
         .alias("value"),

        F.col("ingest_ts").cast("long").alias("ingest_ts_raw"),
        F.to_timestamp(F.from_unixtime((F.col("ingest_ts").cast("double") / F.lit(1e9)))).alias("ingest_ts"),

        F.current_timestamp().alias("load_ts"),
    )
    .where(F.col("year").isNotNull())
    .where(F.col("value").isNotNull())
)


# -----------------------------------------------------------------------------
# 4) Optional reconcile for file deletions
# -----------------------------------------------------------------------------
# Auto Loader does not detect file deletions. If you need delete propagation,
# run a periodic job that compares table.source_file to the current file listing.


def reconcile_deleted_files(table_fqn: str, list_glob: str, path_glob_filter: str = "*.parquet") -> None:
    current_files = (
        spark.read.format("binaryFile")
        .option("pathGlobFilter", path_glob_filter)
        .load(list_glob)
        .select(F.col("path").alias("source_file"))
        .distinct()
    )

    bronze_files = spark.table(table_fqn).select("source_file").distinct()
    missing = bronze_files.join(current_files, "source_file", "left_anti")

    missing.createOrReplaceTempView("_missing_files")
    spark.sql(f"DELETE FROM {table_fqn} WHERE source_file IN (SELECT source_file FROM _missing_files)")


print("OK: Bronze ingests completed (OSM datasets + GISCO/EUROSTAT reference datasets).")