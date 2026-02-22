import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, IntegerType, DoubleType, BooleanType,
    ArrayType
)

# ============================================================
# CONFIG
# ============================================================
SILVER_SRC = "geo_databricks_sub.silver.fact_tomtom_ev_events_r7"
WATERMARK = "2 hours"

# ============================================================
# SCHEMAS: parse the nested JSON strings
# ============================================================

# ---- POI Search schema (poi_json)
poi_brand_schema = StructType([StructField("name", StringType(), True)])

poi_poi_schema = StructType([
    StructField("name", StringType(), True),
    StructField("phone", StringType(), True),
    StructField("brands", ArrayType(poi_brand_schema), True),
    StructField("categories", ArrayType(StringType()), True),
])

poi_address_schema = StructType([
    StructField("streetNumber", StringType(), True),
    StructField("streetName", StringType(), True),
    StructField("municipality", StringType(), True),
    StructField("countrySubdivisionName", StringType(), True),
    StructField("postalCode", StringType(), True),
    StructField("countryCode", StringType(), True),
    StructField("freeformAddress", StringType(), True),
    StructField("localName", StringType(), True),
])

poi_position_schema = StructType([
    StructField("lat", DoubleType(), True),
    StructField("lon", DoubleType(), True),
])

poi_charging_av_schema = StructType([
    StructField("id", StringType(), True),
])

poi_data_sources_schema = StructType([
    StructField("chargingAvailability", poi_charging_av_schema, True),
])

poi_connector_schema = StructType([
    StructField("connectorType", StringType(), True),
    StructField("ratedPowerKW", DoubleType(), True),
    StructField("voltageV", DoubleType(), True),
    StructField("currentA", DoubleType(), True),
    StructField("currentType", StringType(), True),
])

poi_charging_park_schema = StructType([
    StructField("connectors", ArrayType(poi_connector_schema), True),
])

poi_result_schema = StructType([
    StructField("type", StringType(), True),
    StructField("id", StringType(), True),
    StructField("score", DoubleType(), True),
    StructField("dist", DoubleType(), True),
    StructField("poi", poi_poi_schema, True),
    StructField("address", poi_address_schema, True),
    StructField("position", poi_position_schema, True),
    StructField("chargingPark", poi_charging_park_schema, True),
    StructField("dataSources", poi_data_sources_schema, True),
])

poi_json_schema = StructType([
    StructField("results", ArrayType(poi_result_schema), True),
])

# ---- Availability snapshots schema (availability_snapshots)
avail_per_pl_schema = StructType([
    StructField("powerKW", DoubleType(), True),
    StructField("available", IntegerType(), True),
    StructField("occupied", IntegerType(), True),
    StructField("reserved", IntegerType(), True),
    StructField("unknown", IntegerType(), True),
    StructField("outOfService", IntegerType(), True),
])

avail_current_schema = StructType([
    StructField("available", IntegerType(), True),
    StructField("occupied", IntegerType(), True),
    StructField("reserved", IntegerType(), True),
    StructField("unknown", IntegerType(), True),
    StructField("outOfService", IntegerType(), True),
])

avail_availability_schema = StructType([
    StructField("current", avail_current_schema, True),
    StructField("perPowerLevel", ArrayType(avail_per_pl_schema), True),
])

avail_connector_schema = StructType([
    StructField("type", StringType(), True),
    StructField("total", IntegerType(), True),
    StructField("availability", avail_availability_schema, True),
])

avail_json_schema = StructType([
    StructField("connectors", ArrayType(avail_connector_schema), True),
    StructField("chargingAvailability", StringType(), True),
])

availability_snapshot_schema = StructType([
    StructField("chargingAvailabilityId", StringType(), True),
    StructField("ok", BooleanType(), True),
    StructField("status_code", IntegerType(), True),
    StructField("error", StringType(), True),
    StructField("avail_json", avail_json_schema, True),
])

availability_snapshots_schema = ArrayType(availability_snapshot_schema)

# ============================================================
# GOLD 1: candidate ↔ station map (from poi_json.results)
# ============================================================
@dlt.table(
    name="fact_tomtom_ev_candidate_station_map_r7",
    partition_cols=["p_date"],
    table_properties={"quality": "gold"},
)
@dlt.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dlt.expect("snapshot_ts_not_null", "snapshot_ts IS NOT NULL")
def gold_candidate_station_map():
    s = dlt.read_stream(SILVER_SRC)

    # Parse poi_json string -> struct
    s = s.withColumn("poi", F.from_json(F.col("poi_json"), poi_json_schema))

    # Explode results (one row per station)
    r = (
        s
        .withColumn("res", F.explode_outer(F.col("poi.results")))
        .withColumn("poi_id", F.col("res.id"))
        .withColumn("dist_m", F.col("res.dist"))
        .withColumn("poi_score", F.col("res.score"))
        .withColumn("charging_availability_id", F.col("res.dataSources.chargingAvailability.id"))
        .withColumn("station_name", F.col("res.poi.name"))
        .withColumn("station_brand", F.expr("element_at(res.poi.brands, 1).name"))
        .withColumn("station_category", F.expr("element_at(res.poi.categories, 1)"))
        .withColumn("station_lat", F.col("res.position.lat"))
        .withColumn("station_lon", F.col("res.position.lon"))
        .withColumn("connectors_static_json", F.to_json(F.col("res.chargingPark.connectors")))
        .withColumn("address_freeform", F.col("res.address.freeformAddress"))
        .withColumn("country_code", F.col("res.address.countryCode"))
        .withColumn("postal_code", F.col("res.address.postalCode"))
    )

    return r.select(
        "p_date",
        "event_id",
        "run_id",
        "snapshot_ts",
        "region_code",
        "region",
        "h3_r7",
        "degurba",
        "request_point_lat",
        "request_point_lon",
        "radius_m",

        "poi_id",
        "charging_availability_id",
        "dist_m",
        "poi_score",
        "station_name",
        "station_brand",
        "station_category",
        "station_lat",
        "station_lon",
        "connectors_static_json",
        "address_freeform",
        "postal_code",
        "country_code",

        # lineage/debug
        "http_status",
        "error_message",
        "airflow_dag_id",
        "airflow_task_id",
        "airflow_run_id",
        "airflow_try_number",
        "ingest_ts",
        "eventhub_enqueued_ts",
        "eh_partition",
        "eh_offset",
        "eh_sequence_number",
    )

# ============================================================
# GOLD 2: availability snapshots (from availability_snapshots[*].avail_json.connectors)
# One row per (chargingAvailabilityId, connectorType, powerKW?) at snapshot_ts.
# ============================================================
@dlt.table(
    name="fact_tomtom_ev_availability_snapshots_r7",
    partition_cols=["p_date"],
    table_properties={"quality": "gold"},
)
@dlt.expect_or_drop("event_id_not_null", "event_id IS NOT NULL")
@dlt.expect("snapshot_ts_not_null", "snapshot_ts IS NOT NULL")
def gold_availability_snapshots():
    s = dlt.read_stream(SILVER_SRC)

    # Parse availability_snapshots string -> array<struct>
    s = s.withColumn("av", F.from_json(F.col("availability_snapshots"), availability_snapshots_schema))

    # explode availability_snapshots
    a = (
        s
        .withColumn("a", F.explode_outer(F.col("av")))
        .withColumn("charging_availability_id", F.col("a.chargingAvailabilityId"))
        .withColumn("avail_ok", F.col("a.ok"))
        .withColumn("avail_status_code", F.col("a.status_code"))
        .withColumn("avail_error", F.col("a.error"))
        .withColumn("connectors", F.col("a.avail_json.connectors"))
    )

    # explode connectors
    c = (
        a
        .withColumn("c", F.explode_outer(F.col("connectors")))
        .withColumn("connector_type", F.col("c.type"))
        .withColumn("total", F.col("c.total"))
        .withColumn("cur", F.col("c.availability.current"))
        .withColumn("ppl", F.col("c.availability.perPowerLevel"))
    )

    # If perPowerLevel exists -> explode it; otherwise create a single row with power_kw = null using current
    with_pl = (
        c
        .withColumn("pl", F.explode_outer(F.col("ppl")))
        .withColumn("power_kw", F.col("pl.powerKW"))
        .withColumn("available", F.col("pl.available"))
        .withColumn("occupied", F.col("pl.occupied"))
        .withColumn("reserved", F.col("pl.reserved"))
        .withColumn("unknown", F.col("pl.unknown"))
        .withColumn("out_of_service", F.col("pl.outOfService"))
        .where(F.col("ppl").isNotNull())
    )

    no_pl = (
        c
        .where(F.col("ppl").isNull())
        .withColumn("power_kw", F.lit(None).cast("double"))
        .withColumn("available", F.col("cur.available"))
        .withColumn("occupied", F.col("cur.occupied"))
        .withColumn("reserved", F.col("cur.reserved"))
        .withColumn("unknown", F.col("cur.unknown"))
        .withColumn("out_of_service", F.col("cur.outOfService"))
    )

    out = with_pl.unionByName(no_pl, allowMissingColumns=True)

    # Optional dedupe: same event_id should not repeat (safe even if retries happen)
    out = (
        out
        .withWatermark("snapshot_ts", WATERMARK)
        .dropDuplicates(["event_id", "charging_availability_id", "connector_type", "power_kw"])
    )

    return out.select(
        "p_date",
        "event_id",
        "run_id",
        "snapshot_ts",
        "region_code",
        "region",
        "h3_r7",
        "degurba",
        "request_point_lat",
        "request_point_lon",

        "charging_availability_id",
        "avail_ok",
        "avail_status_code",
        "avail_error",

        "connector_type",
        "power_kw",
        "total",
        "available",
        "occupied",
        "reserved",
        "unknown",
        "out_of_service",

        # lineage/debug
        "airflow_dag_id",
        "airflow_task_id",
        "airflow_run_id",
        "airflow_try_number",
        "ingest_ts",
        "eventhub_enqueued_ts",
        "eh_partition",
        "eh_offset",
        "eh_sequence_number",
    )