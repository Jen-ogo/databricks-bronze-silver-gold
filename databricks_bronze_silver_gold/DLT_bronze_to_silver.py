import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType, IntegerType,
)

# =============================================================================
# CONFIG
# =============================================================================
BRONZE_SRC = "geo_databricks_sub.bronze.fact_tomtom_flowsegment_events_r7"
WATERMARK = "2 hours" 

# =============================================================================
# SCHEMA: payload_json
# =============================================================================
payload_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("snapshot_ts", StringType(), True),

    StructField("source", StringType(), True),
    StructField("airflow_dag_id", StringType(), True),
    StructField("airflow_task_id", StringType(), True),
    StructField("airflow_run_id", StringType(), True),
    StructField("airflow_try_number", IntegerType(), True),

    StructField("tomtom_zoom", IntegerType(), True),
    StructField("tomtom_version", StringType(), True),

    StructField("request_point_key", StringType(), True),
    StructField("request_point_lat", DoubleType(), True),
    StructField("request_point_lon", DoubleType(), True),

    StructField("region_code", StringType(), True),
    StructField("region", StringType(), True),
    StructField("h3_r7", StringType(), True),
    StructField("degurba", IntegerType(), True),
    StructField("macro_score", DoubleType(), True),

    StructField("traffic_scope", StringType(), True),
    StructField("near_ev_station", BooleanType(), True),
    StructField("ev_station_id", StringType(), True),

    StructField("road_feature_id", StringType(), True),
    StructField("road_osm_id", StringType(), True),
    StructField("highway", StringType(), True),
    StructField("maxspeed_kph", DoubleType(), True),
    StructField("lanes", DoubleType(), True),
    StructField("road_len_m", DoubleType(), True),
    StructField("road_centroid_wkt_4326", StringType(), True),

    StructField("http_status", IntegerType(), True),
    StructField("error_message", StringType(), True),

    StructField("raw_json", StringType(), True),  # TomTom JSON string (escaped)
])

# =============================================================================
# BRONZE STREAM
# =============================================================================
@dlt.view(name="bronze_tomtom_flowsegment_events_r7_stream")
def bronze_stream():
    return dlt.read_stream(BRONZE_SRC)

# =============================================================================
# SILVER
# =============================================================================
@dlt.table(
    name="fact_tomtom_flowsegment_events_r7",
    partition_cols=["p_date"],
    table_properties={
        "quality": "silver",
        "pipelines.autoOptimize.managed": "true",
    },
)
# validators
@dlt.expect_or_drop("payload_not_null", "payload_json is not null")
@dlt.expect_or_drop("p_date_not_null", "p_date is not null")
def fact_tomtom_flowsegment_events_r7():
    b = dlt.read_stream("bronze_tomtom_flowsegment_events_r7_stream")

    s = (
        b
        .withColumn("p", F.from_json(F.col("payload_json"), payload_schema))
        .withColumn("snapshot_ts_payload", F.to_timestamp(F.col("p.snapshot_ts")))
        .withColumn("event_id_payload", F.col("p.event_id"))
        .withColumn("run_id_payload", F.col("p.run_id"))
    )

    # priority: payload -> flat columns 
    s = (
        s
        .withColumn("event_id", F.coalesce(F.col("event_id"), F.col("event_id_payload")))
        .withColumn("run_id", F.coalesce(F.col("run_id"), F.col("run_id_payload")))
        .withColumn("snapshot_ts", F.coalesce(F.col("snapshot_ts"), F.col("snapshot_ts_payload")))
        .drop("event_id_payload", "run_id_payload", "snapshot_ts_payload")
    )

    # extra expectations (beyond not null)
    dlt.expect("snapshot_ts_not_null", "snapshot_ts is not null")(lambda: s)
    dlt.expect("region_code_not_null", "p.region_code is not null")(lambda: s)

    # DEDUPE (stateful) — keep latest record based on snapshot_ts (event time) for each event_id
    s = (
        s
        .withWatermark("snapshot_ts", WATERMARK)
        .dropDuplicates(["event_id"])
    )

    out = s.select(
        "ingest_ts",
        "eventhub_enqueued_ts",
        "eh_partition",
        "eh_offset",
        "eh_sequence_number",
        "p_date",
        "event_id",
        "run_id",
        "snapshot_ts",

        # payload -> flat columns
        F.col("p.source").alias("source"),
        F.col("p.airflow_dag_id").alias("airflow_dag_id"),
        F.col("p.airflow_task_id").alias("airflow_task_id"),
        F.col("p.airflow_run_id").alias("airflow_run_id"),
        F.col("p.airflow_try_number").alias("airflow_try_number"),

        F.col("p.tomtom_zoom").alias("tomtom_zoom"),
        F.col("p.tomtom_version").alias("tomtom_version"),

        F.col("p.request_point_key").alias("request_point_key"),
        F.col("p.request_point_lat").alias("request_point_lat"),
        F.col("p.request_point_lon").alias("request_point_lon"),

        F.col("p.region_code").alias("region_code"),
        F.col("p.region").alias("region"),
        F.col("p.h3_r7").alias("h3_r7"),
        F.col("p.degurba").alias("degurba"),
        F.col("p.macro_score").alias("macro_score"),

        F.col("p.traffic_scope").alias("traffic_scope"),
        F.col("p.near_ev_station").alias("near_ev_station"),
        F.col("p.ev_station_id").alias("ev_station_id"),

        F.col("p.road_feature_id").alias("road_feature_id"),
        F.col("p.road_osm_id").alias("road_osm_id"),
        F.col("p.highway").alias("highway"),
        F.col("p.maxspeed_kph").alias("maxspeed_kph"),
        F.col("p.lanes").alias("lanes"),
        F.col("p.road_len_m").alias("road_len_m"),
        F.col("p.road_centroid_wkt_4326").alias("road_centroid_wkt_4326"),

        F.col("p.http_status").alias("http_status"),
        F.col("p.error_message").alias("error_message"),
        F.col("p.raw_json").alias("raw_json"),

        # raw
        "payload_json",
    )

    return out