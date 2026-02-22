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
BRONZE_SRC = "geo_databricks_sub.bronze.fact_tomtom_ev_events_r7"
WATERMARK = "2 hours"

# ============================================================
# SCHEMA: payload_json (envelope produced by Airflow)
# Keep it close to the actual payload.
# ============================================================
payload_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("snapshot_ts", StringType(), True),

    StructField("source", StringType(), True),
    StructField("http_status", IntegerType(), True),
    StructField("error_message", StringType(), True),

    StructField("airflow_dag_id", StringType(), True),
    StructField("airflow_task_id", StringType(), True),
    StructField("airflow_run_id", StringType(), True),
    StructField("airflow_try_number", IntegerType(), True),

    StructField("region_code", StringType(), True),
    StructField("region", StringType(), True),
    StructField("h3_r7", StringType(), True),
    StructField("degurba", IntegerType(), True),

    StructField("request_point_lat", DoubleType(), True),
    StructField("request_point_lon", DoubleType(), True),
    StructField("radius_m", IntegerType(), True),

    # Keep these as raw JSON (stringified later if needed)
    StructField("poi_json", StringType(), True),                 # might arrive as struct or string; will normalize
    StructField("availability_snapshots", StringType(), True),   # same
])

# ============================================================
# BRONZE STREAM VIEW
# ============================================================
@dlt.view(name="bronze_tomtom_ev_events_r7_stream")
def bronze_stream():
    return dlt.read_stream(BRONZE_SRC)

# ============================================================
# SILVER TABLE
# - Parse payload_json into typed columns
# - Deduplicate by event_id (watermark on snapshot_ts)
# - Keep payload_json for audit/debug
# ============================================================
@dlt.table(
    name="fact_tomtom_ev_events_r7",
    partition_cols=["p_date"],
    table_properties={"quality": "silver"},
)
@dlt.expect_or_drop("payload_not_null", "payload_json IS NOT NULL")
def silver_events():
    b = dlt.read_stream("bronze_tomtom_ev_events_r7_stream")

    # Parse envelope
    s = (
        b
        .withColumn("p", F.from_json(F.col("payload_json"), payload_schema))
        .withColumn("snapshot_ts_payload", F.to_timestamp(F.col("p.snapshot_ts")))
        .withColumn("event_id_payload", F.col("p.event_id"))
        .withColumn("run_id_payload", F.col("p.run_id"))
    )

    # Prefer values from payload if bronze indexing fields are null/misaligned
    s = (
        s
        .withColumn("event_id", F.coalesce(F.col("event_id"), F.col("event_id_payload")))
        .withColumn("run_id", F.coalesce(F.col("run_id"), F.col("run_id_payload")))
        .withColumn("snapshot_ts", F.coalesce(F.col("snapshot_ts"), F.col("snapshot_ts_payload")))
        .drop("event_id_payload", "run_id_payload", "snapshot_ts_payload")
    )

    # Basic quality checks (do not drop errors automatically except critical ones)
    s = (
        s
        .withColumn("request_point_lat", F.col("p.request_point_lat"))
        .withColumn("request_point_lon", F.col("p.request_point_lon"))
    )

    # Dedupe for stable downstream gold
    s = (
        s
        .withWatermark("snapshot_ts", WATERMARK)
        .dropDuplicates(["event_id"])
    )

    # Normalize poi_json / availability_snapshots as STRING for stability:
    # - If producer sends them as struct/array in the future, to_json keeps it consistent.
    poi_json_str = F.when(
        F.col("p.poi_json").isNull(), F.lit(None).cast("string")
    ).otherwise(F.col("p.poi_json").cast("string"))

    avail_json_str = F.when(
        F.col("p.availability_snapshots").isNull(), F.lit(None).cast("string")
    ).otherwise(F.col("p.availability_snapshots").cast("string"))

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

        F.col("p.source").alias("source"),
        F.col("p.http_status").alias("http_status"),
        F.col("p.error_message").alias("error_message"),

        F.col("p.airflow_dag_id").alias("airflow_dag_id"),
        F.col("p.airflow_task_id").alias("airflow_task_id"),
        F.col("p.airflow_run_id").alias("airflow_run_id"),
        F.col("p.airflow_try_number").alias("airflow_try_number"),

        F.col("p.region_code").alias("region_code"),
        F.col("p.region").alias("region"),
        F.col("p.h3_r7").alias("h3_r7"),
        F.col("p.degurba").alias("degurba"),

        F.col("p.request_point_lat").alias("request_point_lat"),
        F.col("p.request_point_lon").alias("request_point_lon"),
        F.col("p.radius_m").alias("radius_m"),

        poi_json_str.alias("poi_json"),
        avail_json_str.alias("availability_snapshots"),

        # Raw envelope for audit/debug
        "payload_json",
    )

    return out