import json
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StructField, StringType

# =============================================================================
# TomTom EV -> BRONZE consumer (Azure Event Hubs -> Delta)
# =============================================================================
# Reads from EventHub entity (tomtom-ev-raw) and writes raw envelope to:
#   geo_databricks_sub.bronze.fact_tomtom_ev_events_r7
# =============================================================================

# -----------------------------------------------------------------------------
# Widgets
# -----------------------------------------------------------------------------
dbutils.widgets.text(
  "checkpoint",
  "dbfs:/tmp/checkpoints/ev/tomtom_ev_r7_hub_to_bronze/RESET_20260222_01"
)
dbutils.widgets.text("target_table", "geo_databricks_sub.bronze.fact_tomtom_ev_events_r7")
dbutils.widgets.text("trigger", "10 seconds")
dbutils.widgets.text("max_events_per_trigger", "5000")

dbutils.widgets.text("secret_scope", "kv-geo-dbx-sub")
dbutils.widgets.text("eventhub_conn_key", "eventhub-conn-str-ev")  # <-- secret created

# Optional: to enforce eventhub name in code (mostly for logging)
dbutils.widgets.text("eventhub_name", "tomtom-ev-raw")

# Control read position (ONLY meaningful with NEW checkpoint)
dbutils.widgets.dropdown("from_beginning", "false", ["false", "true"])

CHECKPOINT = dbutils.widgets.get("checkpoint").strip()
TARGET_TABLE = dbutils.widgets.get("target_table").strip()
TRIGGER = dbutils.widgets.get("trigger").strip()
MAX_EVENTS_PER_TRIGGER = int(dbutils.widgets.get("max_events_per_trigger"))

SECRET_SCOPE = dbutils.widgets.get("secret_scope").strip()
EVENTHUB_CONN_KEY = dbutils.widgets.get("eventhub_conn_key").strip()
EVENTHUB_NAME = dbutils.widgets.get("eventhub_name").strip()
FROM_BEGINNING = dbutils.widgets.get("from_beginning").strip().lower() == "true"

assert CHECKPOINT, "checkpoint widget must be set"
assert TARGET_TABLE, "target_table widget must be set"
assert SECRET_SCOPE, "secret_scope widget must be set"
assert EVENTHUB_CONN_KEY, "eventhub_conn_key widget must be set"
assert EVENTHUB_NAME, "eventhub_name widget must be set"

# -----------------------------------------------------------------------------
# Load connection string from secret scope
# -----------------------------------------------------------------------------
EVENTHUB_CONN_STR = dbutils.secrets.get(scope=SECRET_SCOPE, key=EVENTHUB_CONN_KEY)
EVENTHUB_CONN_STR = (EVENTHUB_CONN_STR or "").replace("\r", "").replace("\n", "").strip()

assert EVENTHUB_CONN_STR.startswith("Endpoint=sb://"), "Invalid EventHub connection string format"

# -----------------------------------------------------------------------------
# Ensure EntityPath is EXACTLY what we want (remove old EntityPath if any)
# -----------------------------------------------------------------------------
def _force_entity_path(conn_str: str, eventhub_name: str) -> str:
    parts = [p for p in conn_str.split(";") if p and not p.startswith("EntityPath=")]
    parts.append(f"EntityPath={eventhub_name}")
    return ";".join(parts)

EVENTHUB_CONN_STR = _force_entity_path(EVENTHUB_CONN_STR, EVENTHUB_NAME)

# -----------------------------------------------------------------------------
# Encrypt connection string if JVM helper exists (runtime-dependent)
# -----------------------------------------------------------------------------
def _encrypt_if_possible(conn_str: str) -> str:
    """
    Some runtimes require encrypted conn string:
      sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str)
    On other runtimes this class may not exist.
    We'll try encrypt; if not available -> fallback to plain.
    """
    try:
        jvm = sc._jvm
        _ = jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt
        return jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(conn_str)
    except Exception as e:
        print(f"[WARN] EventHubsUtils.encrypt not available, using plain connection string. "
              f"Details: {type(e).__name__}: {e}")
        return conn_str

EVENTHUB_CONN_FOR_SPARK = _encrypt_if_possible(EVENTHUB_CONN_STR)

# -----------------------------------------------------------------------------
# Logging (sanitized)
# -----------------------------------------------------------------------------
def _sanitize_conn_str(conn_str: str) -> str:
    # Hide SharedAccessKey value
    out = []
    for p in conn_str.split(";"):
        if p.startswith("SharedAccessKey="):
            out.append("SharedAccessKey=***")
        else:
            out.append(p)
    return ";".join(out)

print(f"[INFO] target EventHub entity: {EVENTHUB_NAME}")
print(f"[INFO] from_beginning: {FROM_BEGINNING}")
print(f"[INFO] checkpoint: {CHECKPOINT}")
print(f"[INFO] target_table: {TARGET_TABLE}")
print(f"[INFO] conn str (sanitized): {_sanitize_conn_str(EVENTHUB_CONN_STR)}")

# -----------------------------------------------------------------------------
# EventHubs config
# -----------------------------------------------------------------------------
ehConf = {
  "eventhubs.connectionString": EVENTHUB_CONN_FOR_SPARK,
  "maxEventsPerTrigger": str(MAX_EVENTS_PER_TRIGGER),
}

# Only set startingPosition if you're truly resetting history
# IMPORTANT: this must be paired with a NEW checkpoint path!
if FROM_BEGINNING:
    ehConf["eventhubs.startingPosition"] = json.dumps({
      "offset": "-1",
      "seqNo": -1,
      "enqueuedTime": None,
      "isInclusive": True
    })
    print("[WARN] startingPosition is set to beginning. Make sure checkpoint is NEW, "
          "otherwise Spark will ignore startingPosition and keep checkpoint offsets.")

# -----------------------------------------------------------------------------
# Minimal schema for indexing (payload_json stays raw)
# -----------------------------------------------------------------------------
payload_min_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("snapshot_ts", StringType(), True),  # ISO "...Z" from producer
])

# -----------------------------------------------------------------------------
# Read stream from Event Hubs
# -----------------------------------------------------------------------------
raw = (
    spark.readStream
      .format("eventhubs")
      .options(**ehConf)
      .load()
)

# -----------------------------------------------------------------------------
# Normalize to BRONZE columns (matches your DDL)
# -----------------------------------------------------------------------------
bronze_stream = (
    raw
      .withColumn("ingest_ts", F.current_timestamp())
      .withColumn("eventhub_enqueued_ts", F.col("enqueuedTime").cast("timestamp"))
      .withColumn("eh_partition", F.col("partition").cast("string"))
      .withColumn("eh_offset", F.col("offset").cast("string"))
      .withColumn("eh_sequence_number", F.col("sequenceNumber").cast("long"))
      .withColumn("payload_json", F.col("body").cast("string"))
)

# Extract a few fields from payload_json (optional indexing)
bronze_stream = (
    bronze_stream
      .withColumn("p", F.from_json(F.col("payload_json"), payload_min_schema))
      .withColumn("event_id", F.col("p.event_id"))
      .withColumn("run_id", F.col("p.run_id"))
      .withColumn("snapshot_ts", F.to_timestamp(F.col("p.snapshot_ts")))
      .drop("p")
)

# Partition column p_date: prefer snapshot_ts date, fallback to ingest_ts date
bronze_stream = bronze_stream.withColumn(
    "p_date",
    F.coalesce(F.to_date("snapshot_ts"), F.to_date("ingest_ts"))
)

final_df = bronze_stream.select(
    "ingest_ts",
    "eventhub_enqueued_ts",
    "eh_partition",
    "eh_offset",
    "eh_sequence_number",
    "event_id",
    "run_id",
    "snapshot_ts",
    "payload_json",
    "p_date",
)

# -----------------------------------------------------------------------------
# Write stream to Delta BRONZE
# -----------------------------------------------------------------------------
query = (
    final_df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", CHECKPOINT)
      .trigger(processingTime=TRIGGER)
      .toTable(TARGET_TABLE)
)

query