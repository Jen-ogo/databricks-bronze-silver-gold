import json
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType, IntegerType,
    ArrayType
)

# =============================================================================
# Widgets (runtime configuration)
# =============================================================================
dbutils.widgets.text("checkpoint", "dbfs:/tmp/checkpoints/traffic/tomtom_flowsegment_r7_hub_to_gold/TEST_RESET_20260208_01")
dbutils.widgets.text("target_table", "gold.fact_tomtom_traffic_flowsegment_snapshots_r7")
dbutils.widgets.text("trigger", "10 seconds")
dbutils.widgets.text("max_events_per_trigger", "5000")
dbutils.widgets.text("watermark", "2 hours")

# Secret settings (Azure Key Vault-backed Databricks Secret Scope)
# - scope: Databricks secret scope name (backed by Key Vault)
# - key:   secret name inside the Key Vault scope
dbutils.widgets.text("secret_scope", "kv-geo-dbx-sub")
dbutils.widgets.text("eventhub_conn_key", "eventhub-conn-str")

CHECKPOINT = dbutils.widgets.get("checkpoint")
TARGET_TABLE = dbutils.widgets.get("target_table")
TRIGGER = dbutils.widgets.get("trigger")
MAX_EVENTS_PER_TRIGGER = int(dbutils.widgets.get("max_events_per_trigger"))
WATERMARK = dbutils.widgets.get("watermark")

SECRET_SCOPE = dbutils.widgets.get("secret_scope")
EVENTHUB_CONN_KEY = dbutils.widgets.get("eventhub_conn_key")

# =============================================================================
# Event Hubs credentials (loaded securely from Key Vault via Secret Scope)
# =============================================================================
EVENTHUB_CONN_STR = dbutils.secrets.get(scope=SECRET_SCOPE, key=EVENTHUB_CONN_KEY)

# Basic sanity check
assert EVENTHUB_CONN_STR.strip().startswith("Endpoint=sb://"), "Invalid EventHub connection string format"
print(f"Loaded EventHub connection string from secret scope '{SECRET_SCOPE}' (key '{EVENTHUB_CONN_KEY}')")

ehConf = {
  # Encrypt connection string on the JVM side (required by Spark EventHubs connector)
  "eventhubs.connectionString": sc._jvm.org.apache.spark.eventhubs.EventHubsUtils.encrypt(EVENTHUB_CONN_STR),
  "maxEventsPerTrigger": str(MAX_EVENTS_PER_TRIGGER),

  # By default we DO NOT set startingPosition:
  # - the consumer position is managed exclusively by the checkpoint
  # - avoids seqNo-related crashes when mixing modes
  #
  # In order to read "from the beginning", use a NEW checkpoint and uncomment:
  # "eventhubs.startingPosition": json.dumps({
  #   "offset": "-1",
  #   "seqNo": -1,
  #   "enqueuedTime": None,
  #   "isInclusive": True
  # }),
}

# =============================================================================
# Incoming event schema
# Extra fields (if present) are ignored.
# =============================================================================
event_schema = StructType([
    StructField("event_id", StringType(), True),
    StructField("run_id", StringType(), True),
    StructField("snapshot_ts", StringType(), True),

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
    StructField("raw_json", StringType(), True),   # JSON string returned by TomTom
])

# =============================================================================
# TomTom raw_json schema (to extract metrics into GOLD, consistent with batch logic)
# =============================================================================
coord_schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])

tomtom_schema = StructType([
    StructField("flowSegmentData", StructType([
        StructField("frc", StringType(), True),
        StructField("currentSpeed", IntegerType(), True),
        StructField("freeFlowSpeed", IntegerType(), True),
        StructField("currentTravelTime", IntegerType(), True),
        StructField("freeFlowTravelTime", IntegerType(), True),
        StructField("confidence", DoubleType(), True),
        StructField("roadClosure", BooleanType(), True),
        StructField("coordinates", StructType([
            StructField("coordinate", ArrayType(coord_schema), True)
        ]), True),
    ]), True),
])

# =============================================================================
# Read stream from Azure Event Hubs
# =============================================================================
raw = (
    spark.readStream
      .format("eventhubs")
      .options(**ehConf)
      .load()
)

# Parse message body -> JSON -> columns
parsed = (
    raw
      .withColumn("body_str", F.col("body").cast("string"))
      .withColumn("j", F.from_json(F.col("body_str"), event_schema))
      .select("j.*")
      .withColumn("snapshot_ts", F.to_timestamp("snapshot_ts"))
      # Ensure event_id exists (producer should send it, but keep a safe fallback)
      .withColumn("event_id", F.coalesce(F.col("event_id"), F.expr("uuid()")))
)

# =============================================================================
# Parse TomTom JSON payload and derive metrics
# =============================================================================
tomtom = (
    parsed
      .withColumn("tt", F.from_json(F.col("raw_json"), tomtom_schema))
      .withColumn("fsd", F.col("tt.flowSegmentData"))
      .withColumn("coords", F.col("fsd.coordinates.coordinate"))
      .withColumn("frc", F.col("fsd.frc"))
      .withColumn("current_speed", F.col("fsd.currentSpeed"))
      .withColumn("free_flow_speed", F.col("fsd.freeFlowSpeed"))
      .withColumn("current_travel_time", F.col("fsd.currentTravelTime"))
      .withColumn("free_flow_travel_time", F.col("fsd.freeFlowTravelTime"))
      .withColumn("confidence", F.col("fsd.confidence"))
      .withColumn("road_closure", F.col("fsd.roadClosure"))
      .withColumn("segment_coords_json", F.to_json(F.col("coords")))
)

# Build LINESTRING WKT from coordinates (lon lat order)
tomtom = tomtom.withColumn(
    "segment_linestring_wkt_4326",
    F.when(
        F.size(F.col("coords")) >= 2,
        F.concat(
            F.lit("LINESTRING("),
            F.concat_ws(
                ", ",
                F.transform(
                    F.col("coords"),
                    lambda c: F.concat(c["longitude"], F.lit(" "), c["latitude"])
                )
            ),
            F.lit(")")
        )
    )
)

# Derived metrics
tomtom = (
    tomtom
      .withColumn(
          "speed_ratio",
          F.when(
              (F.col("current_speed").isNotNull()) & (F.col("free_flow_speed") > 0),
              F.col("current_speed") / F.col("free_flow_speed")
          )
      )
      .withColumn(
          "delay_sec",
          F.when(
              (F.col("current_travel_time").isNotNull()) & (F.col("free_flow_travel_time").isNotNull()),
              F.col("current_travel_time") - F.col("free_flow_travel_time")
          )
      )
      .withColumn(
          "delay_ratio",
          F.when(
              (F.col("current_travel_time").isNotNull()) & (F.col("free_flow_travel_time") > 0),
              F.col("current_travel_time") / F.col("free_flow_travel_time")
          )
      )
)

# =============================================================================
# Output schema (must match DESCRIBE TABLE of TARGET_TABLE)
# =============================================================================
table_cols = [
  "run_id","snapshot_ts","request_point_key","request_point_lat","request_point_lon",
  "tomtom_zoom","tomtom_version","region_code","region","h3_r7","degurba","macro_score",
  "traffic_scope","near_ev_station","ev_station_id","road_feature_id","road_osm_id",
  "highway","maxspeed_kph","lanes","road_len_m","road_centroid_wkt_4326",
  "frc","current_speed","free_flow_speed","current_travel_time","free_flow_travel_time",
  "confidence","road_closure","segment_linestring_wkt_4326","segment_coords_json",
  "speed_ratio","delay_sec","delay_ratio","http_status","error_message","raw_json","event_id"
]

# =============================================================================
# Deduplication + watermark
# =============================================================================
# Watermark bounds the amount of state Spark must keep for dropDuplicates on streaming data.
# We deduplicate by event_id assuming producer generates stable unique IDs per snapshot event.
final_df = (
    tomtom
      .select(*[F.col(c) for c in table_cols])
      .withWatermark("snapshot_ts", WATERMARK)
      .dropDuplicates(["event_id"])
)

# =============================================================================
# Write stream to Delta table
# =============================================================================
query = (
    final_df.writeStream
      .format("delta")
      .outputMode("append")
      .option("checkpointLocation", CHECKPOINT)
      .trigger(processingTime=TRIGGER)
      .toTable(TARGET_TABLE)
)

query