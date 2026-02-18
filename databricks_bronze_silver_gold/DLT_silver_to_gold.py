import dlt
from pyspark.sql import functions as F
from pyspark.sql.types import (
    StructType, StructField,
    StringType, BooleanType, DoubleType, IntegerType,
    ArrayType
)

# =============================================================================
# CONFIG
# =============================================================================
SILVER_SRC = "geo_databricks_sub.silver.fact_tomtom_flowsegment_events_r7"

# =============================================================================
# TomTom raw_json schema (for parsing in GOLD)
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
# SILVER INPUT STREAM (external table)
# =============================================================================
@dlt.view(name="silver_tomtom_flowsegment_events_r7_stream")
def silver_stream():
    # assume silver table has same schema as bronze, but with parsed payload_json columns + expectations applied
    return spark.readStream.table(SILVER_SRC)

# =============================================================================
# GOLD OUTPUT
# =============================================================================
@dlt.table(
    name="fact_tomtom_traffic_flowsegment_snapshots_r7",
    partition_cols=["p_date"],
    table_properties={
        "quality": "gold",
        "pipelines.autoOptimize.managed": "true",
    },
)
@dlt.expect_or_drop("event_id_not_null", "event_id is not null")
@dlt.expect_or_drop("snapshot_ts_not_null", "snapshot_ts is not null")
@dlt.expect_or_drop("p_date_not_null", "p_date is not null")
def fact_tomtom_traffic_flowsegment_snapshots_r7():
    s = dlt.read_stream("silver_tomtom_flowsegment_events_r7_stream")

    # unpack raw_json
    t = (
        s
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

    # LINESTRING WKT (lon lat order)
    t = t.withColumn(
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

    # derived metrics (+ check for divide by zero)
    t = (
        t
        .withColumn(
            "speed_ratio",
            F.when((F.col("current_speed").isNotNull()) & (F.col("free_flow_speed") > 0),
                   F.col("current_speed") / F.col("free_flow_speed"))
        )
        .withColumn(
            "delay_sec",
            F.when((F.col("current_travel_time").isNotNull()) & (F.col("free_flow_travel_time").isNotNull()),
                   F.col("current_travel_time") - F.col("free_flow_travel_time"))
        )
        .withColumn(
            "delay_ratio",
            F.when((F.col("current_travel_time").isNotNull()) & (F.col("free_flow_travel_time") > 0),
                   F.col("current_travel_time") / F.col("free_flow_travel_time"))
        )
    )

    # expectations (dont drop, just monitor)
    dlt.expect("speed_ratio_non_negative", "speed_ratio is null or speed_ratio >= 0")(lambda: t)
    dlt.expect("delay_sec_reasonable", "delay_sec is null or delay_sec > -3600")(lambda: t)

    return t.select(
        "run_id", "snapshot_ts", "p_date",
        "request_point_key", "request_point_lat", "request_point_lon",
        "tomtom_zoom", "tomtom_version",
        "region_code", "region", "h3_r7", "degurba", "macro_score",
        "traffic_scope", "near_ev_station", "ev_station_id",
        "road_feature_id", "road_osm_id", "highway", "maxspeed_kph", "lanes", "road_len_m",
        "road_centroid_wkt_4326",
        "frc", "current_speed", "free_flow_speed",
        "current_travel_time", "free_flow_travel_time",
        "confidence", "road_closure",
        "segment_linestring_wkt_4326", "segment_coords_json",
        "speed_ratio", "delay_sec", "delay_ratio",
        "http_status", "error_message",
        "raw_json", "event_id",

        # lineage/debug
        "ingest_ts", "eventhub_enqueued_ts", "eh_partition", "eh_offset", "eh_sequence_number",
        "airflow_dag_id", "airflow_task_id", "airflow_run_id", "airflow_try_number",
    )