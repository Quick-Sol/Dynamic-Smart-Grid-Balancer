# spark_grid_processor.py
# Databricks notebook or Spark Structured Streaming job

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, window, avg, sum as spark_sum, count, max as spark_max,
    stddev, lag, lead, when, expr, current_timestamp,
    from_json, to_timestamp, struct, lit
)
from pyspark.sql.types import *
from pyspark.sql.window import Window

# Initialize Spark with Delta Lake support
spark = SparkSession.builder \
    .appName("SmartGrid-DemandBalancer") \
    .config("spark.sql.extensions", "io.delta.sql.DeltaSparkSessionExtension") \
    .config("spark.sql.catalog.spark_catalog", "org.apache.spark.sql.delta.catalog.DeltaCatalog") \
    .config("spark.sql.streaming.stateStore.providerClass", "com.databricks.sql.streaming.state.RocksDBStateStoreProvider") \
    .getOrCreate()

# Schema for smart meter readings
meter_schema = StructType([
    StructField("meter_id", StringType(), True),
    StructField("zip_code", StringType(), True),
    StructField("timestamp", StringType(), True),
    StructField("power_kw", DoubleType(), True),
    StructField("voltage", DoubleType(), True),
    StructField("frequency", DoubleType(), True),
    StructField("temperature_f", DoubleType(), True),
    StructField("grid_stress_indicator", DoubleType(), True),
    StructField("reading_interval_sec", IntegerType(), True)
])

# Kafka source configuration
kafka_config = {
    "kafka.bootstrap.servers": "kafka-broker-1:9092,kafka-broker-2:9092,kafka-broker-3:9092",
    "subscribe": "smart-meter-readings",
    "startingOffsets": "latest",
    "failOnDataLoss": "false",
    "kafka.security.protocol": "SASL_SSL",
    "kafka.sasl.mechanism": "PLAIN",
    "kafka.sasl.jaas.config": "kafkashaded.org.apache.kafka.common.security.plain.PlainLoginModule required username='YOUR_USERNAME' password='YOUR_PASSWORD';"
}

# Read from Kafka
raw_stream = spark.readStream \
    .format("kafka") \
    .options(**kafka_config) \
    .load()

# Parse JSON readings
parsed_stream = raw_stream \
    .select(from_json(col("value").cast("string"), meter_schema).alias("data")) \
    .select("data.*") \
    .withColumn("event_time", to_timestamp("timestamp"))

# ============================================
# REAL-TIME ZIP CODE AGGREGATION (10s windows)
# ============================================

zip_aggregations = parsed_stream \
    .withWatermark("event_time", "30 seconds") \
    .groupBy(
        window("event_time", "10 seconds", "10 seconds"),
        "zip_code"
    ) \
    .agg(
        spark_sum("power_kw").alias("total_load_mw"),
        avg("power_kw").alias("avg_load_kw"),
        count("*").alias("meter_count"),
        spark_max("power_kw").alias("peak_load_kw"),
        stddev("power_kw").alias("load_stddev"),
        avg("voltage").alias("avg_voltage"),
        avg("frequency").alias("avg_frequency"),
        avg("temperature_f").alias("avg_temp_f"),
        avg("grid_stress_indicator").alias("avg_stress")
    ) \
    .withColumn("load_mw", col("total_load_mw") / 1000.0) \
    .select(
        col("window.start").alias("window_start"),
        col("window.end").alias("window_end"),
        "zip_code",
        "load_mw",
        "avg_load_kw",
        "meter_count",
        "peak_load_kw",
        "load_stddev",
        "avg_voltage",
        "avg_frequency",
        "avg_temp_f",
        "avg_stress"
    )

# Write zip aggregations to Delta table for querying
zip_query = zip_aggregations.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/zip_aggregations") \
    .table("grid.zip_code_loads")

# ============================================
# PREDICTIVE ANALYTICS: 15-Minute Overload Prediction
# Using SQL Window Functions (LEAD/LAG, Cumulative Sums)
# ============================================

# Create 1-minute tumbling windows for trend analysis
minute_trends = parsed_stream \
    .withWatermark("event_time", "5 minutes") \
    .groupBy(
        window("event_time", "1 minute", "1 minute"),
        "zip_code"
    ) \
    .agg(spark_sum("power_kw").alias("minute_load_kw"))

# Define window for 15-minute lookback with 1-minute steps
trend_window = Window \
    .partitionBy("zip_code") \
    .orderBy("window_start") \
    .rowsBetween(-14, 0)  # 15 minutes of history

# Advanced predictive analytics using window functions
predictions = minute_trends \
    .withColumn("window_start", col("window.start")) \
    .withColumn("window_end", col("window.end")) \
    .withColumn("load_acceleration", 
        # Rate of change: current - previous
        col("minute_load_kw") - lag("minute_load_kw", 1).over(
            Window.partitionBy("zip_code").orderBy("window_start")
        )
    ) \
    .withColumn("cumulative_15min_load", 
        # Cumulative sum over 15 minutes
        spark_sum("minute_load_kw").over(trend_window)
    ) \
    .withColumn("avg_15min_load", 
        # Moving average
        avg("minute_load_kw").over(trend_window)
    ) \
    .withColumn("load_trend_slope",
        # Linear regression slope approximation using LAG
        (col("minute_load_kw") - lag("minute_load_kw", 5).over(
            Window.partitionBy("zip_code").orderBy("window_start")
        )) / 5.0
    ) \
    .withColumn("predicted_next_minute",
        # Linear extrapolation
        col("minute_load_kw") + col("load_trend_slope")
    ) \
    .withColumn("predicted_5min",
        # 5-minute ahead prediction
        col("minute_load_kw") + (col("load_trend_slope") * 5)
    ) \
    .withColumn("predicted_15min",
        # 15-minute ahead prediction
        col("minute_load_kw") + (col("load_trend_slope") * 15)
    ) \
    .withColumn("overload_risk_score",
        # Risk scoring: 0-100 based on acceleration and capacity
        when(col("load_trend_slope") > 1000, 
             least(lit(100), col("predicted_15min") / 50000 * 100)
        ).otherwise(lit(0))
    ) \
    .withColumn("alert_level",
        when(col("overload_risk_score") > 80, "CRITICAL")
        .when(col("overload_risk_score") > 60, "HIGH")
        .when(col("overload_risk_score") > 40, "MEDIUM")
        .otherwise("LOW")
    ) \
    .withColumn("recommended_action",
        when(col("alert_level") == "CRITICAL", 
             "INITIATE_LOAD_SHEDDING: Reduce non-essential loads immediately")
        .when(col("alert_level") == "HIGH", 
             "SPIN_UP_RESERVES: Activate peaker plants and demand response")
        .when(col("alert_level") == "MEDIUM", 
             "MONITOR_CLOSELY: Prepare contingency protocols")
        .otherwise("NORMAL_OPERATIONS")
    )

# Write predictions to Delta for alerting
pred_query = predictions.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/predictions") \
    .table("grid.overload_predictions")

# ============================================
# REAL-TIME ALERT SINK (Kafka back to control systems)
# ============================================

alerts = predictions \
    .filter(col("alert_level").isin(["CRITICAL", "HIGH"])) \
    .select(
        col("zip_code").alias("key"),
        to_json(struct(
            "zip_code",
            "window_start",
            "overload_risk_score",
            "alert_level",
            "recommended_action",
            "predicted_15min",
            "current_load_kw"  # alias for minute_load_kw
        )).alias("value")
    )

alert_query = alerts.writeStream \
    .format("kafka") \
    .option("kafka.bootstrap.servers", "kafka-broker-1:9092") \
    .option("topic", "grid-alerts") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/alerts") \
    .start()

# ============================================
# GRID-WIDE DASHBOARD AGGREGATION
# ============================================

grid_wide = parsed_stream \
    .withWatermark("event_time", "1 minute") \
    .groupBy(window("event_time", "10 seconds")) \
    .agg(
        spark_sum("power_kw").alias("total_grid_load_mw"),
        count("*").alias("active_meters"),
        spark_max("power_kw").alias("single_peak_kw"),
        avg("temperature_f").alias("avg_temp_f")
    ) \
    .withColumn("grid_load_mw", col("total_grid_load_mw") / 1000.0)

grid_query = grid_wide.writeStream \
    .format("delta") \
    .outputMode("append") \
    .option("checkpointLocation", "/mnt/delta/checkpoints/grid_wide") \
    .table("grid.grid_wide_metrics")

# Wait for all streams
spark.streams.awaitAnyTermination()
