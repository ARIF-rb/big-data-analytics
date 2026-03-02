"""
Gold Layer Aggregation Pipeline

Computes 1-minute windowed aggregations with 2-minute watermarks and performs
stream-stream joins to produce final analytics-ready metrics.

Aggregates:
    - avg_temperature: Average temperature per bridge per window
    - max_vibration: Maximum vibration per bridge per window
    - max_tilt_angle: Maximum tilt angle per bridge per window

Usage:
    python pipelines/gold_aggregation.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, window, avg, max as spark_max
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

def create_gold_aggregation(spark):
    """
    Create and start gold aggregation stream with windowed metrics.
    
    Reads three silver streams, applies watermarks, computes 1-minute tumbling
    window aggregations, and performs stream-stream joins to produce unified
    bridge metrics.
    
    Args:
        spark (SparkSession): Active Spark session
        
    Returns:
        StreamingQuery: Running streaming query object for gold metrics
    """
    # Define schema for silver streams
    silver_schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("bridge_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("ingest_time", StringType(), True),
        StructField("event_time_ts", TimestampType(), True),
        StructField("processing_time", TimestampType(), True),
        StructField("partition_date", StringType(), True),
        StructField("name", StringType(), True),
        StructField("location", StringType(), True),
        StructField("installation_date", StringType(), True),
        StructField("is_valid", StringType(), True)
    ])
    
    # Read silver streams with explicit schema
    temp_stream = (spark.readStream
                   .format("parquet")
                   .schema(silver_schema)
                   .load("silver/bridge_temperature"))
    
    vib_stream = (spark.readStream
                  .format("parquet")
                  .schema(silver_schema)
                  .load("silver/bridge_vibration"))
    
    tilt_stream = (spark.readStream
                   .format("parquet")
                   .schema(silver_schema)
                   .load("silver/bridge_tilt"))
    
    # Apply 2-minute watermarks for late data handling
    temp_watermarked = temp_stream.withWatermark("event_time_ts", "2 minutes")
    vib_watermarked = vib_stream.withWatermark("event_time_ts", "2 minutes")
    tilt_watermarked = tilt_stream.withWatermark("event_time_ts", "2 minutes")
    
    # Compute 1-minute tumbling window aggregations for temperature
    temp_agg = (temp_watermarked
                .groupBy(
                    col("bridge_id"),
                    window(col("event_time_ts"), "1 minute")
                )
                .agg(avg("value").alias("avg_temperature"))
                .select(
                    col("bridge_id"),
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("avg_temperature")
                ))
    
    # Compute 1-minute tumbling window aggregations for vibration
    vib_agg = (vib_watermarked
               .groupBy(
                   col("bridge_id"),
                   window(col("event_time_ts"), "1 minute")
               )
               .agg(spark_max("value").alias("max_vibration"))
               .select(
                   col("bridge_id"),
                   col("window.start").alias("window_start"),
                   col("window.end").alias("window_end"),
                   col("max_vibration")
               ))
    
    # Compute 1-minute tumbling window aggregations for tilt
    tilt_agg = (tilt_watermarked
                .groupBy(
                    col("bridge_id"),
                    window(col("event_time_ts"), "1 minute")
                )
                .agg(spark_max("value").alias("max_tilt_angle"))
                .select(
                    col("bridge_id"),
                    col("window.start").alias("window_start"),
                    col("window.end").alias("window_end"),
                    col("max_tilt_angle")
                ))
    
    # Perform stream-stream joins on window bounds
    joined = (temp_agg
              .join(vib_agg, ["bridge_id", "window_start", "window_end"], "inner")
              .join(tilt_agg, ["bridge_id", "window_start", "window_end"], "inner"))
    
    # Write joined metrics to gold layer
    query = (joined.writeStream
             .format("parquet")
             .option("checkpointLocation", "checkpoints/gold_metrics")
             .option("path", "gold/bridge_metrics")
             .outputMode("append")
             .start())
    
    return query

if __name__ == "__main__":
    # Set environment for Windows compatibility
    os.environ['PYSPARK_PYTHON'] = 'python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("GoldAggregation") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.shuffle.partitions", "2") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("Starting Gold Aggregation Layer")
    print("=" * 60)
    
    # Create and start gold aggregation stream
    query = create_gold_aggregation(spark)
    
    print("\nGold aggregation stream started successfully!")
    print("Press Ctrl+C to stop...")
    print("=" * 60)
    
    # Wait for termination
    try:
        query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping gold aggregation...")
        query.stop()
        spark.stop()
        print("Gold layer stopped successfully!")
