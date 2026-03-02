"""
Bronze Layer Ingestion Pipeline

Reads raw JSON sensor events from file system and persists them as Parquet
with minimal transformation. Ensures immutability and exactly-once semantics
through checkpointing.

Usage:
    python pipelines/bronze_ingest.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, current_timestamp, to_timestamp
from pyspark.sql.types import StructType, StructField, StringType, DoubleType, TimestampType
import os

def create_bronze_stream(spark, sensor_type):
    """
    Create and start a bronze ingestion stream for a specific sensor type.
    
    Args:
        spark (SparkSession): Active Spark session
        sensor_type (str): Type of sensor (temperature, vibration, or tilt)
        
    Returns:
        StreamingQuery: Running streaming query object
    """
    # Define schema for JSON events
    schema = StructType([
        StructField("event_time", StringType(), True),
        StructField("bridge_id", StringType(), True),
        StructField("sensor_type", StringType(), True),
        StructField("value", DoubleType(), True),
        StructField("ingest_time", StringType(), True)
    ])
    
    # Read streaming data from JSON files
    raw_stream = (spark.readStream
                  .format("json")
                  .schema(schema)
                  .option("maxFilesPerTrigger", 10)
                  .load(f"streams/bridge_{sensor_type}"))
    
    # Add processing columns
    bronze_stream = (raw_stream
                     .withColumn("event_time_ts", to_timestamp(col("event_time")))
                     .withColumn("processing_time", current_timestamp())
                     .withColumn("partition_date", col("event_time_ts").cast("date")))
    
    # Write to bronze layer with checkpointing
    query = (bronze_stream.writeStream
             .format("parquet")
             .option("checkpointLocation", f"checkpoints/bronze_{sensor_type}")
             .option("path", f"bronze/bridge_{sensor_type}")
             .outputMode("append")
             .partitionBy("partition_date")
             .start())
    
    return query

if __name__ == "__main__":
    # Set environment for Windows compatibility
    os.environ['PYSPARK_PYTHON'] = 'python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    
    # Create Spark session with Windows-friendly configuration
    spark = SparkSession.builder \
        .appName("BronzeIngest") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .config("spark.sql.adaptive.enabled", "false") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("Starting Bronze Ingestion Layer")
    print("=" * 60)
    
    # Start streams for all sensor types
    queries = []
    for sensor_type in ["temperature", "vibration", "tilt"]:
        print(f"Starting bronze stream for: {sensor_type}")
        query = create_bronze_stream(spark, sensor_type)
        queries.append(query)
    
    print("\nAll bronze streams started successfully!")
    print("Press Ctrl+C to stop...")
    print("=" * 60)
    
    # Wait for termination
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping bronze streams...")
        for query in queries:
            query.stop()
        spark.stop()
        print("Bronze layer stopped successfully!")
