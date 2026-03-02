"""
Silver Layer Enrichment Pipeline

Enriches bronze layer data with static metadata and applies data quality
validation rules. Routes valid records to silver layer and invalid records
to rejected folder.

Usage:
    python pipelines/silver_enrichment.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, when
import os

def create_silver_stream(spark, sensor_type):
    """
    Create and start a silver enrichment stream for a specific sensor type.
    
    Performs stream-static join with bridge metadata and applies data quality
    checks. Valid records are written to silver layer, invalid records to
    rejected folder.
    
    Args:
        spark (SparkSession): Active Spark session
        sensor_type (str): Type of sensor (temperature, vibration, or tilt)
        
    Returns:
        tuple: (valid_query, rejected_query) - Two streaming query objects
    """
    # Read bronze stream
    bronze_stream = (spark.readStream
                     .format("parquet")
                     .load(f"bronze/bridge_{sensor_type}"))
    
    # Load static metadata for stream-static join
    bridges_metadata = (spark.read
                        .format("csv")
                        .option("header", "true")
                        .load("metadata/bridges.csv"))
    
    # Perform stream-static join to enrich with metadata
    enriched_stream = bronze_stream.join(bridges_metadata, "bridge_id", "left")
    
    # Define sensor-specific validation ranges
    if sensor_type == "temperature":
        valid_range = (col("value") >= -40) & (col("value") <= 80)
    elif sensor_type == "vibration":
        valid_range = (col("value") >= 0) & (col("value") <= 150)
    else:  # tilt
        valid_range = (col("value") >= 0) & (col("value") <= 90)
    
    # Apply data quality checks
    quality_checked = enriched_stream.withColumn(
        "is_valid",
        when(col("value").isNotNull() & 
             col("event_time_ts").isNotNull() & 
             valid_range, True).otherwise(False)
    )
    
    # Split into valid and invalid streams
    valid_stream = quality_checked.filter(col("is_valid") == True)
    invalid_stream = quality_checked.filter(col("is_valid") == False)
    
    # Write valid records to silver layer
    valid_query = (valid_stream.writeStream
                   .format("parquet")
                   .option("checkpointLocation", f"checkpoints/silver_{sensor_type}")
                   .option("path", f"silver/bridge_{sensor_type}")
                   .outputMode("append")
                   .start())
    
    # Write rejected records for audit
    rejected_query = (invalid_stream.writeStream
                      .format("parquet")
                      .option("checkpointLocation", f"checkpoints/silver_rejected_{sensor_type}")
                      .option("path", f"silver/rejected/bridge_{sensor_type}")
                      .outputMode("append")
                      .start())
    
    return valid_query, rejected_query

if __name__ == "__main__":
    # Set environment for Windows compatibility
    os.environ['PYSPARK_PYTHON'] = 'python'
    os.environ['PYSPARK_DRIVER_PYTHON'] = 'python'
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("SilverEnrichment") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .config("spark.sql.streaming.schemaInference", "true") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("WARN")
    
    print("=" * 60)
    print("Starting Silver Enrichment Layer")
    print("=" * 60)
    
    # Start streams for all sensor types
    queries = []
    for sensor_type in ["temperature", "vibration", "tilt"]:
        print(f"Starting silver stream for: {sensor_type}")
        valid_q, rejected_q = create_silver_stream(spark, sensor_type)
        queries.extend([valid_q, rejected_q])
    
    print("\nAll silver streams started successfully!")
    print("Press Ctrl+C to stop...")
    print("=" * 60)
    
    # Wait for termination
    try:
        for query in queries:
            query.awaitTermination()
    except KeyboardInterrupt:
        print("\nStopping silver streams...")
        for query in queries:
            query.stop()
        spark.stop()
        print("Silver layer stopped successfully!")
