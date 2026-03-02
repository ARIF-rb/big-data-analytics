"""
Pipeline Validation Script

Validates the end-to-end bridge monitoring pipeline by reading and analyzing
data from bronze, silver, and gold layers. Provides comprehensive statistics
and sample outputs.

Usage:
    python validate_pipeline.py
"""

from pyspark.sql import SparkSession
from pyspark.sql.functions import col, count, avg, max as spark_max, min as spark_min
import os

def validate_bronze_layer(spark):
    """
    Validate bronze layer data integrity and count records.
    
    Args:
        spark (SparkSession): Active Spark session
        
    Returns:
        dict: Statistics about bronze layer records
    """
    print("\n### 1. BRONZE LAYER (Raw Ingestion) ###")
    try:
        bronze_temp = spark.read.parquet("bronze/bridge_temperature")
        bronze_vib = spark.read.parquet("bronze/bridge_vibration")
        bronze_tilt = spark.read.parquet("bronze/bridge_tilt")
        
        stats = {
            'temperature': bronze_temp.count(),
            'vibration': bronze_vib.count(),
            'tilt': bronze_tilt.count()
        }
        
        print(f"✓ Temperature records: {stats['temperature']}")
        print(f"✓ Vibration records:   {stats['vibration']}")
        print(f"✓ Tilt records:        {stats['tilt']}")
        
        print("\nSample Bronze Data:")
        bronze_temp.select("bridge_id", "value", "event_time_ts").show(3)
        
        return stats
    except Exception as e:
        print(f"✗ Bronze layer error: {e}")
        return {}

def validate_silver_layer(spark):
    """
    Validate silver layer data quality and enrichment.
    
    Args:
        spark (SparkSession): Active Spark session
        
    Returns:
        dict: Statistics about silver layer records including rejections
    """
    print("\n### 2. SILVER LAYER (Enriched & Validated) ###")
    try:
        silver_temp = spark.read.parquet("silver/bridge_temperature")
        silver_vib = spark.read.parquet("silver/bridge_vibration")
        silver_tilt = spark.read.parquet("silver/bridge_tilt")
        
        stats = {
            'temperature': silver_temp.count(),
            'vibration': silver_vib.count(),
            'tilt': silver_tilt.count()
        }
        
        print(f"✓ Temperature records: {stats['temperature']}")
        print(f"✓ Vibration records:   {stats['vibration']}")
        print(f"✓ Tilt records:        {stats['tilt']}")
        
        print("\nSample Silver Data (with metadata):")
        silver_temp.select("bridge_id", "name", "location", "value").show(3)
        
        # Check for rejected records
        try:
            rejected_temp = spark.read.parquet("silver/rejected/bridge_temperature")
            rejected_count = rejected_temp.count()
            print(f"\n⚠ Rejected temperature records: {rejected_count}")
            stats['rejected'] = rejected_count
        except:
            print("\n✓ No rejected records (all data is valid)")
            stats['rejected'] = 0
            
        return stats
    except Exception as e:
        print(f"✗ Silver layer error: {e}")
        return {}

def validate_gold_layer(spark):
    """
    Validate gold layer aggregated metrics and perform analytics.
    
    Args:
        spark (SparkSession): Active Spark session
        
    Returns:
        dict: Statistics about gold layer metrics
    """
    print("\n### 3. GOLD LAYER (Aggregated Metrics) ###")
    try:
        gold = spark.read.parquet("gold/bridge_metrics")
        total_metrics = gold.count()
        
        print(f"✓ Total 1-minute metric windows: {total_metrics}")
        
        print("\n📊 Sample Aggregated Metrics:")
        gold.orderBy(col("window_start").desc()).show(5, truncate=False)
        
        print("\n📈 Metrics Summary by Bridge:")
        gold.groupBy("bridge_id") \
            .agg(
                count("*").alias("windows"),
                avg("avg_temperature").alias("avg_temp"),
                spark_max("max_vibration").alias("peak_vibration"),
                spark_max("max_tilt_angle").alias("peak_tilt")
            ) \
            .orderBy("windows", ascending=False) \
            .show()
        
        print("\n🏆 Top 5 Bridges by Vibration:")
        gold.orderBy(col("max_vibration").desc()).select(
            "bridge_id", "window_start", "max_vibration", "avg_temperature", "max_tilt_angle"
        ).show(5)
        
        return {'total_windows': total_metrics}
    except Exception as e:
        print(f"✗ Gold layer error: {e}")
        print("Note: Gold layer may need more time to generate data")
        return {}

if __name__ == "__main__":
    # Set environment for Windows compatibility
    os.environ["PYSPARK_PYTHON"] = "python"
    os.environ["PYSPARK_DRIVER_PYTHON"] = "python"
    
    # Create Spark session
    spark = SparkSession.builder \
        .appName("ValidationQueries") \
        .master("local[*]") \
        .config("spark.driver.host", "127.0.0.1") \
        .config("spark.driver.bindAddress", "127.0.0.1") \
        .getOrCreate()
    
    spark.sparkContext.setLogLevel("ERROR")
    
    print("\n" + "="*70)
    print("BRIDGE MONITORING PIPELINE - VALIDATION RESULTS")
    print("="*70)
    
    # Validate all layers
    bronze_stats = validate_bronze_layer(spark)
    silver_stats = validate_silver_layer(spark)
    gold_stats = validate_gold_layer(spark)
    
    # Stop Spark session
    spark.stop()
    
    print("\n" + "="*70)
    print("Validation Complete!")
    print("="*70 + "\n")
