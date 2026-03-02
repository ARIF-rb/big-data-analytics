# Bridge Monitoring - End-to-End Streaming Pipeline

## Project Overview
Real-time IoT sensor monitoring system for bridges using PySpark Structured Streaming with Bronze-Silver-Gold medallion architecture.

## Architecture
- **Bronze Layer**: Raw data ingestion from JSON streams
- **Silver Layer**: Data enrichment with metadata + quality validation
- **Gold Layer**: 1-minute windowed aggregations with stream-stream joins

## Setup & Execution

### Prerequisites
- Python 3.8+
- Apache Spark 3.x
- Java 11 or 17
- Hadoop winutils (Windows)

### Environment Setup
```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
```

### Running the Pipeline

**Terminal 1 - Data Generator:**
```powershell
python data_generator/data_generator.py --duration 600 --rate 1
```

**Terminal 2 - Bronze Layer:**
```powershell
python pipelines/bronze_ingest.py
```

**Terminal 3 - Silver Layer:**
```powershell
python pipelines/silver_enrichment.py
```

**Terminal 4 - Gold Layer:**
```powershell
python pipelines/gold_aggregation.py
```

### Validation
```powershell
python validate_pipeline.py
```

## Results
- **Bronze**: 5,264 records ingested
- **Silver**: 5,271 enriched records, 0 rejected
- **Gold**: 148 aggregated metric windows
- **Sensors**: Temperature, Vibration, Tilt
- **Bridges**: 5 bridges monitored

## Key Features
✓ Watermark-based late data handling (2 minutes)
✓ Tumbling window aggregations (1 minute)
✓ Stream-static joins for enrichment
✓ Stream-stream joins for metrics
✓ Data quality validation
✓ Checkpointing for fault tolerance
