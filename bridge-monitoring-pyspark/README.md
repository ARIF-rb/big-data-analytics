# Bridge Monitoring — Real-Time PySpark Streaming Pipeline

An end-to-end IoT sensor monitoring system for bridges using **PySpark Structured Streaming** with a Bronze-Silver-Gold medallion architecture. Simulates live sensor data and processes it through three pipeline stages in real time.

## Use Case

Infrastructure monitoring systems that ingest high-frequency sensor data (temperature, vibration, tilt) from multiple bridges and need real-time windowed aggregations for anomaly detection and reporting dashboards.

## Architecture

| Layer | Role |
|---|---|
| **Bronze** | Raw JSON stream ingestion — no transformation, just durability |
| **Silver** | Metadata enrichment + data quality validation (reject bad records) |
| **Gold** | 1-minute tumbling window aggregations with stream-stream joins |

## Features

- Synthetic IoT data generator simulating 5 bridges × 3 sensor types
- Watermark-based late data handling (2-minute tolerance)
- Stream-static joins for bridge metadata enrichment
- Stream-stream joins for cross-sensor correlation
- Checkpointing for fault tolerance and restartability
- Pipeline validation script to verify each layer's output

## Tech Stack

| Layer | Tools |
|---|---|
| Language | Python 3.8+ |
| Processing | Apache Spark 3.x (PySpark Structured Streaming) |
| Runtime | Java 11 or 17 |
| OS Support | Windows (with Hadoop winutils), Linux/macOS |

## Prerequisites

- Python 3.8 or higher
- **Java 11 or 17** — verify with `java -version`
- Apache Spark 3.x — verify with `spark-submit --version`
- **Windows only**: [Hadoop winutils](https://github.com/cdarlint/winutils) placed at `C:\hadoop\bin`

## Installation

```bash
pip install pyspark
```

## Environment Setup (Windows)

```powershell
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:HADOOP_HOME = "C:\hadoop"
$env:PATH = "$env:JAVA_HOME\bin;$env:HADOOP_HOME\bin;$env:PATH"
```

On Linux/macOS, set `JAVA_HOME` to your JDK path and skip `HADOOP_HOME`.

## Running

Start each component in a separate terminal, in order:

**Terminal 1 — Data Generator:**
```bash
python data_generator/data_generator.py --duration 600 --rate 1
```

**Terminal 2 — Bronze Layer:**
```bash
python pipelines/bronze_ingest.py
```

**Terminal 3 — Silver Layer:**
```bash
python pipelines/silver_enrichment.py
```

**Terminal 4 — Gold Layer:**
```bash
python pipelines/gold_aggregation.py
```

**Validation (after pipeline runs):**
```bash
python validate_pipeline.py
```

## Output & Results

From a sample run:

| Layer | Records | Notes |
|---|---|---|
| Bronze | 5,264 | Raw records ingested |
| Silver | 5,271 | Enriched records; 0 rejected |
| Gold | 148 | Aggregated 1-minute metric windows |

Sensors monitored: **Temperature**, **Vibration**, **Tilt** across **5 bridges**

## Notes

- All 4 processes must run simultaneously — the pipeline is streaming, not batch
- Checkpoint directories are written to disk; delete them to reset pipeline state
- The data generator runs for `--duration` seconds (default 600s = 10 minutes)
- Adjust `--rate` to simulate different sensor frequencies (records per second)
