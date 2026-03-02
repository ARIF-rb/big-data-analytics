# scripts/run_all.ps1
$env:JAVA_HOME = "C:\Program Files\Eclipse Adoptium\jdk-17.0.17.10-hotspot"
$env:HADOOP_HOME = "C:\hadoop"

Start-Process powershell -ArgumentList "python data_generator/data_generator.py --duration 600"
Start-Sleep -Seconds 20
Start-Process powershell -ArgumentList "python pipelines/bronze_ingest.py"
Start-Sleep -Seconds 30
Start-Process powershell -ArgumentList "python pipelines/silver_enrichment.py"
Start-Sleep -Seconds 30
Start-Process powershell -ArgumentList "python pipelines/gold_aggregation.py"