Evidence.dev Streamlit dashboard

This small app reads parquet output files written by the Spark job (mounted from ./data) and displays simple KPIs.

Build & run with docker-compose (from repo root):

# Build the dashboard image and start services
docker-compose -f docker-compose-spark.yml build evidence-dashboard

docker-compose -f docker-compose-spark.yml up -d spark-master spark-worker evidence-dashboard

Then open http://localhost:8501

Notes:
- The dashboard expects parquet data under ./data/output/kpis and ./data/output/weekly_metrics
- If running the Spark job via `runSparkJob.sh`, the output path used in that script is `/opt/spark-data/output/` inside the containers which maps to `./data/output` on the host.
