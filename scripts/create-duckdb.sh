#!/bin/bash

echo "Creating DuckDB database from Parquet files..."

# Find the latest run directory
LATEST_RUN=$(ls -t data/gold/kpis/ | grep "run_" | head -1)

if [ -z "$LATEST_RUN" ]; then
    echo "ERROR: No run directories found. Please run the Gold job first."
    exit 1
fi

LATEST_RUN_PATH="data/gold/kpis/$LATEST_RUN"
echo "Using latest run: $LATEST_RUN"

# Create DuckDB database
DB_PATH="evidence/sources/taxi_analytics/taxi_analytics.duckdb"
echo "Creating DuckDB at: $DB_PATH"

# Ensure the directory exists
mkdir -p "evidence/sources/taxi_analytics"

# Remove existing database if it exists
rm -f "$DB_PATH"

# Create DuckDB database with tables from Parquet files
/Users/chaddalrymple/code/2025-fall-csci-e88c-Group/.venv/bin/python - << EOF
import duckdb
import os

# Connect to DuckDB
conn = duckdb.connect('$DB_PATH')

# Define datasets and their parquet paths
latest_run = '$LATEST_RUN_PATH'
datasets = ['weekly_trip_volume', 'weekly_trips_revenue', 'time_vs_distance', 'kpi_summary']

for dataset in datasets:
    parquet_path = os.path.join(latest_run, dataset)
    if os.path.exists(parquet_path):
        try:
            print(f"Creating table {dataset} from {parquet_path}")
            conn.execute(f"CREATE TABLE {dataset} AS SELECT * FROM parquet_scan('{parquet_path}/*.snappy.parquet')")
            print(f"Created table {dataset}")
        except Exception as e:
            print(f"Error creating table {dataset}: {e}")
    else:
        print(f"Warning: {dataset} not found at {parquet_path}")

# Show tables created
print("\nTables in database:")
result = conn.execute("SHOW TABLES").fetchall()
for row in result:
    table_name = row[0]
    count = conn.execute(f"SELECT COUNT(*) FROM {table_name}").fetchone()[0]
    print(f"  {table_name}: {count} rows")

conn.close()
print(f"\nDuckDB database created at: $DB_PATH")
EOF

echo ""
echo "Setup complete! You can now run: npm run dev"
