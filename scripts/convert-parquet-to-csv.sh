#!/bin/bash

echo "Converting Parquet files to CSV for Evidence..."

# Create evidence data directory
mkdir -p evidence/sources/data

# Find the latest run directory
LATEST_RUN=$(ls -t data/gold/kpis/ | grep "run_" | head -1)

if [ -z "$LATEST_RUN" ]; then
    echo "ERROR: No run directories found. Please run the Gold job first."
    exit 1
fi

LATEST_RUN_PATH="data/gold/kpis/$LATEST_RUN"
echo "Using latest run: $LATEST_RUN"

# Use Docker Spark to convert Parquet to CSV
echo "Starting conversion using Docker Spark..."

# Use a simpler approach - copy parquet files and convert them using Python
echo "Creating temporary Python script to convert Parquet to CSV..."

# Create Python conversion script
cat > /tmp/convert_parquet.py << 'EOF'
import pandas as pd
import os
import glob

# Create output directory
os.makedirs('evidence/sources/data', exist_ok=True)

# Find latest run directory
run_dirs = glob.glob('data/gold/kpis/run_*')
if not run_dirs:
    print("No run directories found")
    exit(1)

latest_run = max(run_dirs)
print(f"Using latest run: {os.path.basename(latest_run)}")

datasets = ['weekly_trip_volume', 'weekly_trips_revenue', 'time_vs_distance', 'kpi_summary']

for dataset in datasets:
    parquet_path = os.path.join(latest_run, dataset)
    csv_path = f'evidence/sources/data/{dataset}.csv'
    
    if os.path.exists(parquet_path):
        try:
            print(f"Converting {dataset}...")
            df = pd.read_parquet(parquet_path)
            df.to_csv(csv_path, index=False)
            print(f"Converted {dataset}.csv")
        except Exception as e:
            print(f"Error converting {dataset}: {e}")
    else:
        print(f"Warning: {dataset} not found at {parquet_path}")

print("Conversion complete!")
EOF

# Run the Python script
python3 /tmp/convert_parquet.py

# Clean up temp files
rm -f /tmp/convert_parquet.py

echo ""
echo "Evidence data files:"
ls -lh evidence/sources/data/

echo ""
echo "Conversion complete! You can now run: npm run dev"
