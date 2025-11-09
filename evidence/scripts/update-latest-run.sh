#!/bin/bash

echo "🔄 Updating dashboard with latest run data..."

# Navigate to project root
cd "$(dirname "$0")/../.."

# Run the DuckDB update script
if [ -f "scripts/create-duckdb.sh" ]; then
    echo "📊 Refreshing DuckDB database..."
    ./scripts/create-duckdb.sh
else
    echo "❌ DuckDB update script not found!"
    exit 1
fi

echo "✅ Dashboard data updated successfully!"
echo "💡 Run 'npm run dev' to view the updated dashboard"
