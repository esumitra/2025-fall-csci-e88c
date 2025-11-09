# 🚕 NYC Taxi Analytics Dashboard

This Evidence dashboard provides comprehensive visualization and analysis of NYC taxi data, built on top of a Spark data processing pipeline with DuckDB for fast analytics.

## 📊 Dashboard Pages

### 1. Main Dashboard (`/`)
- **Key Performance Indicators**: Peak hour percentage, revenue per mile, night trips percentage
- **Weekly Revenue Trends**: Total revenue, trip volume, and average fare trends
- **Borough Analysis**: Trip volume distribution and trends by NYC borough
- **Trip Efficiency**: Duration vs distance analysis and efficiency metrics

### 2. Detailed KPI Analysis (`/kpis`)
- In-depth analysis of each KPI with context and recommendations
- Comparative analysis across different metrics
- Time series analysis of operational efficiency
- Borough performance deep dive

### 3. Data Quality & Monitoring (`/monitoring`)
- Data freshness and completeness checks
- Anomaly detection for revenue and trip volume
- Data distribution analysis
- Pipeline health monitoring

## 🚀 Getting Started

### Prerequisites
- Node.js and npm installed
- Python environment with pandas and duckdb
- Completed Spark pipeline execution (Silver and Gold jobs)

### Quick Start
From the main project directory:

```bash
# Start the complete pipeline and dashboard
npm run pipeline:full

# Or start just the dashboard (if data already exists)
npm run dev
```

### Manual Setup
1. **Create DuckDB Database**:
   ```bash
   npm run update:duckdb
   ```

2. **Start Development Server**:
   ```bash
   npm run dev
   ```

## 🔄 Data Refresh

### After Running Spark Jobs
```bash
npm run update:dashboard
```

### Quick DuckDB Refresh
```bash
npm run update:duckdb
```

## 📈 Key Features

- **Interactive Visualizations**: Line charts, bar charts, scatter plots, and KPI cards
- **Real-time Data**: Automatically refreshes with latest Spark pipeline outputs
- **Data Quality Monitoring**: Anomaly detection and completeness checks
- **Multi-page Analysis**: Overview, detailed KPIs, and monitoring pages

## 🛠️ Technical Details

- **Database**: DuckDB for fast analytics on parquet files
- **Data Source**: Spark Gold layer (`data/gold/kpis/`)
- **Framework**: Evidence.dev for interactive dashboards
- **Visualizations**: Built-in Evidence components with custom styling

## 📚 Resources

- [Evidence Documentation](https://docs.evidence.dev/)
- [Main Project Documentation](../README.md)

---

*Part of the NYC Taxi Analytics pipeline project.*
