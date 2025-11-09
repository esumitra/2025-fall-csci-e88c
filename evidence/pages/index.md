---
title: NYC Taxi Analytics Dashboard
---

# NYC Taxi Analytics Dashboard

Welcome to the NYC Taxi Analytics Dashboard! This dashboard provides comprehensive insights into taxi trip patterns, revenue trends, and operational KPIs derived from NYC taxi data.

## Key Performance Indicators

```sql peak_hour
  select value from taxi_analytics.kpi_summary where metric = 'peak_hour_pct'
```

```sql avg_revenue
  select value from taxi_analytics.kpi_summary where metric = 'avg_revenue_per_mile'
```

```sql night_trips
  select value from taxi_analytics.kpi_summary where metric = 'night_trip_pct'
```

<div class="grid grid-cols-3 gap-4">
  <BigValue 
    data={peak_hour} 
    value=value
    title="Peak Hour Traffic"
    fmt='#,##0.00"%"'
  />
  <BigValue 
    data={avg_revenue} 
    value=value
    title="Revenue per Mile"
    fmt=usd2
  />
  <BigValue 
    data={night_trips} 
    value=value
    title="Night Trips"
    fmt='#,##0.00"%"'
  />
</div>

## Weekly Revenue Trends

```sql revenue_trends
  select 
    pickup_week_start,
    total_trips,
    total_revenue,
    total_revenue / total_trips as avg_fare
  from taxi_analytics.weekly_trips_revenue
  order by pickup_week_start
```

<LineChart
  data={revenue_trends}
  x=pickup_week_start
  y=total_revenue
  title="Weekly Total Revenue"
  yAxisTitle="Revenue ($)"
  fmt=usd0
/>

<LineChart
  data={revenue_trends}
  x=pickup_week_start
  y=total_trips
  title="Weekly Trip Volume"
  yAxisTitle="Number of Trips"
  fmt=num0
/>

<LineChart
  data={revenue_trends}
  x=pickup_week_start
  y=avg_fare
  title="Average Fare per Trip"
  yAxisTitle="Average Fare ($)"
  fmt=usd2
/>

## Trip Volume by Borough

```sql borough_volume
  select 
    pickup_borough,
    sum(trip_volume) as total_trips
  from taxi_analytics.weekly_trip_volume
  group by pickup_borough
  order by total_trips desc
```

<BarChart
  data={borough_volume}
  x=pickup_borough
  y=total_trips
  title="Total Trip Volume by Borough"
  yAxisTitle="Number of Trips"
  fmt=num0
/>

```sql weekly_borough_trends
  select *
  from taxi_analytics.weekly_trip_volume
  order by pickup_week_start, pickup_borough
```

<AreaChart
  data={weekly_borough_trends}
  x=pickup_week_start
  y=trip_volume
  series=pickup_borough
  title="Weekly Trip Volume Trends by Borough"
  yAxisTitle="Number of Trips"
  fmt=num0
/>

## Trip Duration vs Distance Analysis

```sql time_distance
  select 
    pickup_week_start,
    avg_duration_min,
    avg_distance_miles,
    avg_duration_min / avg_distance_miles as minutes_per_mile
  from taxi_analytics.time_vs_distance
  order by pickup_week_start
```

<ScatterPlot
  data={time_distance}
  x=avg_distance_miles
  y=avg_duration_min
  size=minutes_per_mile
  title="Trip Duration vs Distance"
  xAxisTitle="Average Distance (miles)"
  yAxisTitle="Average Duration (minutes)"
/>

<LineChart
  data={time_distance}
  x=pickup_week_start
  y=avg_duration_min
  y2=avg_distance_miles
  title="Weekly Trends: Duration vs Distance"
  yAxisTitle="Duration (minutes)"
  y2AxisTitle="Distance (miles)"
/>

## Data Summary

```sql data_summary
  select 
    'KPI Summary' as table_name,
    count(*) as row_count
  from taxi_analytics.kpi_summary
  
  union all
  
  select 
    'Weekly Trip Volume' as table_name,
    count(*) as row_count
  from taxi_analytics.weekly_trip_volume
  
  union all
  
  select 
    'Weekly Revenue' as table_name,
    count(*) as row_count
  from taxi_analytics.weekly_trips_revenue
  
  union all
  
  select 
    'Time vs Distance' as table_name,
    count(*) as row_count
  from taxi_analytics.time_vs_distance
```

<DataTable data={data_summary} />

## Data Refresh Information

This dashboard displays the latest available data from your Spark processing pipeline. To refresh the data:

1. Run the Spark Silver and Gold jobs: `npm run spark:silver && npm run spark:gold`
2. Update the DuckDB database: `./scripts/create-duckdb.sh`
3. Restart the Evidence development server: `npm run dev`

---

*Dashboard last updated: {new Date().toLocaleString()}*
