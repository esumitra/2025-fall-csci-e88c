---
title: Data Quality & Monitoring
---

# 🔍 Data Quality & Monitoring

## Data Freshness

```sql data_freshness
  select 
    'Weekly Trip Volume' as dataset,
    max(pickup_week_start) as latest_date,
    count(*) as total_records
  from taxi_analytics.weekly_trip_volume
  
  union all
  
  select 
    'Weekly Revenue' as dataset,
    max(pickup_week_start) as latest_date,
    count(*) as total_records
  from taxi_analytics.weekly_trips_revenue
  
  union all
  
  select 
    'Time vs Distance' as dataset,
    max(pickup_week_start) as latest_date,
    count(*) as total_records
  from taxi_analytics.time_vs_distance
```

<DataTable 
  data={data_freshness}
  title="Dataset Freshness Summary"
/>

## Record Counts by Week

```sql weekly_records
  select 
    pickup_week_start,
    count(distinct pickup_borough) as boroughs_count,
    sum(trip_volume) as total_trips
  from taxi_analytics.weekly_trip_volume
  group by pickup_week_start
  order by pickup_week_start
```

<BarChart
  data={weekly_records}
  x=pickup_week_start
  y=total_trips
  title="Total Trips per Week"
  yAxisTitle="Number of Trips"
  fmt=num0
/>

<BarChart
  data={weekly_records}
  x=pickup_week_start
  y=boroughs_count
  title="Borough Coverage per Week"
  yAxisTitle="Number of Boroughs"
/>

## Data Completeness

```sql completeness_check
  select 
    'Trip Volume' as metric,
    count(*) as total_records,
    count(case when trip_volume > 0 then 1 end) as valid_records,
    count(case when trip_volume > 0 then 1 end) * 100.0 / count(*) as completeness_pct
  from taxi_analytics.weekly_trip_volume
  
  union all
  
  select 
    'Revenue Data' as metric,
    count(*) as total_records,
    count(case when total_revenue > 0 then 1 end) as valid_records,
    count(case when total_revenue > 0 then 1 end) * 100.0 / count(*) as completeness_pct
  from taxi_analytics.weekly_trips_revenue
  
  union all
  
  select 
    'Duration Data' as metric,
    count(*) as total_records,
    count(case when avg_duration_min > 0 then 1 end) as valid_records,
    count(case when avg_duration_min > 0 then 1 end) * 100.0 / count(*) as completeness_pct
  from taxi_analytics.time_vs_distance
  
  union all
  
  select 
    'Distance Data' as metric,
    count(*) as total_records,
    count(case when avg_distance_miles > 0 then 1 end) as valid_records,
    count(case when avg_distance_miles > 0 then 1 end) * 100.0 / count(*) as completeness_pct
  from taxi_analytics.time_vs_distance
```

<DataTable 
  data={completeness_check}
  title="Data Completeness Analysis"
/>

## Anomaly Detection

```sql revenue_anomalies
  select 
    pickup_week_start,
    total_revenue,
    avg(total_revenue) over () as avg_revenue,
    (total_revenue - avg(total_revenue) over ()) / stddev(total_revenue) over () as z_score,
    case 
      when abs((total_revenue - avg(total_revenue) over ()) / stddev(total_revenue) over ()) > 2 then 'Anomaly'
      else 'Normal'
    end as status
  from taxi_analytics.weekly_trips_revenue
  order by pickup_week_start
```

<LineChart
  data={revenue_anomalies}
  x=pickup_week_start
  y=total_revenue
  series=status
  title="Weekly Revenue with Anomaly Detection"
  yAxisTitle="Total Revenue ($)"
  fmt=usd0
/>

```sql trip_anomalies
  select 
    pickup_week_start,
    total_trips,
    avg(total_trips) over () as avg_trips,
    (total_trips - avg(total_trips) over ()) / stddev(total_trips) over () as z_score,
    case 
      when abs((total_trips - avg(total_trips) over ()) / stddev(total_trips) over ()) > 2 then 'Anomaly'
      else 'Normal'
    end as status
  from taxi_analytics.weekly_trips_revenue
  order by pickup_week_start
```

<LineChart
  data={trip_anomalies}
  x=pickup_week_start
  y=total_trips
  series=status
  title="Weekly Trip Volume with Anomaly Detection"
  yAxisTitle="Number of Trips"
  fmt=num0
/>

## Data Distribution Analysis

```sql borough_distribution
  select 
    pickup_borough,
    min(trip_volume) as min_trips,
    max(trip_volume) as max_trips,
    avg(trip_volume) as avg_trips,
    stddev(trip_volume) as stddev_trips
  from taxi_analytics.weekly_trip_volume
  group by pickup_borough
  order by avg_trips desc
```

<DataTable 
  data={borough_distribution}
  title="Trip Volume Distribution by Borough"
/>

```sql efficiency_distribution
  select 
    pickup_week_start,
    avg_duration_min,
    avg_distance_miles,
    avg_duration_min / avg_distance_miles as efficiency_ratio
  from taxi_analytics.time_vs_distance
  order by pickup_week_start
```

<ScatterPlot
  data={efficiency_distribution}
  x=avg_distance_miles
  y=avg_duration_min
  title="Duration vs Distance Distribution"
  xAxisTitle="Average Distance (miles)"
  yAxisTitle="Average Duration (minutes)"
/>

## Data Pipeline Health

```sql pipeline_health
  select 
    'KPI Summary' as table_name,
    count(*) as record_count,
    case when count(*) >= 3 then '✅ Healthy' else '⚠️ Warning' end as status
  from taxi_analytics.kpi_summary
  
  union all
  
  select 
    'Weekly Trip Volume' as table_name,
    count(*) as record_count,
    case when count(*) >= 10 then '✅ Healthy' else '⚠️ Warning' end as status
  from taxi_analytics.weekly_trip_volume
  
  union all
  
  select 
    'Weekly Revenue' as table_name,
    count(*) as record_count,
    case when count(*) >= 3 then '✅ Healthy' else '⚠️ Warning' end as status
  from taxi_analytics.weekly_trips_revenue
  
  union all
  
  select 
    'Time vs Distance' as table_name,
    count(*) as record_count,
    case when count(*) >= 3 then '✅ Healthy' else '⚠️ Warning' end as status
  from taxi_analytics.time_vs_distance
```

<DataTable 
  data={pipeline_health}
  title="Pipeline Health Status"
/>

## Recommendations

### Data Quality Actions
1. **Monitor Anomalies**: Set up alerts for revenue/trip volume Z-scores > 2
2. **Completeness Checks**: Ensure all required fields have valid values
3. **Freshness Monitoring**: Data should be updated at least weekly

### Pipeline Improvements
1. **Automated Validation**: Add data quality checks to Spark jobs
2. **Error Handling**: Implement robust error handling in ETL pipeline
3. **Monitoring**: Set up automated monitoring for data pipeline health

---

*Data quality metrics updated automatically with each pipeline run.*
