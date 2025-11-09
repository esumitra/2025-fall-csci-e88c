---
title: Detailed KPI Analysis
---

# 📊 Detailed KPI Analysis

## Key Performance Indicators Overview

```sql kpis
  select * from taxi_analytics.kpi_summary
```

### Peak Hour Analysis
Peak hour percentage indicates the concentration of trips during high-demand periods.

<BigValue 
  data={kpis} 
  value=value
  where="metric='peak_hour_pct'"
  title="Peak Hour Traffic Percentage"
  fmt=num2
  suffix="%"
/>

This metric shows that **{kpis.find(d => d.metric === 'peak_hour_pct')?.value.toFixed(1)}%** of all trips occur during peak hours, indicating high demand concentration during busy periods.

### Revenue Efficiency

<BigValue 
  data={kpis} 
  value=value
  where="metric='avg_revenue_per_mile'"
  title="Average Revenue per Mile"
  fmt=usd2
/>

The average revenue per mile is **${kpis.find(d => d.metric === 'avg_revenue_per_mile')?.value.toFixed(2)}**, which reflects the efficiency of trip monetization across the fleet.

### Night Operations

<BigValue 
  data={kpis} 
  value=value
  where="metric='night_trip_pct'"
  title="Night Trip Percentage"
  fmt=num2
  suffix="%"
/>

**{kpis.find(d => d.metric === 'night_trip_pct')?.value.toFixed(1)}%** of trips occur during night hours, indicating the importance of 24/7 operations.

## Comparative Analysis

```sql kpi_comparison
  select 
    metric,
    value,
    case 
      when metric = 'peak_hour_pct' then 'Operational Efficiency'
      when metric = 'avg_revenue_per_mile' then 'Financial Performance'
      when metric = 'night_trip_pct' then 'Service Coverage'
    end as category
  from taxi_analytics.kpi_summary
```

<BarChart
  data={kpi_comparison}
  x=metric
  y=value
  series=category
  title="KPI Values by Category"
  yAxisTitle="Value"
/>

## Time Series Analysis

```sql weekly_metrics
  select 
    r.pickup_week_start,
    r.total_trips,
    r.total_revenue,
    t.avg_duration_min,
    t.avg_distance_miles,
    r.total_revenue / r.total_trips as avg_fare,
    t.avg_duration_min / t.avg_distance_miles as efficiency_ratio
  from taxi_analytics.weekly_trips_revenue r
  join taxi_analytics.time_vs_distance t 
    on r.pickup_week_start = t.pickup_week_start
  order by r.pickup_week_start
```

### Revenue Metrics Over Time

<LineChart
  data={weekly_metrics}
  x=pickup_week_start
  y=total_revenue
  y2=avg_fare
  title="Revenue Trends"
  yAxisTitle="Total Revenue ($)"
  y2AxisTitle="Average Fare ($)"
/>

### Operational Efficiency

<LineChart
  data={weekly_metrics}
  x=pickup_week_start
  y=efficiency_ratio
  title="Trip Efficiency (Minutes per Mile)"
  yAxisTitle="Minutes per Mile"
/>

Lower values indicate more efficient trips (faster travel per unit distance).

## Borough Performance

```sql borough_analysis
  select 
    pickup_borough,
    sum(trip_volume) as total_trips,
    sum(trip_volume) * 100.0 / (select sum(trip_volume) from taxi_analytics.weekly_trip_volume) as pct_of_total
  from taxi_analytics.weekly_trip_volume
  group by pickup_borough
  order by total_trips desc
```

<BarChart
  data={borough_analysis}
  x=pickup_borough
  y=pct_of_total
  title="Trip Distribution by Borough"
  yAxisTitle="Percentage of Total Trips"
  fmt=num1
  suffix="%"
/>

### Borough Trends

```sql borough_weekly
  select *,
    trip_volume * 100.0 / sum(trip_volume) over (partition by pickup_week_start) as weekly_pct
  from taxi_analytics.weekly_trip_volume
  order by pickup_week_start, pickup_borough
```

<AreaChart
  data={borough_weekly}
  x=pickup_week_start
  y=weekly_pct
  series=pickup_borough
  title="Weekly Borough Share (%)"
  yAxisTitle="Percentage of Weekly Trips"  
  fmt=num1
  suffix="%"
/>

## Recommendations

Based on the KPI analysis:

1. **Peak Hour Optimization**: With {kpis.find(d => d.metric === 'peak_hour_pct')?.value.toFixed(1)}% of trips during peak hours, consider dynamic pricing strategies.

2. **Revenue Enhancement**: Current revenue per mile is ${kpis.find(d => d.metric === 'avg_revenue_per_mile')?.value.toFixed(2)} - explore route optimization opportunities.

3. **Night Service**: {kpis.find(d => d.metric === 'night_trip_pct')?.value.toFixed(1)}% night trips suggest good 24/7 coverage, maintain this service level.

---

*Analysis based on latest available data from the Spark processing pipeline.*
