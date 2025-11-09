---
title: KPI Deep Dive Dashboard
---

# 📊 KPI Deep Dive Dashboard

## 🚦 Peak Hour Analysis

```sql peak_data
  select * from taxi_analytics.peak_hour_analysis
```

### Peak Hour Distribution

<BigValue 
  data={peak_data} 
  value=peak_hour_percentage
  where="time_period='Peak Hours (7-9 AM, 5-7 PM)'"
  title="Peak Hour Trips"
  fmt=num1
  suffix="%"
/>

<div class="grid grid-cols-2 gap-4">
  <div>
    <h4>Operational Insights</h4>
    {#each peak_data.filter(d => d.time_period === 'Peak Hours (7-9 AM, 5-7 PM)') as insight}
      <div class="p-4 bg-blue-50 rounded-lg">
        <p class="font-semibold">{insight.operational_insight}</p>
        <p class="text-sm text-gray-600 mt-2"><strong>Recommendation:</strong> {insight.recommendation}</p>
      </div>
    {/each}
  </div>
  <div>
    <h4>Peak vs Off-Peak Distribution</h4>
    <BarChart
      data={peak_data}
      x=time_period
      y=peak_hour_percentage
      title="Trip Distribution by Time Period"
      fmt=pct1
    />
  </div>
</div>

## 💰 Revenue Efficiency Analysis

```sql revenue_data
  select * from taxi_analytics.revenue_efficiency
```

### Current Performance vs Benchmark

<div class="grid grid-cols-3 gap-4">
  <BigValue 
    data={revenue_data} 
    value=revenue_per_mile
    where="analysis_type='Current Performance'"
    title="Current Revenue/Mile"
    fmt=usd2
  />
  <BigValue 
    data={revenue_data} 
    value=industry_benchmark
    where="analysis_type='Current Performance'"
    title="Industry Benchmark"
    fmt=usd2
  />
  <BigValue 
    data={revenue_data} 
    value=vs_benchmark
    where="analysis_type='Current Performance'"
    title="vs Benchmark"
    fmt=usd2
  />
</div>

### Performance Assessment

{#each revenue_data.filter(d => d.analysis_type === 'Current Performance') as perf}
  <div class="p-4 {perf.vs_benchmark >= 0 ? 'bg-green-50 border-green-200' : 'bg-yellow-50 border-yellow-200'} border rounded-lg">
    <h4 class="font-bold text-lg">{perf.performance_status}</h4>
    <p class="mt-2">{perf.assessment}</p>
  </div>
{/each}

### Improvement Opportunities

<DataTable 
  data={revenue_data.filter(d => d.analysis_type === 'Potential Revenue Impact')}
  columns={[
    {id: 'target_revenue_per_mile', title: 'Target Revenue/Mile', fmt: 'usd2'},
    {id: 'improvement_opportunity', title: 'Improvement Opportunity', fmt: 'usd2'},
    {id: 'target_description', title: 'Target'},
    {id: 'strategy', title: 'Strategy'}
  ]}
/>

## 🌙 Night Operations Analysis

```sql night_data
  select * from taxi_analytics.night_operations
```

### Night Service Coverage

<div class="grid grid-cols-2 gap-4">
  <BigValue 
    data={night_data} 
    value=night_trip_percentage
    where="service_period='Night Operations (10 PM - 6 AM)'"
    title="Night Trips"
    fmt=num1
    suffix="%"
  />
  
  <div>
    {#each night_data.filter(d => d.service_period === 'Night Operations (10 PM - 6 AM)') as night}
      <div class="p-4 bg-purple-50 rounded-lg">
        <h4 class="font-semibold">{night.service_level}</h4>
        <p class="text-sm text-gray-600 mt-2"><strong>Opportunity:</strong> {night.opportunity}</p>
      </div>
    {/each}
  </div>
</div>

### Day vs Night Distribution

<BarChart
  data={night_data}
  x=service_period
  y=night_trip_percentage
  title="Service Distribution: Day vs Night"
  fmt=num1
  suffix="%"
/>

## 📈 Weekly Performance Trends

```sql weekly_performance
  select 
    pickup_week_start,
    avg_fare_per_trip,
    wow_revenue_growth_pct,
    wow_trips_growth_pct,
    revenue_per_1000_trips
  from taxi_analytics.weekly_trips_revenue
  where wow_revenue_growth_pct is not null
  order by pickup_week_start
```

### Revenue Growth Trends

<LineChart
  data={weekly_performance}
  x=pickup_week_start
  y=wow_revenue_growth_pct
  title="Week-over-Week Revenue Growth"
  yAxisTitle="Growth Rate (%)"
  fmt=num1
  suffix="%"
/>

<LineChart
  data={weekly_performance}
  x=pickup_week_start
  y=avg_fare_per_trip
  title="Average Fare Trends"
  yAxisTitle="Average Fare ($)"
  fmt=usd2
/>

## 🚗 Trip Efficiency Metrics

```sql efficiency_data
  select 
    pickup_week_start,
    minutes_per_mile,
    avg_speed_mph,
    efficiency_score,
    trip_length_category,
    duration_category
  from taxi_analytics.time_vs_distance
  order by pickup_week_start
```

### Speed and Efficiency Trends

<LineChart
  data={efficiency_data}
  x=pickup_week_start
  y=avg_speed_mph
  title="Average Speed Trends"
  yAxisTitle="Speed (mph)"
/>

<ScatterPlot
  data={efficiency_data}
  x=minutes_per_mile
  y=avg_speed_mph
  title="Speed vs Time Efficiency"
  xAxisTitle="Minutes per Mile"
  yAxisTitle="Average Speed (mph)"
/>

### Efficiency Distribution

```sql efficiency_summary
  select 
    trip_length_category,
    count(*) as weeks_count,
    avg(efficiency_score) as avg_efficiency,
    avg(avg_speed_mph) as avg_speed
  from taxi_analytics.time_vs_distance
  group by trip_length_category
  order by avg_efficiency
```

<BarChart
  data={efficiency_summary}
  x=trip_length_category
  y=avg_efficiency
  title="Efficiency Score by Trip Length"
  yAxisTitle="Efficiency Score"
/>

## 🏙️ Borough Performance Deep Dive

```sql borough_performance
  select 
    pickup_borough,
    sum(trip_volume) as total_trips,
    avg(pct_of_weekly_trips) as avg_weekly_share,
    avg(wow_growth_pct) as avg_growth_rate
  from taxi_analytics.weekly_trip_volume
  where wow_growth_pct is not null
  group by pickup_borough
  order by total_trips desc
```

### Borough Market Share & Growth

<BarChart
  data={borough_performance}
  x=pickup_borough
  y=avg_weekly_share
  title="Average Weekly Market Share by Borough"
  yAxisTitle="Market Share (%)"
  fmt=num1
  suffix="%"
/>

<BarChart
  data={borough_performance}
  x=pickup_borough
  y=avg_growth_rate
  title="Average Growth Rate by Borough"
  yAxisTitle="Growth Rate (%)"
  fmt=num1
  suffix="%"
/>

## 📋 Executive Summary

```sql executive_summary
  select 
    'Revenue Efficiency' as metric_category,
    performance_category as status,
    formatted_value as current_value,
    '$' || benchmark_value as benchmark,
    case when vs_benchmark >= 0 then '✅' else '⚠️' end as indicator
  from taxi_analytics.kpi_summary
  where metric = 'avg_revenue_per_mile'
  
  union all
  
  select 
    'Peak Hour Management' as metric_category,
    performance_category,
    formatted_value,
    benchmark_value || '%' as benchmark,
    case when vs_benchmark > -5 and vs_benchmark < 5 then '✅' else '⚠️' end
  from taxi_analytics.kpi_summary
  where metric = 'peak_hour_pct'
  
  union all
  
  select 
    'Night Service Coverage' as metric_category,
    performance_category,
    formatted_value,
    benchmark_value || '%' as benchmark,
    case when vs_benchmark >= 0 then '✅' else '⚠️' end
  from taxi_analytics.kpi_summary
  where metric = 'night_trip_pct'
```

<DataTable 
  data={executive_summary}
  title="Executive KPI Summary"
/>

---

*Deep dive analysis based on enhanced KPI calculations and industry benchmarks.*
