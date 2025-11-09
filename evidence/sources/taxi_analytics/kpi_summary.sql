-- Enhanced KPI Summary with context and benchmarks
select 
  metric,
  value,
  -- Add context and descriptions
  case 
    when metric = 'peak_hour_pct' then 'Peak-Hour Trip % (7–9 AM / 5–7 PM)'
    when metric = 'avg_revenue_per_mile' then 'Avg Revenue per Mile'
    when metric = 'night_trip_pct' then 'Night Trip %'
    else metric
  end as metric_description,
  -- Add performance indicators
  case 
    when metric = 'peak_hour_pct' then 
      case 
        when value > 35 then 'High Peak Concentration'
        when value > 25 then 'Moderate Peak Concentration' 
        else 'Low Peak Concentration'
      end
    when metric = 'avg_revenue_per_mile' then
      case 
        when value > 10 then 'Excellent Revenue Efficiency'
        when value > 7 then 'Good Revenue Efficiency'
        when value > 5 then 'Average Revenue Efficiency'
        else 'Below Average Revenue Efficiency'
      end
    when metric = 'night_trip_pct' then
      case 
        when value > 20 then 'High Night Service'
        when value > 10 then 'Moderate Night Service'
        else 'Limited Night Service'
      end
    else 'Unknown'
  end as performance_category,
  -- Format values for display
  case 
    when metric like '%_pct' then round(value, 1) || '%'
    when metric = 'avg_revenue_per_mile' then '$' || round(value, 2)
    else round(value, 2)::text
  end as formatted_value,
  -- Industry benchmarks (hypothetical)
  case 
    when metric = 'peak_hour_pct' then 30.0
    when metric = 'avg_revenue_per_mile' then 8.5
    when metric = 'night_trip_pct' then 15.0
    else null
  end as benchmark_value,
  -- Performance vs benchmark
  case 
    when metric = 'peak_hour_pct' then value - 30.0
    when metric = 'avg_revenue_per_mile' then value - 8.5
    when metric = 'night_trip_pct' then value - 15.0
    else null
  end as vs_benchmark
from kpi_summary
order by 
  case 
    when metric = 'peak_hour_pct' then 1
    when metric = 'avg_revenue_per_mile' then 2
    when metric = 'night_trip_pct' then 3
    else 4
  end
