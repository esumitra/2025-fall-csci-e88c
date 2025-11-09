-- Revenue Efficiency Analysis: Avg Revenue per Mile detailed breakdown
select 
  'Current Performance' as analysis_type,
  value as revenue_per_mile,
  8.50 as industry_benchmark,
  value - 8.50 as vs_benchmark,
  case 
    when value > 8.50 then 'Above Benchmark (+' || round(value - 8.50, 2) || ')'
    when value < 8.50 then 'Below Benchmark (-' || round(8.50 - value, 2) || ')'
    else 'At Benchmark'
  end as performance_status,
  case 
    when value > 10 then 'Excellent - Premium pricing effective'
    when value > 8.5 then 'Good - Meeting market standards'
    when value > 7 then 'Fair - Room for improvement'
    else 'Poor - Pricing strategy needs review'
  end as assessment
from kpi_summary 
where metric = 'avg_revenue_per_mile'

union all

select 
  'Potential Revenue Impact' as analysis_type,
  value,
  case 
    when value < 8.50 then 8.50
    else value * 1.1  -- 10% improvement target
  end as target_revenue_per_mile,
  case 
    when value < 8.50 then 8.50 - value
    else value * 0.1
  end as improvement_opportunity,
  case 
    when value < 8.50 then 'Reach industry benchmark'
    else 'Achieve 10% improvement'
  end as target_description,
  case 
    when value < 8.50 then 'Focus on route optimization and dynamic pricing'
    else 'Explore premium service offerings'
  end as strategy
from kpi_summary 
where metric = 'avg_revenue_per_mile'
