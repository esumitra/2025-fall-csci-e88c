-- Night Operations Analysis: Detailed night trip breakdown
select 
  'Night Operations (10 PM - 6 AM)' as service_period,
  value as night_trip_percentage,
  100 - value as day_trip_percentage,
  case 
    when value > 20 then 'Excellent 24/7 coverage'
    when value > 15 then 'Good night service availability' 
    when value > 10 then 'Moderate night coverage'
    else 'Limited night operations'
  end as service_level,
  case 
    when value > 20 then 'Night surcharge opportunities'
    when value < 10 then 'Expand night service'
    else 'Maintain current night operations'
  end as opportunity
from kpi_summary 
where metric = 'night_trip_pct'

union all

select 
  'Day Operations (6 AM - 10 PM)' as service_period,
  100 - value as day_percentage,
  value as night_percentage,
  'Day operations represent ' || round(100 - value, 1) || '% of trips',
  case 
    when value < 10 then 'Strong day focus - consider night expansion'
    when value > 25 then 'High night activity - optimize night fleet'
    else 'Balanced day/night operations'
  end as strategy
from kpi_summary 
where metric = 'night_trip_pct'
