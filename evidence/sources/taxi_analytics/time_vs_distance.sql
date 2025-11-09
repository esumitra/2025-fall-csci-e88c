-- Avg Trip Time vs Distance with efficiency metrics
select 
  pickup_week_start,
  avg_duration_min,
  avg_distance_miles,
  -- Key efficiency metric: minutes per mile
  avg_duration_min / avg_distance_miles as minutes_per_mile,
  -- Speed in mph (60 minutes / minutes_per_mile)
  60.0 / (avg_duration_min / avg_distance_miles) as avg_speed_mph,
  -- Efficiency score (lower is better - closer to direct travel time)
  case 
    when avg_distance_miles > 0 then
      round((avg_duration_min / avg_distance_miles) / 2.0, 2) -- Assuming 2 min/mile is optimal city driving
    else null
  end as efficiency_score,
  -- Trip length categories
  case 
    when avg_distance_miles < 1.0 then 'Short (< 1 mile)'
    when avg_distance_miles < 3.0 then 'Medium (1-3 miles)'
    when avg_distance_miles < 5.0 then 'Long (3-5 miles)'
    else 'Very Long (5+ miles)'
  end as trip_length_category,
  -- Duration categories  
  case 
    when avg_duration_min < 10 then 'Quick (< 10 min)'
    when avg_duration_min < 20 then 'Standard (10-20 min)'
    when avg_duration_min < 30 then 'Extended (20-30 min)'
    else 'Long (30+ min)'
  end as duration_category,
  -- Week-over-week efficiency changes
  (avg_duration_min / avg_distance_miles) - 
  lag(avg_duration_min / avg_distance_miles) over (order by pickup_week_start) as wow_efficiency_change
from time_vs_distance
where avg_distance_miles > 0  -- Avoid division by zero
order by pickup_week_start
