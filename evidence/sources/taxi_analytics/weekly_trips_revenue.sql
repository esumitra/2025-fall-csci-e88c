-- Total Trips & Total Revenue (Weekly) with derived metrics
select 
  pickup_week_start,
  total_trips,
  total_revenue,
  -- Average fare per trip
  total_revenue / total_trips as avg_fare_per_trip,
  -- Revenue per trip (formatted)
  round(total_revenue / total_trips, 2) as avg_fare_rounded,
  -- Week-over-week revenue growth
  total_revenue - lag(total_revenue) over (order by pickup_week_start) as wow_revenue_change,
  -- Revenue growth rate
  case 
    when lag(total_revenue) over (order by pickup_week_start) > 0 then
      (total_revenue - lag(total_revenue) over (order by pickup_week_start)) * 100.0 / 
      lag(total_revenue) over (order by pickup_week_start)
    else null
  end as wow_revenue_growth_pct,
  -- Trip volume growth
  total_trips - lag(total_trips) over (order by pickup_week_start) as wow_trips_change,
  case 
    when lag(total_trips) over (order by pickup_week_start) > 0 then
      (total_trips - lag(total_trips) over (order by pickup_week_start)) * 100.0 / 
      lag(total_trips) over (order by pickup_week_start)
    else null
  end as wow_trips_growth_pct,
  -- Revenue efficiency (revenue per 1000 trips)
  (total_revenue / total_trips) * 1000 as revenue_per_1000_trips
from weekly_trips_revenue
order by pickup_week_start
