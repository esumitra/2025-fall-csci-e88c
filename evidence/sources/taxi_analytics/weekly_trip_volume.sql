-- Weekly Trip Volume by Borough with additional metrics
select 
  pickup_week_start,
  pickup_borough,
  trip_volume,
  -- Calculate percentage of total trips for that week
  trip_volume * 100.0 / sum(trip_volume) over (partition by pickup_week_start) as pct_of_weekly_trips,
  -- Calculate running total
  sum(trip_volume) over (partition by pickup_borough order by pickup_week_start) as cumulative_trips,
  -- Calculate week-over-week growth
  trip_volume - lag(trip_volume) over (partition by pickup_borough order by pickup_week_start) as wow_change,
  -- Calculate growth rate
  case 
    when lag(trip_volume) over (partition by pickup_borough order by pickup_week_start) > 0 then
      (trip_volume - lag(trip_volume) over (partition by pickup_borough order by pickup_week_start)) * 100.0 / 
      lag(trip_volume) over (partition by pickup_borough order by pickup_week_start)
    else null
  end as wow_growth_pct
from weekly_trip_volume
order by pickup_week_start, pickup_borough
