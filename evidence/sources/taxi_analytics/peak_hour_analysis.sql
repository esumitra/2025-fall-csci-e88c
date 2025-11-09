-- Peak-Hour Analysis: Detailed breakdown of 7–9 AM / 5–7 PM trips
-- Note: KPI values are already in percentage format (31.59 = 31.59%)
select 
  'Peak Hours (7-9 AM, 5-7 PM)' as time_period,
  round(value, 2) as peak_hour_percentage,
  round(100 - value, 2) as non_peak_percentage,
  case 
    when value > 35 then 'Very High Peak Concentration - Consider dynamic pricing'
    when value > 30 then 'High Peak Concentration - Monitor capacity'
    when value > 25 then 'Moderate Peak Concentration - Normal operations'
    else 'Low Peak Concentration - May indicate issues'
  end as operational_insight,
  case 
    when value > 30 then 'Increase fleet during peak hours'
    when value < 25 then 'Investigate demand patterns'
    else 'Maintain current operations'
  end as recommendation
from kpi_summary 
where metric = 'peak_hour_pct'

union all

select 
  'Off-Peak Hours' as time_period,
  round(100 - value, 2) as off_peak_percentage,
  round(value, 2) as peak_percentage,
  'Off-peak period represents ' || round(100 - value, 1) || '% of daily trips' as operational_insight,
  'Consider off-peak incentives to balance demand' as recommendation
from kpi_summary 
where metric = 'peak_hour_pct'
