select
  business_trip_key,
  source_year_month,
  count(*) as row_count
from {{ ref('fct_taxi_trips') }}
group by 1, 2
having count(*) > 1

