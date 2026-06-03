-- Test: Driver ages must be realistic
select 
    natural_key,
    season,
    age_during_season,
    'Unrealistic driver age' as error_type
from {{ ref('dim_driver') }}
where age_during_season < 15 or age_during_season > 60

-- Test: Circuit coordinates must be valid
union all

select 
    natural_key,
    null as season,
    null as age_during_season,
    'Invalid coordinates' as error_type
from {{ ref('dim_circuit') }}
where (circuit_latitude is not null and (cast(circuit_latitude as float) < -90 or cast(circuit_latitude as float) > 90))
   or (circuit_longitude is not null and (cast(circuit_longitude as float) < -180 or cast(circuit_longitude as float) > 180))

-- Test: Status categories must be complete
union all

select 
    natural_key,
    null as season,
    null as age_during_season,
    'Inconsistent status flags' as error_type
from {{ ref('dim_status') }}
where (status_category = 'Completed' and is_race_completed = false)
   or (status_category in ('Mechanical Failure', 'Accident') and is_race_completed = true)