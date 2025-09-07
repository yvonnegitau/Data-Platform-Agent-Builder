-- Test: All races must have valid circuits
select 
    r.season,
    r.round,
    r.circuit_id,
    'Missing circuit' as error_type
from {{ ref('dim_races') }} r
left join {{ ref('dim_circuit') }} c on r.circuit_id = c.natural_key
where c.natural_key is null

-- Test: All constructor standings must reference valid constructors
union all

select 
    cs.season,
    cs.constructor_id,
    null as circuit_id,
    'Missing constructor' as error_type
from postgres_bronze.f1_bronze_staging.stg_constructor_standings cs
left join {{ ref('dim_constructor') }} c 
    on cs.constructor_id = c.natural_key 
    and cs.season = c.season
where c.natural_key is null
  and cs.data_quality = 'VALID'

-- Test: Race sequence must be continuous within seasons
union all

select 
    season,
    cast(max(race_number_in_season) as varchar) as round,
    null as circuit_id,
    'Non-continuous race sequence' as error_type
from {{ ref('dim_races') }}
group by season
having count(*) != max(race_number_in_season)