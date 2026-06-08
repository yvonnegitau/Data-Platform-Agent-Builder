{{ config(severity='warn') }}

-- Test: All races must have valid circuits
select 
    r.season,
    r.round,
    r.circuit_id,
    'Missing circuit' as error_type
from {{ ref('dim_races') }} r
left join {{ ref('dim_circuit') }} c on r.circuit_id = c.natural_key
where c.natural_key is null

-- Test: All race results must have a resolvable constructor key
union all

select
    season,
    cast(round as varchar) as round,
    null as circuit_id,
    'Race result missing constructor key' as error_type
from {{ ref('fact_race_results') }}
where constructor_key is null

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