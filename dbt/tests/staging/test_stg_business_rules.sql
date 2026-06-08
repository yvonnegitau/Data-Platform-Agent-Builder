{{ config(severity='warn') }}

-- Test: Ensure total wins doesn't exceed total rounds
select 
    season,
    constructor_id,
    total_wins,
    total_rounds,
    'Wins exceed rounds' as error_type
from {{ ref('stg_constructor_standings') }}
where total_wins > total_rounds
  and data_quality = 'VALID'

-- Test: Ensure no duplicate championship positions per season
union all

select 
    season,
    constructor_id,
    position,
    count(*) as duplicate_count,
    'Duplicate championship position' as error_type
from {{ ref('stg_constructor_standings') }}
where data_quality = 'VALID'
group by season, position, constructor_id
having count(*) > 1

-- Test: Ensure higher positions generally have fewer points (1st place should have more points than 2nd, etc.)
union all

select 
    pp.season,
    pp.constructor_id,
    pp.position,
    pp.points,
    'Points vs position logic error' as error_type
from (
    select 
        season,
        constructor_id,
        position,
        points,
        lag(points) over (partition by season order by position) as previous_position_points
    from {{ ref('stg_constructor_standings') }}
    where data_quality = 'VALID'
) pp
where pp.previous_position_points is not null 
  and pp.points > pp.previous_position_points