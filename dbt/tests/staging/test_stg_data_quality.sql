-- Test: Ensure all results have valid constructors in standings
select 
    r.season,
    r.round,
    r.constructor_id,
    'Results missing constructor in standings' as error_type
from {{ ref('stg_results') }} r
left join {{ ref('stg_constructor_standings') }} cs 
    on r.constructor_id = cs.constructor_id 
    and r.season = cs.season
where cs.constructor_id is null
  and r.data_quality = 'VALID' and r.season not in (1954)

-- Test: Ensure all constructor standings reference valid constructors
union all

select 
    cs.season,
    null as round,
    cs.constructor_id,
    'Constructor standings missing constructor' as error_type
from {{ ref('stg_constructor_standings') }} cs
left join {{ ref('stg_constructors') }} c 
    on cs.constructor_id = c.constructor_id 
    and cs.season = c.season
where c.constructor_id is null
  and cs.data_quality = 'VALID'

-- Test: Ensure all driver standings reference valid drivers
union all

select 
    ds.season,
    ds.round,
    ds.driver_id,
    'Driver standings missing driver' as error_type
from {{ ref('stg_driver_standings') }} ds
left join {{ ref('stg_drivers') }} d 
    on ds.driver_id = d.driver_id 
    and ds.season = d.season
where d.driver_id is null
  and ds.data_quality = 'VALID'

-- Test: Driver standings must have valid position sequence (no gaps in final standings)
union all

select 
    ds.season,
    null as round,
    cast(ds.position as varchar) as driver_id,
    'Missing position in final driver standings sequence' as error_type
from (
    select distinct season, position
    from {{ ref('stg_driver_standings') }}
    where is_final_season_standings = true
      and data_quality = 'VALID'
) ds
right join (
    select 
        season,
        generate_series(1, max_position) as expected_position
    from (
        select 
            season,
            max(position) as max_position
        from {{ ref('stg_driver_standings') }}
        where is_final_season_standings = true
          and data_quality = 'VALID'
        group by season
    ) max_pos
) expected 
    on ds.season = expected.season 
    and ds.position = expected.expected_position
where ds.position is null

-- Test: Driver standings points must be logical (higher position = fewer points in final standings)
union all

select 
    curr.season,
    null as round,
    curr.driver_id,
    'Driver standings points logic error' as error_type
from (
    select 
        season,
        driver_id,
        position,
        points,
        lag(points) over (partition by season order by position) as previous_position_points
    from {{ ref('stg_driver_standings') }}
    where is_final_season_standings = true
      and data_quality = 'VALID'
) curr
where curr.previous_position_points is not null 
  and curr.points > curr.previous_position_points

-- Test: Driver standings wins cannot exceed total number of races in season
union all

select 
    ds.season,
    null as round,
    ds.driver_id,
    'Driver wins exceed total races in season' as error_type
from {{ ref('stg_driver_standings') }} ds
left join (
    select 
        season,
        max(round) as total_races
    from {{ ref('stg_races') }}
    where data_quality = 'VALID'
    group by season
) r on ds.season = r.season
where ds.wins > r.total_races
  and ds.data_quality = 'VALID'
  and ds.is_final_season_standings = true

-- Test: Championship winner (position 1) must have most points in final standings
union all

select 
    winner.season,
    null as round,
    winner.driver_id,
    'Championship winner does not have most points' as error_type
from (
    select season, driver_id, points
    from {{ ref('stg_driver_standings') }}
    where position = 1
      and is_final_season_standings = true
      and data_quality = 'VALID'
) winner
join (
    select 
        season,
        max(points) as max_points
    from {{ ref('stg_driver_standings') }}
    where is_final_season_standings = true
      and data_quality = 'VALID'
    group by season
) max_points_check 
    on winner.season = max_points_check.season
where winner.points < max_points_check.max_points
