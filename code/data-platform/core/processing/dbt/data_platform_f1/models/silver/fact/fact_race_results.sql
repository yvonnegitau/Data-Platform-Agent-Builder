{{ 
    config(
    materialized='table',
    tags=['silver','fact']
)
}}
-- Get the base race results
with base_results as (
    select * from postgres_bronze.f1_bronze_staging.stg_results
    where data_quality = 'VALID'
),

-- Get race information
races_base as (
    select * from {{ ref('dim_races') }}
),

-- Get contructor information
constructors_base as (
    select * from {{ ref('dim_constructor') }}
),

-- Get Driver information
drivers_base as (
    select * from {{ ref('dim_driver') }}
),

-- Status
status_base as (
    select * from {{ ref('dim_status') }}
),

-- Circuit information
circuits_base as (
    select * from {{ ref('dim_circuit') }}
),

base_foundtation as (
    select
        br.*,
        r.dim_race_key as race_key,
        c.dim_constructor_key as constructor_key,
        d.dim_driver_key as driver_key,
        s.dim_status_key as status_key,
        ci.dim_circuit_key as circuit_key

        from base_results br
        inner join races_base r on br.season = r.season and br.round = r.round
        inner join constructors_base c on br.constructor_id = c.natural_key and br.season = c.season
        inner join drivers_base d on br.driver_id = d.natural_key and br.season = d.season
        inner join status_base s on s.status_description = br.race_status
        inner join circuits_base ci on r.circuit_id = ci.natural_key

),
performance_flags as(
    select 
        *,
        case when position = 1 then 1 else 0 end as is_win,
        case when position in (1,2,3) then 1 else 0 end as is_podium,
        case when points > 0 then 1 else 0 end as is_points_finish,
        case when position_text ~ '^[0-9]+$' then 1 else 0 end as is_classified_finish,
        count(*) over (partition by constructor_key, season, round ) as drivers_in_race
    from 
    base_foundtation
        
),
cumulative_metrics as (
    select 
    *,
    sum(points) over (partition by driver_key, season order by season, round) as season_points_running,
    sum(is_win) over (partition by driver_key, season order by season, round) as season_wins_running,
    sum(is_podium) over (partition by driver_key, season order by season, round) as season_podiums_running,
    sum(is_points_finish) over (partition by driver_key, season order by season, round) as season_points_finishes_running,
    sum(is_classified_finish) over (partition by driver_key, season order by season, round) as season_classified_finishes_running,

    -- career metrics
    sum(points) over (partition by driver_key order by season, round) as career_points_running,
    sum(is_win) over (partition by driver_key order by season, round) as career_wins_running,
    sum(is_podium) over (partition by driver_key order by season, round) as career_podiums_running,
    sum(is_points_finish) over (partition by driver_key order by season, round) as career_points_finishes_running,
    sum(is_classified_finish) over (partition by driver_key order by season, round) as career_classified_finishes_running,

    case when is_classified_finish = 1 then grid_position - position else null end as grid_to_finish_diff

    from performance_flags
),
championship_metrics as (
    select 
    *,
    rank() over (partition by season, round order by season_points_running DESC) as championship_position_running,
    (max(season_points_running) over (partition by season, round)) - season_points_running as points_behind_leader_running

    from cumulative_metrics
)

select * from championship_metrics