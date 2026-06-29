{{config(
    materialized='table', 
    tags=['silver', 'dim'],
    order_by=['season', 'natural_key']
)}}

-- Career Stats
with career_stats as (
    select 
    driver_id,
    season,
    count (*) as total_career_races,
    sum(case when position = 1 then 1 else 0 end) as total_career_wins,
    sum(case when position <= 3 and position is not null then 1 else 0 end) as total_career_podiums,
    sum(case when position is not null or race_status ilike '%Finished%' then 1 else 0 end) as total_career_finishes,
    sum(case when position <= 10 then 1 else 0 end) as total_career_points_finishes,
    sum(coalesce(points, 0)) as total_career_points,
    sum(case when race_status not ilike '%Finished%' then 1 else 0 end) as total_career_dnf,

    -- qualifying performance
    avg(coalesce(grid_position, 0)) as avg_grid_position,
    min(grid_position) as best_grid_position

    from {{ ref('stg_results') }}
    where data_quality = 'VALID'
    group by driver_id, season
),
--cumulative stats
cummulative_career_stats as (
    select
    driver_id,
    season,
    sum(total_career_wins) over (partition by driver_id order by season rows between unbounded preceding and 1 preceding) as wins_before_season,
    sum(total_career_podiums) over (partition by driver_id order by season rows between unbounded preceding and 1 preceding) as podiums_before_season,
    sum(total_career_finishes) over (partition by driver_id order by season rows between unbounded preceding and 1 preceding) as finishes_before_season,
    sum(total_career_points) over (partition by driver_id order by season rows between unbounded preceding and 1 preceding) as points_before_season,
    sum(total_career_dnf) over (partition by driver_id order by season rows between unbounded preceding and 1 preceding) as dnf_before_season,
    sum(total_career_races) over (partition by driver_id order by season rows between unbounded preceding and 1 preceding) as races_before_season,
    first_value(season) over (partition by driver_id order by season rows between unbounded preceding and unbounded following) as debut_season
    from career_stats
),
driver_base as (
    select * from {{ ref('stg_drivers') }}
)

select
    {{ dbt_utils.generate_surrogate_key(['d.driver_id','d.season']) }} as dim_driver_key,
    d.driver_id as natural_key,
    d.season,
    d.first_name,
    d.last_name,
    d.full_name,
    d.nationality as driver_nationality,
    d.driver_url,
    d.birth_date,

    -- career stats
    coalesce(cos.races_before_season, 0) as career_races_before_season,
    coalesce(cos.wins_before_season, 0) as career_wins_before_season,
    coalesce(cos.podiums_before_season, 0) as career_podiums_before_season,
    coalesce(cos.finishes_before_season, 0) as career_finishes_before_season,
    coalesce(cos.points_before_season, 0) as career_points_before_season,
    coalesce(cos.dnf_before_season, 0) as career_dnf_before_season,

    -- career milestones
    case when cos.debut_season = d.season then true else false end as is_debut_season,
    d.season - cos.debut_season as seasons_in_f1,

    -- Age calculations
    extract(year from age(current_date,d.birth_date)) as current_age,
    extract(year from age(make_date(d.season,3,1),d.birth_date)) as age_during_season,
    extract(year from age(make_date(cos.debut_season,3,1),d.birth_date)) as age_at_debut,

    -- Driver Classification
    case
        when extract(year from age(make_date(d.season,3,1),d.birth_date)) < 23 then 'Young Driver'
        when extract(year from age(make_date(d.season,3,1),d.birth_date)) between 23 and 30 then 'Experienced Driver'
        when extract(year from age(make_date(d.season,3,1),d.birth_date)) > 30 then 'Veteran Driver'
        else 'Unknown Classification'
    end as driver_classification,

    -- Experince
    case 
        when coalesce(cos.races_before_season, 0) = 0 then 'Rookie'
        when coalesce(cos.races_before_season, 0) < 25 then 'Novice'
        when coalesce(cos.races_before_season, 0) < 75 then 'Experienced'
        when coalesce(cos.races_before_season, 0) < 150 then 'Veteran'
        else 'Legend'
    end as driver_experience,

    -- Success rate calculations
    case
        when coalesce(cos.races_before_season,0) > 0
        then round(((coalesce(cos.wins_before_season, 0)::float / coalesce(cos.races_before_season,0)) * 100)::numeric, 2)
        else 0.0
    end as win_percentage_before_season,
    case
        when coalesce(cos.races_before_season,0) > 0
        then round(((coalesce(cos.podiums_before_season, 0)::float / coalesce(cos.races_before_season,0)) * 100)::numeric, 2)
        else 0.0
    end as podium_percentage_before_season,

    -- Nationality
    case 
        when d.nationality in ('British', 'German', 'Italian', 'Spanish', 'French', 'Dutch', 'Belgian', 'Austrian', 'Swiss', 'Finnish', 'Danish', 'Swedish', 'Norwegian') then 'European'
        when d.nationality in ('American', 'Canadian', 'Mexican', 'Brazilian', 'Argentine', 'Colombian', 'Venezuelan') then 'Americas'
        when d.nationality in ('Japanese', 'Chinese', 'Thai', 'Indonesian', 'Indian', 'Malaysian', 'South Korean') then 'Asian'
        when d.nationality = 'Australian' then 'Oceanian'
        when d.nationality = 'South African' then 'African'
        else 'Other'
    end as nationality_region,

    d.extracted_at,
    d.processed_at,
    current_timestamp as dim_created_at,
    true as is_current,  -- Always true since we rebuild
    1 as row_version  -- Always 1 since we rebuild

from driver_base d
left join cummulative_career_stats cos on d.driver_id = cos.driver_id and d.season = cos.season
