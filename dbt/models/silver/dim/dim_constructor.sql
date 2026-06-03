{{config(
    materialized='table', 
    tags=['silver', 'dim'],
    order_by=['season', 'natural_key'] 
)}}

-- Get constructor base data
with constructor_base as (
    select * from postgres_bronze.f1_bronze_staging.stg_constructors
),

-- Get the constructor standings to get the points
standings_base as (
    select * from postgres_bronze.f1_bronze_staging.stg_constructor_standings
    where data_quality = 'VALID'
),

-- Calculate cumulative stats
cumulative_constructor_stats as (
    select
        constructor_id,
        season,
        constructor_name,
        constructor_url,
        constructor_nationality,
        position,
        points,
        total_wins,
        total_rounds,
        
        -- Cumulative stats before this season
        sum(coalesce(points, 0)) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and 1 preceding
        ) as points_before_season,
        
        sum(coalesce(total_wins, 0)) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and 1 preceding
        ) as total_wins_before_season,
        
        avg(coalesce(position, 0)) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and 1 preceding
        ) as avg_position_before_season,
        
        min(position) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and 1 preceding
        ) as best_position_before_season,
        
        max(position) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and 1 preceding
        ) as worst_position_before_season,
        
        count(*) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and 1 preceding
        ) as total_seasons_competed_before_season,
        
        sum(coalesce(total_rounds, 0)) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and 1 preceding
        ) as total_races_before_season,
        
        -- Previous season stats
        lag(position) over (
            partition by constructor_id 
            order by season
        ) as previous_season_position,
        
        lag(points) over (
            partition by constructor_id 
            order by season
        ) as previous_season_points,
        
        -- Career milestones
        first_value(season) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and unbounded following
        ) as debut_season,
        
        -- Total seasons active (including current)
        count(*) over (
            partition by constructor_id 
            order by season 
            rows between unbounded preceding and current row
        ) as total_seasons_active

    from standings_base
),

-- Final dimension with business logic
final_dim as (
    select
        {{ dbt_utils.generate_surrogate_key(['cb.constructor_id', 'ccs.season']) }} as dim_constructor_key,
        cb.constructor_id as natural_key,
        ccs.season,
        cb.constructor_name,
        cb.constructor_url,
        cb.nationality,
        
        -- Current season performance
        ccs.position as current_season_position,
        ccs.points as current_season_points,
        ccs.total_wins as current_season_wins,
        ccs.total_rounds as current_season_rounds,
        
        -- Historical performance before this season
        coalesce(ccs.points_before_season, 0) as career_points_before_season,
        coalesce(ccs.total_wins_before_season, 0) as career_wins_before_season,
        coalesce(ccs.total_races_before_season, 0) as career_races_before_season,
        coalesce(ccs.total_seasons_competed_before_season, 0) as seasons_competed_before,
        
        -- Previous season comparison
        ccs.previous_season_position,
        ccs.previous_season_points,
        case 
            when ccs.previous_season_position is not null 
            then ccs.previous_season_position - ccs.position 
            else null 
        end as position_improvement_from_previous,
        
        -- Career milestones
        ccs.debut_season,
        ccs.total_seasons_active,
        case when ccs.debut_season = ccs.season then true else false end as is_debut_season,
        ccs.season - ccs.debut_season as seasons_since_debut,
        
        -- Performance averages
        case 
            when ccs.total_seasons_competed_before_season > 0 
            then round(ccs.avg_position_before_season, 2) 
            else null 
        end as avg_position_before_season,
        ccs.best_position_before_season,
        ccs.worst_position_before_season,
        
        -- Constructor classification
        case
            when ccs.total_seasons_active = 1 then 'Rookie Constructor'
            when ccs.total_seasons_active <= 3 then 'New Constructor'
            when ccs.total_seasons_active <= 10 then 'Established Constructor'
            when ccs.total_seasons_active <= 20 then 'Veteran Constructor'
            else 'Legendary Constructor'
        end as constructor_experience_level,
        
        -- Performance tier
        case
            when ccs.position = 1 then 'Championship Winner'        -- Constructor's Champion
            when ccs.position between 2 and 3 then 'Podium Constructor'    -- Strong performers
            when ccs.position between 4 and 6 then 'Midfield Constructor'  -- Competitive midfield  
            when ccs.position between 7 and 10 then 'Points Scorer'        -- Regular point scorers
            else 'Backmarker'                                               -- Struggling teams
        end as current_season_tier,
        
        -- Nationality grouping
        case 
            when cb.nationality in ('British', 'German', 'Italian', 'Spanish', 'French', 'Dutch', 'Belgian', 'Austrian', 'Swiss', 'Finnish', 'Danish', 'Swedish', 'Norwegian') then 'European'
            when cb.nationality in ('American', 'Canadian', 'Mexican', 'Brazilian', 'Argentine', 'Colombian', 'Venezuelan') then 'Americas'
            when cb.nationality in ('Japanese', 'Chinese', 'Thai', 'Indonesian', 'Indian', 'Malaysian', 'South Korean') then 'Asian'
            when cb.nationality = 'Australian' then 'Oceanian'
            when cb.nationality = 'South African' then 'African'
            else 'Other'
        end as nationality_region,
        
        -- Success rates
        case 
            when ccs.total_races_before_season > 0 
            then round((ccs.total_wins_before_season::float / ccs.total_races_before_season) * 100, 2)
            else 0.0
        end as career_win_percentage_before_season,
        
        -- Metadata
        cb.extracted_at,
        cb.processed_at,
        current_timestamp as dim_created_at,
        true as is_current,
        1 as row_version

    from constructor_base cb
    inner join cumulative_constructor_stats ccs 
        on cb.constructor_id = ccs.constructor_id and cb.season = ccs.season
)

select * from final_dim
