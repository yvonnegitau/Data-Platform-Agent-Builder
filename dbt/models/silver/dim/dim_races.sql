{{config(
    materialized='table', 
    tags=['silver', 'dim'],
    order_by=['season', 'round']
)}}

-- Get race base data
with race_base as (
    select * from {{ ref('stg_races') }}
    where data_quality = 'VALID'
),

-- Calculate race sequence and statistics
race_sequence_stats as (
    select
        season,
        round,
        race_name,
        race_date,
        race_url,
        circuit_id,  -- Keep as foreign key only
        
        -- Race sequence within season
        row_number() over (partition by season order by round) as race_number_in_season,
        count(*) over (partition by season) as total_races_in_season,
        
        -- Calendar information
        extract(month from race_date) as race_month,
        extract(quarter from race_date) as race_quarter,
        extract(dow from race_date) as race_day_of_week,
        extract(week from race_date) as race_week_of_year,
        
        -- Season progression
        round(
            (row_number() over (partition by season order by round)::float / 
             count(*) over (partition by season)) * 100, 1
        ) as season_completion_percentage,
        
        -- Previous/next race timing
        lag(race_date) over (partition by season order by round) as previous_race_date,
        lead(race_date) over (partition by season order by round) as next_race_date,
        
        -- Race gaps
        race_date - lag(race_date) over (partition by season order by round) as days_since_previous_race,
        lead(race_date) over (partition by season order by round) - race_date as days_to_next_race,
        
        extracted_at,
        processed_at

    from race_base
),

-- Final dimension with business logic
final_dim as (
    select
        {{ dbt_utils.generate_surrogate_key(['season', 'round']) }} as dim_race_key,
        season,
        round,
        race_name,
        race_date,
        race_url,
        
        -- Foreign key to circuit dimension (NOT circuit details)
        circuit_id,
        
        -- Race sequence
        race_number_in_season,
        total_races_in_season,
        season_completion_percentage,
        
        -- Calendar attributes
        race_month,
        race_quarter,
        case race_day_of_week
            when 0 then 'Sunday'
            when 1 then 'Monday'
            when 2 then 'Tuesday'
            when 3 then 'Wednesday'
            when 4 then 'Thursday'
            when 5 then 'Friday'
            when 6 then 'Saturday'
        end as race_day_name,
        race_week_of_year,
        
        -- Season context
        case
            when race_number_in_season = 1 then 'Season Opener'
            when race_number_in_season = total_races_in_season then 'Season Finale'
            when season_completion_percentage <= 25 then 'Early Season'
            when season_completion_percentage <= 50 then 'Mid Season First Half'
            when season_completion_percentage <= 75 then 'Mid Season Second Half'
            else 'Late Season'
        end as season_phase,
        
        -- Race timing
        case
            when race_month in (3, 4, 5) then 'Spring'
            when race_month in (6, 7, 8) then 'Summer'
            when race_month in (9, 10, 11) then 'Autumn'
            else 'Winter'
        end as season_period,
        
        -- Race spacing
        days_since_previous_race,
        days_to_next_race,
        case
            when days_since_previous_race is null then 'Season Opener'
            when days_since_previous_race <= 7 then 'Back-to-Back Weekend'
            when days_since_previous_race <= 14 then 'Normal Gap'
            when days_since_previous_race <= 21 then 'Extended Gap'
            when days_since_previous_race <= 35 then 'Long Break'
            else 'Extended Break'
        end as gap_classification,
        
        -- Special race indicators
        case when race_name ilike '%grand prix%' then true else false end as is_grand_prix,
        case when race_name ilike '%sprint%' then true else false end as has_sprint_format,
        case when race_number_in_season = 1 then true else false end as is_season_opener,
        case when race_number_in_season = total_races_in_season then true else false end as is_season_finale,
        
        -- Previous and next race context
        previous_race_date,
        next_race_date,
        
        -- Metadata
        extracted_at,
        processed_at,
        current_timestamp as dim_created_at,
        true as is_current,
        1 as row_version

    from race_sequence_stats
)

select * from final_dim