{{ config(severity='warn') }}


-- Test: Constructor points cannot be negative
select 
    natural_key,
    season,
    current_season_points,
    career_points_before_season,
    'Negative points' as error_type
from {{ ref('dim_constructor') }}
where current_season_points < 0 
   or career_points_before_season < 0

-- Test: Constructor must have valid season range
union all

select 
    natural_key,
    season,
    null as current_season_points,
    null as career_points_before_season,
    'Invalid season range' as error_type
from {{ ref('dim_constructor') }}
where season < 1950 or season > 2050

-- Test: Constructor experience level must match seasons active (CORRECTED)
union all

select 
    natural_key,
    season,
    null as current_season_points,
    null as career_points_before_season,
    'Experience level mismatch' as error_type
from {{ ref('dim_constructor') }}
where (total_seasons_active = 1 and constructor_experience_level != 'Rookie Constructor')
   or (total_seasons_active between 2 and 3 and constructor_experience_level != 'New Constructor')
   or (total_seasons_active between 4 and 10 and constructor_experience_level != 'Established Constructor')
   or (total_seasons_active between 11 and 20 and constructor_experience_level != 'Veteran Constructor')
   or (total_seasons_active > 20 and constructor_experience_level != 'Legendary Constructor')

-- Test: Current season tier must be logical based on position and wins
union all

select 
    natural_key,
    season,
    current_season_position,
    current_season_wins,
    'Season tier mismatch' as error_type
from {{ ref('dim_constructor') }}
where (current_season_position = 1 and current_season_tier != 'Championship Winner')  -- Only 1st place is champion
   or (current_season_position between 2 and 3 and current_season_tier != 'Podium Constructor')     -- 2nd-3rd place
   or (current_season_position between 4 and 6 and current_season_tier != 'Midfield Constructor')   -- 4th-6th place  
   or (current_season_position between 7 and 10 and current_season_tier != 'Points Scorer')         -- 7th-10th place
   or (current_season_position > 10 and current_season_tier != 'Backmarker')                        -- 11th+ place

-- Test: Debut season logic
union all

select 
    natural_key,
    season,
    null as current_season_points,
    null as career_points_before_season,
    'Debut season logic error' as error_type
from {{ ref('dim_constructor') }}
where (is_debut_season = true and debut_season != season)
   or (is_debut_season = false and debut_season = season)
   or (seasons_since_debut != season - debut_season)

-- Test: Win percentage calculation
union all

select 
    natural_key,
    season,
    null as current_season_points,
    null as career_points_before_season,
    'Win percentage calculation error' as error_type
from {{ ref('dim_constructor') }}
where career_races_before_season > 0 
  and abs(career_win_percentage_before_season - 
          round((career_wins_before_season::float / career_races_before_season) * 100, 2)) > 0.01

-- Test: Position improvement calculation
union all

select 
    natural_key,
    season,
    null as current_season_points,
    null as career_points_before_season,
    'Position improvement calculation error' as error_type
from {{ ref('dim_constructor') }}
where previous_season_position is not null 
  and position_improvement_from_previous != (previous_season_position - current_season_position)

