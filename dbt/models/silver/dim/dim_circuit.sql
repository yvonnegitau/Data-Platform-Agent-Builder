{{config(
    materialized='table', 
    tags=['silver', 'dim'],
)}}


select
    {{ dbt_utils.generate_surrogate_key(['circuit_id']) }} as dim_circuit_key,
    circuit_id as natural_key,
    circuit_name,
    circuit_country,
    circuit_latitude,
    circuit_longitude,
    circuit_locality,
    circuit_url,

    -- ✅ PRIMARY CLASSIFICATION: Race vs Street Circuit
    case 
        -- Street Circuits (Public roads/city centers)
        when circuit_name ilike '%monaco%' then 'Street Circuit'
        when circuit_name ilike '%singapore%' or circuit_name ilike '%marina%' then 'Street Circuit'
        when circuit_name ilike '%baku%' or circuit_name ilike '%azerbaijan%' then 'Street Circuit'
        when circuit_name ilike '%jeddah%' or circuit_name ilike '%saudi%' then 'Street Circuit'
        when circuit_name ilike '%miami%' then 'Street Circuit'
        when circuit_name ilike '%las vegas%' or circuit_name ilike '%vegas%' then 'Street Circuit'
        when circuit_name ilike '%valencia%' then 'Street Circuit'
        when circuit_name ilike '%detroit%' then 'Street Circuit'
        when circuit_name ilike '%phoenix%' then 'Street Circuit'
        when circuit_name ilike '%caesars%' then 'Street Circuit'
        when circuit_name ilike '%dallas%' then 'Street Circuit'
        when circuit_name ilike '%long beach%' then 'Street Circuit'
        when circuit_name ilike '%adelaide%' then 'Street Circuit'
        when circuit_name ilike '%Pedralbes%' then 'Street Circuit'
        when circuit_name ilike '%Boavista%' then 'Street Circuit'
        when circuit_name ilike '%Monsanto%' then 'Street Circuit'
        when circuit_name ilike '%Montjuïc%' then 'Street Circuit'

        -- Semi-Street (Parkland/public roads but more permanent)
        when circuit_name ilike '%albert park%' then 'Street Circuit'  -- Melbourne park roads
        when circuit_name ilike '%montreal%' or circuit_name ilike '%gilles%' then 'Street Circuit'  -- Île Notre-Dame
        
        -- All others are Race Circuits (Purpose-built racing facilities)
        else 'Race Circuit'
    end as circuit_type,

        -- ✅ DETAILED CLASSIFICATION
    case 
        -- Street Circuit Subcategories
        when circuit_name ilike '%monaco%' then 'Classic Street Circuit'
        when circuit_name ilike '%singapore%' then 'Modern Street Circuit'
        when circuit_name ilike '%baku%' then 'Modern Street Circuit'
        when circuit_name ilike '%jeddah%' then 'Modern Street Circuit'
        when circuit_name ilike '%miami%' then 'Modern Street Circuit'
        when circuit_name ilike '%vegas%' then 'Modern Street Circuit'
        when circuit_name ilike '%albert park%' then 'Semi-Street Circuit'
        when circuit_name ilike '%montreal%' then 'Semi-Street Circuit'
        
        -- Race Circuit Subcategories
        when circuit_name ilike '%silverstone%' then 'Classic Race Circuit'
        when circuit_name ilike '%monza%' then 'Classic Race Circuit'
        when circuit_name ilike '%spa%' then 'Classic Race Circuit'
        when circuit_name ilike '%interlagos%' then 'Classic Race Circuit'
        when circuit_name ilike '%suzuka%' then 'Classic Race Circuit'
        when circuit_name ilike '%nurburgring%' then 'Classic Race Circuit'
        when circuit_name ilike '%zandvoort%' then 'Classic Race Circuit'
        when circuit_name ilike '%imola%' then 'Classic Race Circuit'
        
        when circuit_name ilike '%bahrain%' then 'Modern Race Circuit'
        when circuit_name ilike '%shanghai%' then 'Modern Race Circuit'
        when circuit_name ilike '%sepang%' then 'Modern Race Circuit'
        when circuit_name ilike '%istanbul%' then 'Modern Race Circuit'
        when circuit_name ilike '%yas%' or circuit_name ilike '%abu dhabi%' then 'Modern Race Circuit'
        when circuit_name ilike '%korea%' then 'Modern Race Circuit'
        when circuit_name ilike '%buddh%' then 'Modern Race Circuit'
        when circuit_name ilike '%americas%' or circuit_name ilike '%austin%' then 'Modern Race Circuit'
        when circuit_name ilike '%losail%' then 'Modern Race Circuit'
        
        else 'Race Circuit'
    end as circuit_subtype,

    -- ✅ RACING CHARACTERISTICS: What matters for analysis
    case
        when circuit_name ilike '%monaco%' then 'Nearly Impossible'
        when circuit_name ilike '%hungaroring%' then 'Very Difficult'
        when circuit_name ilike '%singapore%' then 'Very Difficult'
        when circuit_name ilike '%zandvoort%' then 'Very Difficult'
        when circuit_name ilike '%barcelona%' then 'Difficult'
        when circuit_name ilike '%suzuka%' then 'Difficult'
        when circuit_name ilike '%miami%' then 'Moderate'
        when circuit_name ilike '%jeddah%' then 'Moderate'            -- DRS zones help
        when circuit_name ilike '%montreal%' then 'Moderate'          -- Wall of Champions
        when circuit_name ilike '%silverstone%' then 'Good'           -- Multiple lines
        when circuit_name ilike '%austin%' then 'Good'                -- DRS + elevation
        when circuit_name ilike '%interlagos%' then 'Good'            -- Multiple lines
        when circuit_name ilike '%baku%' then 'Excellent'             -- Long straight
        when circuit_name ilike '%bahrain%' then 'Excellent'          -- Multiple DRS zones
        when circuit_name ilike '%monza%' then 'Excellent'            -- Slipstream battles
        when circuit_name ilike '%spa%' then 'Excellent'              -- Long Kemmel straight
        else 'Moderate'
    end as overtaking_difficulty,

    -- ✅ SPEED PROFILE
    case
        when circuit_name ilike '%monza%' then 'Very High Speed'
        when circuit_name ilike '%spa%' then 'High Speed'
        when circuit_name ilike '%silverstone%' then 'High Speed'
        when circuit_name ilike '%baku%' then 'Mixed Speed'  -- Slow + very fast sections
        when circuit_name ilike '%jeddah%' then 'High Speed'
        when circuit_name ilike '%bahrain%' then 'Medium Speed'
        when circuit_name ilike '%austin%' then 'Medium Speed'
        when circuit_name ilike '%suzuka%' then 'Medium Speed'
        when circuit_name ilike '%monaco%' then 'Low Speed'
        when circuit_name ilike '%singapore%' then 'Low Speed'
        when circuit_name ilike '%hungaroring%' then 'Low Speed'
        when circuit_name ilike '%miami%' then 'Medium Speed'
        else 'Medium Speed'
    end as speed_profile,

    -- ✅ GEOGRAPHIC REGION
    case
        when circuit_country in ('United Kingdom', 'Germany', 'Italy', 'Spain', 'France', 'Belgium', 'Netherlands', 'Austria', 'Hungary', 'Monaco', 'Portugal', 'Turkey') then 'Europe'
        when circuit_country in ('United States', 'Canada', 'Mexico', 'Brazil', 'Argentina') then 'Americas'
        when circuit_country in ('Japan', 'China', 'Singapore', 'Malaysia', 'South Korea', 'India', 'Bahrain', 'UAE', 'Saudi Arabia', 'Qatar', 'Azerbaijan', 'Russia') then 'Asia-Pacific'
        when circuit_country in ('Australia') then 'Oceania'
        when circuit_country in ('South Africa') then 'Africa'
        else 'Other'
    end as region,

    -- ✅ WEATHER PATTERN (affects strategy)
    case
        when circuit_name ilike '%silverstone%' then 'Rain Prone'
        when circuit_name ilike '%spa%' then 'Rain Prone'
        when circuit_name ilike '%montreal%' then 'Rain Prone'
        when circuit_name ilike '%interlagos%' then 'Rain Prone'
        when circuit_name ilike '%nurburgring%' then 'Rain Prone'
        when circuit_name ilike '%istanbul%' then 'Rain Prone'
        when circuit_name ilike '%bahrain%' then 'Desert/Hot'
        when circuit_name ilike '%abu dhabi%' then 'Desert/Hot'
        when circuit_name ilike '%qatar%' then 'Desert/Hot'
        when circuit_name ilike '%saudi%' then 'Desert/Hot'
        when circuit_name ilike '%singapore%' then 'Tropical/Humid'
        when circuit_name ilike '%sepang%' then 'Tropical/Humid'
        when circuit_name ilike '%suzuka%' then 'Tropical/Humid'
        else 'Temperate'
    end as weather_pattern,

    -- ✅ RACING EXCITEMENT (based on historical data)
    case
        when circuit_name ilike '%bahrain%' then 'High'      -- Consistently great racing
        when circuit_name ilike '%silverstone%' then 'High'  -- Home of F1, great battles
        when circuit_name ilike '%interlagos%' then 'High'   -- Passionate crowds, weather
        when circuit_name ilike '%spa%' then 'High'          -- Classic venue, unpredictable
        when circuit_name ilike '%monza%' then 'High'        -- Slipstream battles
        when circuit_name ilike '%baku%' then 'High'         -- Chaos and excitement
        when circuit_name ilike '%jeddah%' then 'High'       -- Fast and challenging
        when circuit_name ilike '%austin%' then 'Medium-High' -- Good modern racing
        when circuit_name ilike '%montreal%' then 'Medium-High' -- Wall of Champions
        when circuit_name ilike '%suzuka%' then 'Medium-High' -- Technical masterpiece
        when circuit_name ilike '%monaco%' then 'Medium'     -- Prestige over racing
        when circuit_name ilike '%singapore%' then 'Medium-High' -- Night race spectacle
        when circuit_name ilike '%miami%' then 'Medium'      -- New venue
        when circuit_name ilike '%vegas%' then 'Medium'      -- New venue
        when circuit_name ilike '%hungaroring%' then 'Medium' -- Processional
        when circuit_name ilike '%barcelona%' then 'Low'     -- Notoriously difficult for racing
        else 'Medium'
    end as racing_excitement,

    -- ✅ CIRCUIT LENGTH CLASSIFICATION
    case
        when circuit_name ilike '%monaco%' then 'Short' -- 3.337 km
        when circuit_name ilike '%hungaroring%' then 'Short' -- 4.381 km
        when circuit_name ilike '%zandvoort%' then 'Short' -- 4.259 km
        when circuit_name ilike '%red bull ring%' or circuit_name ilike '%a1%' then 'Short' -- 4.318 km
        when circuit_name ilike '%interlagos%' then 'Medium' -- 4.309 km
        when circuit_name ilike '%suzuka%' then 'Medium' -- 5.807 km
        when circuit_name ilike '%silverstone%' then 'Long' -- 5.891 km
        when circuit_name ilike '%spa%' then 'Long' -- 7.004 km
        when circuit_name ilike '%monza%' then 'Medium' -- 5.793 km
        else 'Medium'
    end as track_length_category,

    -- ✅ CORNER COUNT CLASSIFICATION
    case
        when circuit_name ilike '%monaco%' then 'Many Corners' -- 19 corners
        when circuit_name ilike '%hungaroring%' then 'Many Corners' -- 14 corners
        when circuit_name ilike '%singapore%' then 'Many Corners' -- 23 corners
        when circuit_name ilike '%suzuka%' then 'Many Corners' -- 18 corners
        when circuit_name ilike '%monza%' then 'Few Corners' -- 11 corners
        when circuit_name ilike '%spa%' then 'Medium Corners' -- 19 corners but fast
        when circuit_name ilike '%silverstone%' then 'Medium Corners' -- 18 corners
        else 'Medium Corners'
    end as corner_count_category,

    extracted_at,
    processed_at,
    current_timestamp as dim_created_at,
    -- Not true SCD since it is a table
    true as is_current,  -- Always true since we rebuild
    1 as row_version     -- Always 1 since we rebuild

from {{ ref('stg_circuits') }}