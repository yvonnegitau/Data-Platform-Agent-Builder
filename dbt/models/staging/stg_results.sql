{{
    config(
        indexes=[
            { "columns":['season', 'round', 'car_number', 'driver_id','constructor_id'], 'unique': true},
            { "columns":[ 'constructor_id']},
            { "columns":[ 'driver_id'] }
        ]
    )
}}

with source_data as (
    select
        -- Primary Keys
        season::int as season,
        round::int as round,
        "driver__driver_id" as driver_id,
        "constructor__constructor_id" as constructor_id,

        -- Race Result Data
        position::int as position,
        points::float as points,
        grid::int as grid_position,
        laps::int as total_laps,
        "position_text" as position_text,
        number as car_number,
        status as race_status,
        time__time as race_time,
        time__millis as finish_time,

        -- Driver additional information
        -- "driver__code" as driver_code,
        -- "driver__permanent_number" as driver_permanent_number,

        -- -- fastest lap information
        -- "fastest_lap__rank"::int as fastest_lap_rank,
        -- "fastest_lap__lap"::int as fastest_lap_number,
        -- "fastest_lap__time__time"::timestamp as fastest_lap_time,
        -- "fastest_lap__average_speed_speed"::float as fastest_lap_avg_speed,
        -- "fastest_lap__average_speed__units" as fastest_lap_avg_speed_units,

        -- Data Quality
        case
            when season is null or round is null then 'INVALID'
            when "driver__driver_id" is null or "constructor__constructor_id" is null then 'MISSING_IDS'
            when position is null or points is null then 'MISSING_RESULT_DATA'
            else 'VALID'
        end as data_quality,

        -- metadata
        date_extracted_at::timestamp as extracted_at,
        CURRENT_TIMESTAMP as processed_at
    from {{ source('f1_bronze', 'results') }}
    where season is not null and round is not null and "driver__driver_id" is not null
),
cleaned_results as (
    select *
    from source_data
    where data_quality = 'VALID'
)
select *
from cleaned_results
