{{
    config(
        indexes=[
            {
                "columns":['season', 'driver_id']
            }
        ]
    )
}}

with source_data as (
    select 
    driver__driver_id as driver_id,
    season::int as season,
    round::int as round,
    position::int as position,
    position_text,
    points::float as points,
    wins::int as wins,

    -- Data Quality
    case
        when season is null or round is null then 'INVALID'
        when position is null then 'INVALID'
        when driver__driver_id is null then 'MISSING_DRIVER_ID'
        when points is null then 'MISSING_POINTS'
        when cast(position as int) < 1 or cast(position as int) > 20 then 'INVALID'
        when cast(wins as int) < 0 then 'INVALID_WINS'
        else 'VALID'
    end as data_quality,
    case
            when round = (
                select max(r.round) 
                from {{ source('f1_bronze', 'races') }} r
                where r.season = season
            ) then true
            else false
        end as is_final_season_standings,
        

    -- Metadata
    date_extracted_at::timestamp as extracted_at,
    CURRENT_TIMESTAMP as processed_at

    
        from {{ source('f1_bronze', 'driver_standings') }}
        where season is not null and "driver__driver_id" is not null and round is not null

),
cleaned_standings as (
    select *
    from source_data
    where data_quality = 'VALID'    
)
select * from cleaned_standings
