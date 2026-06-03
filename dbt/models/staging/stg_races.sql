{{
        config(
            indexes=[
                {"columns":['season','round'], 'type': 'btree'},
                {"columns": ['circuit_id'], 'type':'btree'}
            ])
    }}

with source_data as(
    select

    -- Primary Keys
    season:: int as season,
    round:: int as round,

    -- Race information
    "race_name" as race_name,
    "date":: date as race_date,
    -- "time"::timestamp as race_time,
    url as race_url,

    -- Race Details
    -- "first_practice__date" as first_practice_date,
    -- "second_practice__date" as second_practice_date,
    -- "third_practice__date" as third_practice_date,
    -- "qualifying__date" as qualifying_date,

    -- Circuit information
    "circuit__circuit_id" as circuit_id,

    -- Data Quality
    case
        when season is null or round is null then 'INVALID'
        when "date" is null then 'MISSING_DATE'
        when "circuit__circuit_id" is null then 'MISSING_CIRCUIT_ID'
        else 'VALID'
    end as data_quality,

    "date_extracted_at"::timestamp as extracted_at,
    CURRENT_TIMESTAMP as processed_at

    from {{source('f1_bronze', 'races')}}
    where season is not null and round is not null
),

cleaned_races as (
    select *
    from source_data
    where data_quality = 'VALID'
)

select * from cleaned_races