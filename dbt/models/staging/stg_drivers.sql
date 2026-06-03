{{
    config(
        indexes = [
            { "columns": ['season', 'driver_id'], 'type': 'btree'}
        ]
    )
}}

with source_data as (
    select
        -- Primary Keys
        year::int as season,
        "driver_id",
        "given_name" as first_name,
        "family_name" as last_name,
        concat("given_name", ' ', "family_name") as full_name,
        nationality,
        date_of_birth::date as birth_date,
        url as driver_url,

        -- Data Quality
        case
            when year is null or "driver_id" is null then 'INVALID'
            when "given_name" is null or "family_name" is null   then 'MISSING_NAME'
            when "date_of_birth" is null then 'MISSING_BIRTH_DATE'
            else 'VALID'
        end as data_quality,

        -- metadata
        date_extracted_at::timestamp as extracted_at,
        CURRENT_TIMESTAMP as processed_at
    from {{ source('f1_bronze', 'drivers') }}
    where year is not null and "driver_id" is not null
),
cleaned_drivers as (
    select *
    from source_data
    where data_quality = 'VALID'
)

select *
from cleaned_drivers