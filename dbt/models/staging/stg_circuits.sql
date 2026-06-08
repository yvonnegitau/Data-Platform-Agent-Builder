{{
    config(
        indexes=[
            {'columns': ['circuit_id'], 'unique':true}
        ]
    )
}}

with source_data as (
    select
     -- primary key
     "circuit_id" as circuit_id,

     -- circuit information
        "circuit_name" as circuit_name,
        "location__country" as circuit_country,
        "location__lat" as circuit_latitude,
        "location__long" as circuit_longitude,
        "location__locality" as circuit_locality,
        "url" as circuit_url,

        -- data quality
        case 
            when "circuit_id" is null then 'MISSING_CIRCUIT_ID'
            when "circuit_name" is null then 'MISSING_CIRCUIT_NAME'
            when "location__country" is null then 'MISSING_COUNTRY'
            else 'VALID'
        end as data_quality,

        date_extracted_at::timestamp as extracted_at,
        CURRENT_TIMESTAMP as processed_at

        from {{ source('f1_bronze','circuits')}}
        where "circuit_id" is not null
), 

cleaned_circuits as (
    select *
    from source_data
    where data_quality = 'VALID'
)
select *
from cleaned_circuits   