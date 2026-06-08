{{
    config(
        indexes=[
            {
                "columns":['season', 'constructor_id']
            }
        ]
    )
}}
with source_data as (
    select
        --Primary Keys
        year::int as season,
        "constructor_id",
        name as constructor_name,
        nationality,
        url as constructor_url,

        -- Data Quality
        case
            when year is null or "constructor_id" is null then 'INVALID'
            when name is null then 'MISSING_NAME'
            else 'VALID'
        end as data_quality,

        -- metadata
        date_extracted_at::timestamp as extracted_at,
        CURRENT_TIMESTAMP as processed_at
    from {{ source('f1_bronze', 'constructors') }}
    where year is not null and "constructor_id" is not null
),
cleaned_constructors as (
    select *
    from source_data
    where data_quality = 'VALID'    
)

select * from cleaned_constructors