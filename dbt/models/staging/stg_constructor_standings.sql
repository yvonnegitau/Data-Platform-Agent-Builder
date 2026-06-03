{{
    config(
        indexes=[
            {
                "columns":['season', 'constructor_id'], 'type': 'btree'
            }
        ]
    )
}}
with source_data as (
    select
        -- Primary Keys
        season::int,
        "constructor__constructor_id" as constructor_id ,
        "constructor__name" as constructor_name,
        "constructor__url" as constructor_url,
        "constructor__nationality" as constructor_nationality,
        round::int as total_rounds,
        position::int as position,
        points::float as points,
        wins::int as total_wins,
        -- Data Quality
        case
            when season is null or "constructor__constructor_id" is null or round is null then 'INVALID'
            when "constructor__name" is null then 'MISSING_NAME'
            else 'VALID'
        end as data_quality,
        -- metadata
        date_extracted_at::timestamp as extracted_at,
        CURRENT_TIMESTAMP as processed_at

        from {{ source('f1_bronze', 'constructor_standings') }}
        where season is not null and "constructor__constructor_id" is not null and round is not null
),
cleaned_standings as (
    select *
    from source_data
    where data_quality = 'VALID'    
)
select * from cleaned_standings


