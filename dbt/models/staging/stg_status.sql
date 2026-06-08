{{ config(
    indexes=[
        {'columns': ['status_id'], 'unique': true}
    ]
)}}

with source_data as (
    select
        -- Primary Keys
        status_id,
        status,
        count as status_count,

        -- Data Quality
        case
            when status_id is null then 'MISSING_STATUS_ID'
            when status is null then 'MISSING_STATUS'
            else 'VALID'
        end as data_quality,

        -- Metadata
        date_extracted_at::timestamp as extracted_at,
        CURRENT_TIMESTAMP as processed_at
    from {{ source('f1_bronze', 'status') }}
    where status_id is not null
),
cleaned_status as (
    select *
    from source_data
    where data_quality = 'VALID'
)
select *
from cleaned_status