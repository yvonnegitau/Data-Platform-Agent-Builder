{{config(
    materialized='table', 
    tags=['silver', 'dim', 'reference'],
    order_by=['status_id']
)}}

-- Simple status dimension with business categorization
with status_base as (
    select * from postgres_bronze.f1_bronze_staging.stg_status
    where data_quality = 'VALID'
),

final_dim as (
    select
        {{ dbt_utils.generate_surrogate_key(['status_id']) }} as dim_status_key,
        status_id as natural_key,
        status as status_description,
        status_count,
        
        -- Business categorization for analytics
        case 
            when status ilike '%finished%' or status = '+%' then 'Completed'
            when status ilike '%retired%' or status ilike '%engine%' or status ilike '%gearbox%' 
                 or status ilike '%transmission%' or status ilike '%hydraulics%' then 'Mechanical Failure'
            when status ilike '%accident%' or status ilike '%collision%' or status ilike '%spun%' 
                 or status ilike '%crash%' then 'Accident'
            when status ilike '%disqualified%' then 'Disqualified'
            when status ilike '%withdraw%' or status ilike '%did not start%' then 'Did Not Start'
            when status ilike '%lap%' then 'Lapped'
            else 'Other'
        end as status_category,
        
        -- Completion indicator
        case 
            when status ilike '%finished%' or status = '+%' then true 
            else false 
        end as is_race_completed,
        
        -- Points eligibility (simplified)
        case 
            when status ilike '%finished%' or status = '+%' then true 
            else false 
        end as is_points_eligible,
        
        -- Metadata
        extracted_at,
        processed_at,
        current_timestamp as dim_created_at,
        true as is_current,
        1 as row_version

    from status_base
)

select * from final_dim