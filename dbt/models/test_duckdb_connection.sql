{{ config(materialized='table') }}

-- Simple test model to verify DuckDB connection
select 
    'DuckDB Test' as test_message,
    current_timestamp as created_at,
    current_database() as database_name,
    current_schema() as schema_name
