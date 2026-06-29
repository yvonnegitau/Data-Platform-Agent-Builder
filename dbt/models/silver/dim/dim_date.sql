{{config(
    materialized='table', 
    tags=['silver', 'dim', 'reference'],
    order_by=['date_key']
)}}

-- Generate date range covering F1 history (1950 to future)
with date_spine as (
    {{ dbt_utils.date_spine(
        datepart="day",
        start_date="cast('1950-01-01' as date)",
        end_date="cast('2050-12-31' as date)"
    )}}
),

date_calculations as (
    select
        date_day,
        
        -- Date key (YYYYMMDD format for joins)
        cast(to_char(date_day, 'YYYYMMDD') as integer) as date_key,
        
        -- Basic date components
        extract(year from date_day) as year,
        extract(month from date_day) as month,
        extract(day from date_day) as day,
        extract(quarter from date_day) as quarter,
        extract(week from date_day) as week_of_year,
        extract(dow from date_day) as day_of_week, -- 0=Sunday, 6=Saturday
        extract(doy from date_day) as day_of_year,
        
        -- Date formatting (Postgres to_char; FM strips padding)
        to_char(date_day, 'YYYY-MM-DD') as date_iso,
        to_char(date_day, 'FMMonth FMDD, YYYY') as date_formatted,
        to_char(date_day, 'Mon') as month_short_name,
        to_char(date_day, 'FMMonth') as month_full_name,
        to_char(date_day, 'Dy') as day_short_name,
        to_char(date_day, 'FMDay') as day_full_name,
        
        -- Week calculations
        date_trunc('week', date_day)::date as week_start_date,
        (date_trunc('week', date_day) + interval '6 days')::date as week_end_date,
        
        -- Month calculations
        date_trunc('month', date_day)::date as month_start_date,
        (date_trunc('month', date_day) + interval '1 month' - interval '1 day')::date as month_end_date,
        
        -- Quarter calculations
        date_trunc('quarter', date_day)::date as quarter_start_date,
        (date_trunc('quarter', date_day) + interval '3 months' - interval '1 day')::date as quarter_end_date,
        
        -- Year calculations
        date_trunc('year', date_day)::date as year_start_date,
        (date_trunc('year', date_day) + interval '1 year' - interval '1 day')::date as year_end_date

    from date_spine
),

final_dim as (
    select
        {{ dbt_utils.generate_surrogate_key(['date_key']) }} as dim_date_key,
        date_key,
        date_day as calendar_date,
        date_iso,
        date_formatted,
        
        -- Date components
        year,
        month,
        day,
        quarter,
        week_of_year,
        day_of_week,
        day_of_year,
        
        -- Date names
        month_short_name,
        month_full_name,
        day_short_name,
        day_full_name,
        
        -- Week information
        week_start_date,
        week_end_date,
        'Week ' || week_of_year || ' ' || year as week_description,
        
        -- Month information
        month_start_date,
        month_end_date,
        month_full_name || ' ' || year as month_year,
        month_short_name || ' ' || year as month_year_short,
        
        -- Quarter information
        quarter_start_date,
        quarter_end_date,
        'Q' || quarter || ' ' || year as quarter_year,
        
        -- Year information
        year_start_date,
        year_end_date,
        
        -- Business classifications
        case 
            when day_of_week in (0, 6) then true 
            else false 
        end as is_weekend,
        
        case 
            when day_of_week = 0 then true 
            else false 
        end as is_sunday, -- F1 race day
        
        case 
            when day_of_week = 6 then true 
            else false 
        end as is_saturday, -- F1 qualifying day
        
        case 
            when day_of_week = 5 then true 
            else false 
        end as is_friday, -- F1 practice day
        
        -- Seasonal classification
        case 
            when month in (3, 4, 5) then 'Spring'
            when month in (6, 7, 8) then 'Summer'
            when month in (9, 10, 11) then 'Autumn'
            else 'Winter'
        end as season,
        
        case 
            when month in (12, 1, 2) then 'Off-Season'
            when month in (3, 4, 5) then 'Early Season'
            when month in (6, 7, 8) then 'Mid Season'
            when month in (9, 10, 11) then 'Late Season'
        end as f1_season_period,
        
        -- F1 specific classifications
        case 
            when year >= 1950 and year <= 1957 then 'Formula One Early Years'
            when year >= 1958 and year <= 1960 then 'Constructor Championship Era Begins'
            when year >= 1961 and year <= 1965 then '1.5L Formula'
            when year >= 1966 and year <= 1985 then '3.0L Formula'
            when year >= 1986 and year <= 1988 then 'Turbo Era'
            when year >= 1989 and year <= 1994 then 'Naturally Aspirated Era'
            when year >= 1995 and year <= 2005 then 'Grooved Tyre Era'
            when year >= 2006 and year <= 2008 then 'V8 Era Begins'
            when year >= 2009 and year <= 2013 then 'V8 + DRS/KERS Era'
            when year >= 2014 and year <= 2021 then 'Turbo Hybrid Era'
            when year >= 2022 then 'New Regulation Era'
            else 'Future'
        end as f1_era,
        
        -- Decade classification
        (floor(year / 10) * 10) || 's' as decade,
        
        -- Relative date flags
        case when date_day = current_date then true else false end as is_today,
        case when date_day = current_date - 1 then true else false end as is_yesterday,
        case when date_day = current_date + 1 then true else false end as is_tomorrow,
        case when date_day >= current_date - 7 and date_day <= current_date then true else false end as is_last_7_days,
        case when date_day >= current_date - 30 and date_day <= current_date then true else false end as is_last_30_days,
        case when date_day >= current_date - 365 and date_day <= current_date then true else false end as is_last_365_days,
        
        -- Fiscal year (assuming calendar year for F1)
        year as fiscal_year,
        
        -- Calendar utilities
        case when day = 1 then true else false end as is_month_start,
        case when date_day = month_end_date then true else false end as is_month_end,
        case when date_day = quarter_start_date then true else false end as is_quarter_start,
        case when date_day = quarter_end_date then true else false end as is_quarter_end,
        case when date_day = year_start_date then true else false end as is_year_start,
        case when date_day = year_end_date then true else false end as is_year_end,
        
        -- Metadata
        current_timestamp as dim_created_at,
        true as is_current,
        1 as row_version

    from date_calculations
)

select * from final_dim