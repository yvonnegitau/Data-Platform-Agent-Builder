{{ config(
    materialized='view',
    schema='f1_silver',
    tags=['silver', 'coverage']
) }}

-- Data-completeness per season: rounds loaded vs rounds in the schedule.
-- Exposed as a view so Metabase and the MCP agent can chart coverage directly.

with schedule as (
    select season, count(*) as rounds_in_schedule
    from {{ ref('dim_races') }}
    group by season
),

loaded as (
    select season, count(distinct round) as rounds_loaded
    from {{ ref('fact_race_results') }}
    group by season
)

select
    s.season,
    coalesce(l.rounds_loaded, 0)                                   as rounds_loaded,
    s.rounds_in_schedule,
    round(coalesce(l.rounds_loaded, 0)::numeric
          / nullif(s.rounds_in_schedule, 0) * 100, 1)             as pct_complete,
    case
        when coalesce(l.rounds_loaded, 0) = s.rounds_in_schedule       then 'Complete'
        when coalesce(l.rounds_loaded, 0) >= s.rounds_in_schedule * 0.5 then 'Partial'
        when coalesce(l.rounds_loaded, 0) > 0                          then 'Minimal'
        else 'Missing'
    end                                                            as status
from schedule s
left join loaded l on s.season = l.season
order by s.season
