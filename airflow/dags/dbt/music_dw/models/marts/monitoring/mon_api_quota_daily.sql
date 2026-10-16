{{ config(comment='YouTube quota consumption per Pacific-time day and endpoint') }}
select
    quota_date,
    api,
    endpoint,
    sum(units)          as units_used,
    count()             as calls,
    min(recorded_at)    as first_call_at,
    max(recorded_at)    as last_call_at
from {{ source('ops', 'api_quota_usage') }}
group by quota_date, api, endpoint
