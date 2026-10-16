{{ config(comment='History of pipeline DQ checks with a flag for the latest result per check') }}
select
    batch_id,
    checked_at,
    check_name,
    layer,
    severity,
    status,
    metric_value,
    threshold,
    details,
    checked_at = max(checked_at) over (partition by check_name)    as is_latest
from {{ source('ops', 'dq_results') }}
