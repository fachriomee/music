{{ config(comment='Current status of every pipeline task run (ReplacingMergeTree read with FINAL)') }}
{#
  A task killed from outside (marked failed in the UI, worker lost) cannot
  write its final status, so its row would say 'running' forever: past the
  DAG-run timeout (4 h) it is shown as 'abandoned'.
#}
with runs as (

    select
        batch_id,
        task_name,
        status                                                      as logged_status,
        started_at,
        finished_at,
        rows_loaded,
        api_calls,
        api_errors,
        details,
        error_message
    from {{ source('ops', 'pipeline_runs') }} final

)

select
    batch_id,
    task_name,
    multiIf(
        logged_status = 'running' and started_at < now64(3) - toIntervalHour(4), 'abandoned',
        logged_status
    )                                                               as status,
    started_at,
    finished_at,
    dateDiff('second', started_at, ifNull(finished_at, now64(3)))  as duration_seconds,
    rows_loaded,
    api_calls,
    api_errors,
    if(api_calls = 0, 0, api_errors / api_calls)                    as api_error_rate,
    details,
    error_message
from runs
