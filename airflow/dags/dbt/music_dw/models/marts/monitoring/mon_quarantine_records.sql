{{ config(comment='Quarantined records (dq.quarantine_records) for the Data Quarantine dashboard') }}
{#
  is_latest_batch = the record was rejected by the most recent pipeline run
  (the latest run that loaded the catalog), so the KPI drops to 0 as soon as
  a run quarantines nothing.
#}
select
    batch_id,
    detected_at,
    toDate(detected_at, 'Asia/Jakarta')                                    as detected_date,
    source,
    entity,
    record_key,
    record_label,
    reject_reason,
    severity,
    record_json,
    batch_id = (
        select argMax(batch_id, started_at)
        from {{ source('ops', 'pipeline_runs') }}
        where task_name = 'load_catalog'
    )                                                                       as is_latest_batch
from {{ source('dq', 'quarantine_records') }} final
