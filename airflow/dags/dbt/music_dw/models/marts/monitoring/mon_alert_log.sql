{{ config(comment='E-mail alerts sent (or skipped / failed) by the pipeline') }}
select
    sent_at,
    batch_id,
    alert_type,
    subject,
    recipients,
    status,
    error_message
from {{ source('ops', 'alert_log') }}
