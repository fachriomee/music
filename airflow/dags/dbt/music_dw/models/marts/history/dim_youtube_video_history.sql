{{ config(
    order_by='(video_id, version_no)',
    comment='SCD Type 2 history of YouTube video metadata (title, channel, duration, licensed content ...)'
) }}
{# Statistics (views, likes) are type-1 columns: they are overwritten, not versioned. #}
select
    video_id,
    version_no,
    video_title,
    channel_id,
    channel_title,
    duration_seconds,
    is_licensed_content = 1                     as is_licensed_content,
    valid_from,
    valid_to,
    is_current = 1                              as is_current_version,
    is_deleted = 1                              as is_deleted_at_source,
    batch_id                                    as created_by_run
from {{ source('clean', 'youtube_videos') }} final
