{{ config(
    order_by='video_id',
    comment='Current (open SCD2) version of every YouTube video, with the latest statistics'
) }}
{#
  Grain: video_id. Durations (ISO-8601) and counts (strings in the API) were
  typed by pandas. licensedContent = the video sits on a channel linked to a
  YouTube content partner and is claimed by it (strong "official" signal).
  Videos that became unavailable are closed (is_deleted = 1) and excluded.
#}
with videos as (

    select
        video_id,
        video_title,
        channel_id,
        channel_title,
        published_at,
        category_id,
        duration_seconds,
        is_licensed_content                         as licensed_flag,
        default_audio_language,
        view_count,
        like_count,
        comment_count,
        stats_fetched_at,
        version_no,
        valid_from
    from {{ source('clean', 'youtube_videos') }} final
    where is_current = 1
      and is_deleted = 0

)

select
    video_id,
    channel_id,
    channel_title,
    video_title,
    published_at,
    category_id,
    default_audio_language,
    duration_seconds,
    licensed_flag = 1                               as is_licensed_content,
    view_count,
    like_count,
    comment_count,
    stats_fetched_at,
    version_no,
    valid_from                                      as version_valid_from
from videos
