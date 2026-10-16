{{ config(
    order_by='channel_id',
    comment='YouTube channels that published at least one matched video'
) }}
select
    channel_id,
    argMax(channel_title, video_id)                         as channel_title,
    endsWith({{ normalize_text('channel_title') }}, ' topic') as is_topic_channel,
    uniqExact(video_id)                                     as matched_video_count,
    max(is_official_channel)                                as is_official_for_some_song
from {{ ref('int_song_video_candidates') }}
where is_accepted and channel_id != ''
group by channel_id
