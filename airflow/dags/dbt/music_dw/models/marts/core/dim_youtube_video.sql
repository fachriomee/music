{{ config(
    order_by='video_id',
    comment='YouTube videos matched to at least one catalog song (current SCD2 version, one row per videoId)'
) }}
with matched as (

    select
        video_id,
        any(video_type)             as video_type
    from {{ ref('int_song_video_candidates') }}
    where is_accepted
    group by video_id

)

select
    v.video_id                                  as video_id,
    v.video_title                               as video_title,
    v.channel_id                                as channel_id,
    v.channel_title                             as channel_title,
    m.video_type                                as video_type,
    v.published_at                              as published_at,
    v.duration_seconds                          as duration_seconds,
    v.category_id                               as category_id,
    v.is_licensed_content                       as is_licensed_content,
    v.view_count                                as view_count,
    v.like_count                                as like_count,
    v.comment_count                             as comment_count,
    v.stats_fetched_at                          as stats_fetched_at,
    v.version_no                                as video_version_no
from matched as m
inner join {{ ref('stg_youtube__videos') }} as v on v.video_id = m.video_id
