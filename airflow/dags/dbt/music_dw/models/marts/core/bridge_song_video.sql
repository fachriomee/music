{{ config(
    order_by='(song_code, video_id)',
    comment='Song 1 -> N YouTube videos. Answers "how many videos per song".'
) }}
select
    c.song_code                                     as song_code,
    c.video_id                                      as video_id,
    c.video_type                                    as video_type,
    c.is_official_channel                           as is_official_channel,
    c.artist_in_video                               as artist_in_video,
    c.match_confidence                              as match_confidence,
    c.search_rank                                   as search_rank,
    v.is_licensed_content                           as is_licensed_content,
    v.view_count                                    as view_count,
    count() over (partition by c.video_id) > 1      as is_shared_video
from {{ ref('int_song_video_candidates') }} as c
inner join {{ ref('dim_youtube_video') }} as v on v.video_id = c.video_id
where c.is_accepted
