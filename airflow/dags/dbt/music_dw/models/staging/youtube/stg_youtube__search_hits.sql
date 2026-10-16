{{ config(
    order_by='(song_code, video_id)',
    comment='(song, YouTube video) candidates returned by the latest search of each song'
) }}
{# Titles were HTML-unescaped and cleaned by pandas (preprocess_youtube). #}
with hits as (

    select
        song_code,
        video_id,
        search_rank,
        video_title,
        channel_id,
        channel_title,
        published_at,
        search_batch_id
    from {{ source('clean', 'youtube_search_hits') }} final

),

searches as (

    select song_code, last_batch_id
    from {{ source('clean', 'youtube_searches') }} final

)

select
    h.song_code                                     as song_code,
    h.video_id                                      as video_id,
    h.search_rank                                   as search_rank,
    h.video_title                                   as video_title,
    h.channel_id                                    as channel_id,
    h.channel_title                                 as channel_title,
    h.published_at                                  as published_at
from hits as h
inner join searches as s
    on s.song_code = h.song_code
   and s.last_batch_id = h.search_batch_id
