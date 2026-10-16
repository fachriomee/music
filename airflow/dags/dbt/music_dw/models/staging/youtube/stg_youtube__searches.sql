{{ config(
    order_by='song_code',
    comment='Latest YouTube search outcome per catalog song'
) }}
with searches as (

    select
        song_code,
        last_batch_id,
        last_searched_at,
        query_text,
        is_success                                  as success_flag,
        hit_count
    from {{ source('clean', 'youtube_searches') }} final

)

select
    song_code,
    last_batch_id,
    last_searched_at,
    query_text,
    success_flag = 1                                as is_success,
    hit_count
from searches
