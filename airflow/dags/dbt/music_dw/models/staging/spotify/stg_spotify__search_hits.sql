{{ config(
    order_by='(song_code, spotify_track_id)',
    comment='(song, Spotify track) candidates returned by the latest search of each song'
) }}
{#
  Hits are upserted per (song_code, spotify_track_id). When a newer search no
  longer returns a track, that hit keeps its old search_batch_id - so only the
  hits of the song's latest search batch are kept (no stale candidates).
#}
with hits as (

    select
        song_code,
        spotify_track_id,
        best_search_rank,
        found_by_artist_query                       as artist_query_flag,
        found_by_queries,
        search_batch_id,
        last_seen_at
    from {{ source('clean', 'spotify_search_hits') }} final

),

searches as (

    select song_code, last_batch_id
    from {{ source('clean', 'spotify_searches') }} final

)

select
    h.song_code                                     as song_code,
    h.spotify_track_id                              as spotify_track_id,
    h.best_search_rank                              as best_search_rank,
    h.artist_query_flag = 1                         as found_by_artist_query,
    h.found_by_queries                              as found_by_queries,
    h.last_seen_at                                  as last_seen_at
from hits as h
inner join searches as s
    on s.song_code = h.song_code
   and s.last_batch_id = h.search_batch_id
