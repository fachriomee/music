-- Reconciliation: the per-song ISRC count in the mart must equal the number
-- of distinct ISRCs in the bridge for that song (no double counting of the
-- same ISRC released on several Spotify tracks).
with bridge as (
    select song_code, uniqExact(isrc) as n from {{ ref('bridge_song_recording') }} group by song_code
)
select m.song_code, m.isrc_count, b.n
from {{ ref('mart_song_isrc_summary') }} as m
inner join bridge as b on b.song_code = m.song_code
where m.isrc_count != b.n
