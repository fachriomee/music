-- Orphan check: a recording (ISRC) in the dimension must be linked to at
-- least one catalog song, otherwise it should never have been loaded.
select r.isrc
from {{ ref('dim_recording') }} as r
left anti join {{ ref('bridge_song_recording') }} as b on b.isrc = r.isrc
