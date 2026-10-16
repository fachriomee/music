-- Completeness: every valid catalog song must appear exactly once in the
-- BI-facing overview, matched or not (absence must be visible, not hidden).
select s.song_code
from {{ ref('stg_catalog__songs') }} as s
left anti join {{ ref('mart_song_overview') }} as o on o.song_code = s.song_code
