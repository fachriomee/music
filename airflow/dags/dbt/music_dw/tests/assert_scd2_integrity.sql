-- SCD Type 2 integrity, for songs, tracks and videos:
--   * at most ONE open version per business key
--   * every version ends after it starts (valid_to >= valid_from)
--   * versions are numbered 1..n without gaps
-- Failing rows = keys that break a rule.
with versions as (

    select 'catalog_song' as entity, toString(song_code) as record_key, version_no,
           is_current_version, valid_from, valid_to
    from {{ ref('dim_song_history') }}

    union all

    select 'spotify_track', spotify_track_id, version_no, is_current_version, valid_from, valid_to
    from {{ ref('dim_spotify_track_history') }}

    union all

    select 'youtube_video', video_id, version_no, is_current_version, valid_from, valid_to
    from {{ ref('dim_youtube_video_history') }}

)

select
    entity,
    record_key,
    countIf(is_current_version)                     as open_versions,
    countIf(valid_to < valid_from)                  as inverted_intervals,
    max(version_no)                                 as last_version,
    count()                                         as versions
from versions
group by entity, record_key
having open_versions > 1
    or inverted_intervals > 0
    or last_version != versions
