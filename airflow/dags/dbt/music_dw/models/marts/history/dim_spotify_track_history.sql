{{ config(
    order_by='(spotify_track_id, version_no)',
    comment='SCD Type 2 history of Spotify track metadata (title, ISRC, artists, album, release date ...)'
) }}
select
    spotify_track_id,
    version_no,
    track_name,
    isrc,
    artist_names,
    album_name,
    album_type,
    release_date,
    duration_ms,
    valid_from,
    valid_to,
    is_current = 1                              as is_current_version,
    is_deleted = 1                              as is_deleted_at_source,
    batch_id                                    as created_by_run
from {{ source('clean', 'spotify_tracks') }} final
