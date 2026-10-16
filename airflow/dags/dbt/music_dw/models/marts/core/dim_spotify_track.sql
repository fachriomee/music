{{ config(
    order_by='spotify_track_id',
    comment='Spotify track = a RELEASE of a recording (single, album, compilation). Many tracks -> one ISRC.'
) }}
{# Only tracks accepted as a recording of at least one catalog song. #}
select
    t.spotify_track_id                          as spotify_track_id,
    t.isrc                                      as isrc,
    t.track_name                                as track_name,
    t.artist_names                              as artist_names,
    t.artist_ids                                as artist_ids,
    t.album_id                                  as album_id,
    t.album_name                                as album_name,
    t.album_type                                as album_type,
    t.release_date                              as release_date,
    t.release_date_precision                    as release_date_precision,
    t.duration_ms                               as duration_ms,
    t.is_explicit                               as is_explicit,
    t.version_label                             as version_label,
    t.isrc_source                               as isrc_source,
    t.version_no                                as track_version_no
from {{ ref('stg_spotify__tracks') }} as t
where t.is_valid_isrc
  and t.spotify_track_id in (
      select spotify_track_id
      from {{ ref('int_song_recording_candidates') }}
      where is_accepted
  )
