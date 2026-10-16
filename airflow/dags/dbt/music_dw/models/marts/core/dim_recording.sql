{{ config(
    order_by='isrc',
    comment='Recording = a specific audio version of a song, identified by ISRC (one row per ISRC).'
) }}
{#
  De-duplication on the natural key: several Spotify track ids (single +
  album + compilation) collapse into one ISRC.
    recording_title / artists : from the earliest release (closest to the original)
    first_release_date        : min over releases
#}
select
    isrc,
    argMin(track_name, ifNull(release_date, toDate32('2299-12-31')))       as recording_title,
    argMin(artist_names, ifNull(release_date, toDate32('2299-12-31')))     as recording_artists,
    arrayElement(recording_artists, 1)                                      as primary_artist,
    argMin(version_label, ifNull(release_date, toDate32('2299-12-31')))    as version_label,
    min(release_date)                                                       as first_release_date,
    toUInt32(median(duration_ms))                                           as duration_ms,
    max(is_explicit)                                                        as is_explicit,
    uniqExact(spotify_track_id)                                             as spotify_release_count,
    groupUniqArray(spotify_track_id)                                        as spotify_track_ids,
    groupUniqArray(album_type)                                              as album_types
from {{ ref('dim_spotify_track') }}
group by isrc
