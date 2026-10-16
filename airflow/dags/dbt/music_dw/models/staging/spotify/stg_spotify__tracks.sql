{{ config(
    order_by='spotify_track_id',
    comment='Current (open SCD2) version of every Spotify track, with matching keys and version label'
) }}
{#
  Grain : spotify_track_id = a RELEASE of a recording; several track ids can
          share one ISRC (single + album + compilation).
  ISRC  : normalised and validated by pandas; tracks with a missing or
          malformed ISRC were quarantined (dq.quarantine_records) instead.
#}
with tracks as (

    select
        spotify_track_id,
        track_name,
        isrc,
        artist_names,
        artist_ids,
        album_id,
        album_name,
        album_type,
        release_date,
        release_date_precision,
        duration_ms,
        is_explicit                                 as explicit_flag,
        track_number,
        disc_number,
        isrc_source,
        version_no,
        valid_from
    from {{ source('clean', 'spotify_tracks') }} final
    where is_current = 1
      and is_deleted = 0

)

select
    spotify_track_id,
    track_name,
    {{ base_title('track_name') }}                                      as track_title_base,
    {{ core_title('track_name') }}                                      as track_title_core,
    artist_names,
    arrayMap(a -> {{ normalize_text('a') }}, artist_names)              as artist_names_norm,
    artist_ids,
    album_id,
    album_name,
    album_type,
    release_date,
    release_date_precision,
    duration_ms,
    explicit_flag = 1                                                   as is_explicit,
    track_number,
    disc_number,
    isrc,
    isrc_source,
    match(isrc, '{{ isrc_regex() }}')                                   as is_valid_isrc,
    -- Recording version qualifier ("how many live versions?")
    multiIf(
        match(lowerUTF8(track_name), '\\b(karaoke)\\b'),                       'karaoke',
        match(lowerUTF8(track_name), '\\b(instrumental)\\b'),                  'instrumental',
        match(lowerUTF8(track_name), '\\b(live|konser)\\b'),                   'live',
        match(lowerUTF8(track_name), '\\b(acoustic|accoustic|akustik|unplugged)\\b'), 'acoustic',
        match(lowerUTF8(track_name), '\\b(remix|mix)\\b'),                     'remix',
        match(lowerUTF8(track_name), '\\b(remaster|remastered)\\b'),           'remaster',
        match(lowerUTF8(track_name), '\\b(piano|orchestra|orkestra|version|versi|ver)\\b'), 'alternate_version',
        match(lowerUTF8(track_name), '\\b(edit)\\b'),                          'edit',
        'original'
    )                                                                   as version_label,
    version_no,
    valid_from                                                          as version_valid_from
from tracks
