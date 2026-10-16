{{ config(
    order_by='song_code',
    comment='BUSINESS QUESTION 2: how many ISRCs (recordings) does each song have on Spotify?'
) }}
{#
  One row per catalog song (songs with zero matches included, so absence is
  visible). spotify_status explains WHY a count is zero:
    not_searched  the song has not been searched yet
    search_error  every search call for the song failed
    no_match      searched, but no hit passed the matching rules
    matched       at least one accepted recording with a valid ISRC
#}
with songs as (

    select song_code from {{ ref('dim_song') }}

),

searches as (

    select song_code, is_success, last_searched_at, failed_calls
    from {{ ref('stg_spotify__searches') }}

),

recordings as (

    select
        song_code,
        uniqExact(isrc)                                         as isrc_count,
        uniqExactIf(isrc, match_type = 'original_artist')       as isrc_count_original_artist,
        uniqExactIf(isrc, match_type = 'other_artist')          as isrc_count_other_artists,
        uniqExactIf(isrc, match_type = 'unknown_original_artist') as isrc_count_unknown_artist,
        uniqExactIf(isrc, match_confidence = 'high')            as isrc_count_high_confidence,
        uniqExactIf(isrc, version_label = 'original')           as isrc_count_original_version,
        uniqExactIf(isrc, version_label = 'live')               as isrc_count_live,
        uniqExactIf(isrc, version_label = 'acoustic')           as isrc_count_acoustic,
        uniqExactIf(isrc, version_label in ('remaster', 'remix', 'alternate_version', 'edit',
                                            'instrumental', 'karaoke')) as isrc_count_other_versions,
        uniqExact(recording_artist)                             as distinct_recording_artists,
        sum(length(spotify_track_ids))                          as spotify_release_count,
        min(first_release_date)                                 as first_release_date,
        max(is_shared_isrc)                                     as has_shared_isrc
    from {{ ref('bridge_song_recording') }}
    group by song_code

),

quality as (

    select
        song_code,
        countIf(not is_accepted)                                as rejected_hits,
        countIf(is_accepted and not is_valid_isrc)              as accepted_hits_invalid_isrc
    from {{ ref('int_song_recording_candidates') }}
    group by song_code

)

select
    s.song_code                                                 as song_code,
    ifNull(r.isrc_count, 0)                                     as isrc_count,
    ifNull(r.isrc_count_original_artist, 0)                     as isrc_count_original_artist,
    ifNull(r.isrc_count_other_artists, 0)                       as isrc_count_other_artists,
    ifNull(r.isrc_count_unknown_artist, 0)                      as isrc_count_unknown_artist,
    ifNull(r.isrc_count_high_confidence, 0)                     as isrc_count_high_confidence,
    ifNull(r.isrc_count_original_version, 0)                    as isrc_count_original_version,
    ifNull(r.isrc_count_live, 0)                                as isrc_count_live,
    ifNull(r.isrc_count_acoustic, 0)                            as isrc_count_acoustic,
    ifNull(r.isrc_count_other_versions, 0)                      as isrc_count_other_versions,
    ifNull(r.distinct_recording_artists, 0)                     as distinct_recording_artists,
    ifNull(r.spotify_release_count, 0)                          as spotify_release_count,
    r.first_release_date                                        as first_release_date,
    ifNull(r.has_shared_isrc, false)                            as has_shared_isrc,
    ifNull(q.rejected_hits, 0)                                  as rejected_hits,
    ifNull(q.accepted_hits_invalid_isrc, 0)                     as accepted_hits_invalid_isrc,
    se.last_searched_at                                         as spotify_last_searched_at,
    multiIf(
        se.song_code is null,            'not_searched',
        not se.is_success,               'search_error',
        ifNull(r.isrc_count, 0) > 0,     'matched',
        'no_match'
    )                                                           as spotify_status
from songs as s
left join searches   as se on se.song_code = s.song_code
left join recordings as r  on r.song_code  = s.song_code
left join quality    as q  on q.song_code  = s.song_code
