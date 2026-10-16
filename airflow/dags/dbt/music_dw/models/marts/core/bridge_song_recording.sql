{{ config(
    order_by='(song_code, isrc)',
    comment='Song (composition) 1 -> N Recording (ISRC). Answers "how many ISRCs per song".'
) }}
{#
  Grain: (song_code, isrc). Rolled up from track level: an ISRC found through
  several releases gets the STRONGEST match evidence among them.
  is_shared_isrc: the same ISRC is linked to more than one catalog song
  (possible when the catalog has duplicate titles and the artist could not
  disambiguate) — surfaced, not hidden.
#}
with accepted as (

    select
        song_code,
        assumeNotNull(isrc)                                         as isrc,   -- valid => not null
        match_type,
        match_confidence,
        title_match_type,
        found_by_artist_query,
        best_search_rank,
        spotify_track_id,
        -- smaller = stronger evidence
        multiIf(match_type = 'original_artist', 1, match_type = 'other_artist', 2, 3)   as type_rank,
        multiIf(match_confidence = 'high', 1, match_confidence = 'medium', 2, 3)        as confidence_rank
    from {{ ref('int_song_recording_candidates') }}
    where is_accepted and is_valid_isrc

),

rolled as (

    select
        song_code,
        isrc,
        argMin(match_type, type_rank)                   as match_type,
        argMin(match_confidence, confidence_rank)       as match_confidence,
        argMin(title_match_type, confidence_rank)       as title_match_type,
        max(found_by_artist_query)                      as found_by_artist_query,
        min(best_search_rank)                           as best_search_rank,
        groupUniqArray(spotify_track_id)                as spotify_track_ids
    from accepted
    group by song_code, isrc

)

select
    r.song_code                                         as song_code,
    r.isrc                                              as isrc,
    r.match_type                                        as match_type,
    r.match_type = 'original_artist'                    as is_original_artist,
    r.match_confidence                                  as match_confidence,
    r.title_match_type                                  as title_match_type,
    r.found_by_artist_query                             as found_by_artist_query,
    r.best_search_rank                                  as best_search_rank,
    d.version_label                                     as version_label,
    d.primary_artist                                    as recording_artist,
    d.first_release_date                                as first_release_date,
    r.spotify_track_ids                                 as spotify_track_ids,
    count() over (partition by r.isrc) > 1              as is_shared_isrc
from rolled as r
inner join {{ ref('dim_recording') }} as d on d.isrc = r.isrc
