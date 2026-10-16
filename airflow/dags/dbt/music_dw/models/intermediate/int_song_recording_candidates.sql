{{ config(
    order_by='(song_code, spotify_track_id)',
    comment='Every Spotify hit of every song search, scored and classified as a recording of that song or not'
) }}
{#
================================================================================
 Song <-> Recording matching
 ------------------------------------------------------------------------------
 Input : each Spotify track returned by the searches made FOR a catalog song.
 Output: one row per (song_code, spotify_track_id) with a match decision.

 Title evidence (track vs song)
   exact_base : same title after removing version qualifiers
                "Andaikan Kau Datang Kembali (Acoustic Ver.)" == "ANDAIKAN KAU DATANG KEMBALI"
   exact_core : same title after removing ANY bracket/dash suffix
                "Warteg" == "WARTEG (WARGA TEGAR)"
   fuzzy      : edit-distance similarity >= var('fuzzy_title_threshold')
   none       : different song (Spotify search is fuzzy and returns noise)

 Artist evidence
   artist_match : a track artist equals (or ~equals) one of the catalog's
                  ORIGINAL ARTIST tokens.

 Decision (match_type)
   rejected_title_mismatch      title evidence = none
   rejected_other_catalog_song  title shared by several catalog songs and the
                                track artist is the original artist of ANOTHER
                                of them (e.g. "DOA" by UNGU is not a cover of
                                "DOA" by THE HERMANSYAH)
   rejected_weak_fuzzy          fuzzy title without artist confirmation
   rejected_original_not_found  the catalog names the original artist, but no
                                recording by that artist was found on Spotify:
                                same-title tracks by others cannot be verified
                                as covers (real data: "SECRET INVASION" matched
                                26 unrelated tracks, incl. a game soundtrack)
   rejected_predates_original   another artist's track released BEFORE the
                                original artist's first release of the song:
                                a cover cannot predate the original, so it is a
                                different composition sharing a common title
                                ("Bertahan Hidup", "Kemana" ...)
   original_artist              recording by the original artist (incl. live,
                                acoustic, remaster ... versions)
   other_artist                 same song by another artist  => cover / version
   unknown_original_artist      catalog has no original artist; title-only match

 Per the catalog Context sheet: a song (composition) has MANY recordings, and
 covers by other artists are distinct recordings (distinct ISRCs) of the SAME
 song — hence other_artist matches are accepted.
================================================================================
#}
with songs as (

    select * from {{ ref('stg_catalog__songs') }}

),

hits as (

    select * from {{ ref('stg_spotify__search_hits') }}

),

tracks as (

    select * from {{ ref('stg_spotify__tracks') }}

),

-- For titles used by several catalog songs: which song "owns" which artist?
title_owners as (

    select
        title_base,
        groupArray(tuple(song_code, artist_tokens)) as owners
    from songs
    where is_title_shared_in_catalog and is_artist_known
    group by title_base

),

scored as (

    select
        h.song_code                                         as song_code,
        h.spotify_track_id                                  as spotify_track_id,
        h.best_search_rank                                  as best_search_rank,
        h.found_by_artist_query                             as found_by_artist_query,
        t.isrc                                              as isrc,
        t.is_valid_isrc                                     as is_valid_isrc,
        t.track_name                                        as track_name,
        t.artist_names                                      as artist_names,
        t.version_label                                     as version_label,
        t.release_date                                      as release_date,
        t.album_type                                        as album_type,
        s.title_base                                        as song_title_base,
        t.track_title_base                                  as track_title_base,
        s.is_artist_known                                   as is_artist_known,
        s.is_generic_title                                  as is_generic_title,
        s.is_title_shared_in_catalog                        as is_title_shared_in_catalog,
        {{ text_similarity('t.track_title_base', 's.title_base') }} as title_similarity,
        multiIf(
            t.track_title_base = s.title_base,                                  'exact_base',
            t.track_title_core = s.title_core and lengthUTF8(s.title_core) >= 2, 'exact_core',
            title_similarity >= {{ var('fuzzy_title_threshold') }},             'fuzzy',
            'none'
        )                                                   as title_match_type,
        s.is_artist_known
            and arrayExists(
                ta -> arrayExists(
                    ca -> ta = ca or {{ text_similarity('ta', 'ca') }} >= {{ var('fuzzy_artist_threshold') }},
                    s.artist_tokens),
                t.artist_names_norm)                        as artist_match,
        -- catalog songs (same title) whose original artist performs this track
        arrayMap(
            o -> tupleElement(o, 1),
            arrayFilter(
                o -> arrayExists(ta -> has(tupleElement(o, 2), ta), t.artist_names_norm),
                ifNull(ow.owners, [])))                     as owner_song_codes
    from hits as h
    inner join songs  as s  on s.song_code = h.song_code
    inner join tracks as t  on t.spotify_track_id = h.spotify_track_id
    left join title_owners as ow on ow.title_base = s.title_base

),

decided as (

    select
        *,
        multiIf(
            title_match_type = 'none',                                          'rejected_title_mismatch',
            length(owner_song_codes) > 0 and not has(owner_song_codes, song_code), 'rejected_other_catalog_song',
            title_match_type = 'fuzzy' and not artist_match,                    'rejected_weak_fuzzy',
            artist_match,                                                       'original_artist',
            is_artist_known,                                                    'other_artist',
            'unknown_original_artist'
        )                                                   as preliminary_match_type
    from scored

),

-- The original artist's recordings anchor a song on Spotify: covers are only
-- accepted next to them, and never before their first release
anchors as (

    select
        song_code,
        countIf(preliminary_match_type = 'original_artist')                    as original_recordings,
        minIf(release_date, preliminary_match_type = 'original_artist')        as original_first_release_date
    from decided
    group by song_code

)

select
    d.*,
    a.original_first_release_date                           as original_first_release_date,
    ifNull(a.original_recordings, 0) > 0                    as has_original_on_spotify,
    multiIf(
        d.preliminary_match_type = 'other_artist'
            and ifNull(a.original_recordings, 0) = 0,                  'rejected_original_not_found',
        d.preliminary_match_type = 'other_artist'
            and a.original_first_release_date is not null
            and d.release_date is not null
            and d.release_date < a.original_first_release_date,       'rejected_predates_original',
        d.preliminary_match_type
    )                                                       as match_type,
    match_type in ('original_artist', 'other_artist', 'unknown_original_artist') as is_accepted,
    multiIf(
        not is_accepted,                                                    'none',
        d.artist_match and d.title_match_type = 'exact_base',               'high',
        d.title_match_type = 'exact_base' and not d.is_generic_title,       'medium',
        'low'
    )                                                       as match_confidence
from decided as d
left join anchors as a on a.song_code = d.song_code
