{{ config(
    order_by='(song_code, video_id)',
    comment='Every YouTube hit of every song search, classified (type, officialness) and accepted/rejected'
) }}
{#
================================================================================
 Song <-> YouTube video matching & classification
 ------------------------------------------------------------------------------
 title_in_video   : the song's base title appears as whole words in the video title
 artist_in_video  : an original-artist token appears in the video title or channel
 is_artist_channel: channel IS the artist ("UNGU", "UNGU Official", "UNGU - Topic", "...VEVO")
 video_type       : keyword classification of the (normalised) video title
                    non_music > karaoke > cover > live > lyric > remix >
                    instrumental > official_mv > official_audio > other
 Decision
   rejected_title_not_in_video       title evidence missing (search noise)
   rejected_non_music                reaction / tutorial / chord videos
   rejected_unavailable              no current video details (deleted /
                                     private: quarantined by preprocess_youtube)
   rejected_generic_without_artist   generic title ("DOA") and the known
                                     original artist is not mentioned
   accepted
================================================================================
#}
with songs as (

    select * from {{ ref('stg_catalog__songs') }}

),

hits as (

    select * from {{ ref('stg_youtube__search_hits') }}

),

videos as (

    select * from {{ ref('stg_youtube__videos') }}

),

joined as (

    select
        h.song_code                                             as song_code,
        h.video_id                                              as video_id,
        h.search_rank                                           as search_rank,
        coalesce(v.video_title, h.video_title)                  as video_title,
        coalesce(v.channel_id, h.channel_id)                    as channel_id,
        coalesce(v.channel_title, h.channel_title)              as channel_title,
        v.video_id is not null                                  as has_details,
        s.title_base                                            as song_title_base,
        s.artist_tokens                                         as artist_tokens,
        s.is_artist_known                                       as is_artist_known,
        s.is_generic_title                                      as is_generic_title
    from hits as h
    inner join songs as s on s.song_code = h.song_code
    left join videos as v on v.video_id = h.video_id

),

features as (

    select
        *,
        {{ normalize_text('video_title') }}                     as video_title_norm,
        {{ normalize_text('channel_title') }}                   as channel_title_norm,
        position(concat(' ', video_title_norm, ' '), concat(' ', song_title_base, ' ')) > 0
                                                                as title_in_video,
        is_artist_known and arrayExists(
            a -> position(concat(' ', video_title_norm, ' ', channel_title_norm, ' '), concat(' ', a, ' ')) > 0,
            artist_tokens)                                      as artist_in_video,
        endsWith(channel_title_norm, ' topic')                  as is_topic_channel,
        is_artist_known and arrayExists(
            a -> channel_title_norm in (a, concat(a, ' official'), concat(a, ' topic'),
                                        concat(a, 'vevo'), concat(a, ' vevo'), concat(a, ' official channel')),
            artist_tokens)                                      as is_artist_channel,
        multiIf(
            match(video_title_norm, '\\b(reaction|react|reacts|tutorial|kunci gitar|chord|chords|lesson|review)\\b'), 'non_music',
            match(video_title_norm, '\\bkaraoke\\b'),                                   'karaoke',
            match(video_title_norm, '\\bcover\\b'),                                     'cover',
            match(video_title_norm, '\\b(live|konser|concert)\\b'),                     'live',
            match(video_title_norm, '\\b(lirik|lyric|lyrics)\\b'),                      'lyric',
            match(video_title_norm, '\\b(remix|dj)\\b'),                                'remix',
            match(video_title_norm, '\\binstrumental\\b'),                              'instrumental',
            match(video_title_norm, '\\b(official (music )?video|official mv|music video|mv)\\b'), 'official_mv',
            match(video_title_norm, '\\bofficial audio\\b') or endsWith(channel_title_norm, ' topic'), 'official_audio',
            'other'
        )                                                       as video_type
    from joined

)

select
    *,
    -- Unknown original artist: only auto-generated "<Artist> - Topic" channels
    -- (label-delivered audio) can be called official.
    is_artist_channel or (not is_artist_known and is_topic_channel)   as is_official_channel,
    multiIf(
        not title_in_video,                                         'rejected_title_not_in_video',
        video_type = 'non_music',                                   'rejected_non_music',
        not has_details,                                            'rejected_unavailable',
        is_generic_title and is_artist_known and not artist_in_video, 'rejected_generic_without_artist',
        'accepted'
    )                                                               as match_status,
    match_status = 'accepted'                                       as is_accepted,
    multiIf(
        not is_accepted,         'none',
        artist_in_video,         'high',
        not is_generic_title,    'medium',
        'low'
    )                                                               as match_confidence
from features
