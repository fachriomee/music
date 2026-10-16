{{ config(
    order_by='song_code',
    comment='One wide row per song: catalog attributes + both business answers. Main BI dataset.'
) }}
select
    d.song_code                                 as song_code,
    d.song_title                                as song_title,
    -- Unique display label: several songs share a title (DOA x3, BULAN x2 ...)
    concat(d.song_title, ' · ', toString(d.song_code)) as song_label,
    ifNull(d.original_artist, '(unknown)')      as original_artist,
    d.is_artist_known                           as is_artist_known,
    d.is_generic_title                          as is_generic_title,
    d.is_title_shared_in_catalog                as is_title_shared_in_catalog,
    d.song_version_no                           as song_version_no,
    d.song_version_valid_from                   as song_version_valid_from,

    -- Q2: ISRCs per song (Spotify)
    i.isrc_count                                as isrc_count,
    i.isrc_count_original_artist                as isrc_count_original_artist,
    i.isrc_count_other_artists                  as isrc_count_other_artists,
    i.isrc_count_unknown_artist                 as isrc_count_unknown_artist,
    i.isrc_count_high_confidence                as isrc_count_high_confidence,
    i.isrc_count_live                           as isrc_count_live,
    i.isrc_count_acoustic                       as isrc_count_acoustic,
    i.distinct_recording_artists                as distinct_recording_artists,
    i.spotify_release_count                     as spotify_release_count,
    i.first_release_date                        as first_release_date,
    i.spotify_status                            as spotify_status,

    -- Q1: videos per song (YouTube)
    v.video_count                               as video_count,
    v.official_channel_video_count              as official_channel_video_count,
    v.ugc_video_count                           as ugc_video_count,
    v.licensed_video_count                      as licensed_video_count,
    v.official_mv_count                         as official_mv_count,
    v.official_audio_count                      as official_audio_count,
    v.lyric_video_count                         as lyric_video_count,
    v.live_video_count                          as live_video_count,
    v.cover_video_count                         as cover_video_count,
    v.karaoke_video_count                       as karaoke_video_count,
    v.total_views                               as total_views,
    v.youtube_status                            as youtube_status,

    -- Bucket for distribution charts
    multiIf(i.isrc_count = 0, '0', i.isrc_count = 1, '1', i.isrc_count <= 3, '2-3',
            i.isrc_count <= 6, '4-6', '7+')     as isrc_count_bucket,
    multiIf(v.video_count = 0, '0', v.video_count <= 3, '1-3', v.video_count <= 10, '4-10',
            v.video_count <= 20, '11-20', '21+') as video_count_bucket,
    now64(3)                                    as built_at
from {{ ref('dim_song') }} as d
left join {{ ref('mart_song_isrc_summary') }}  as i on i.song_code = d.song_code
left join {{ ref('mart_song_video_summary') }} as v on v.song_code = d.song_code
