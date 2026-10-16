{{ config(
    order_by='song_code',
    comment='BUSINESS QUESTION 1: how many videos does each song have on YouTube?'
) }}
{#
  Counts are "videos discoverable in the top-N search results for the song"
  (N = YOUTUBE_MAX_RESULTS). The public Data API cannot enumerate every video
  using a composition; for a publisher the exhaustive source is YouTube
  Content ID (CMS / Reporting API) — see docs §Future improvements.

  youtube_status:
    pending_quota  not searched yet (deferred by the daily quota budget)
    search_error   the search call failed
    no_match       searched, nothing passed the matching rules
    matched        >= 1 accepted video
#}
with songs as (

    select song_code from {{ ref('dim_song') }}

),

searches as (

    select song_code, is_success, last_searched_at, hit_count
    from {{ ref('stg_youtube__searches') }}

),

videos as (

    select
        song_code,
        uniqExact(video_id)                                             as video_count,
        uniqExactIf(video_id, is_official_channel)                      as official_channel_video_count,
        uniqExactIf(video_id, not is_official_channel)                  as ugc_video_count,
        uniqExactIf(video_id, is_licensed_content)                      as licensed_video_count,
        uniqExactIf(video_id, video_type = 'official_mv')               as official_mv_count,
        uniqExactIf(video_id, video_type = 'official_audio')            as official_audio_count,
        uniqExactIf(video_id, video_type = 'lyric')                     as lyric_video_count,
        uniqExactIf(video_id, video_type = 'live')                      as live_video_count,
        uniqExactIf(video_id, video_type = 'cover')                     as cover_video_count,
        uniqExactIf(video_id, video_type = 'karaoke')                   as karaoke_video_count,
        uniqExactIf(video_id, video_type in ('remix', 'instrumental', 'other')) as other_video_count,
        uniqExactIf(video_id, match_confidence = 'high')                as high_confidence_video_count,
        sum(ifNull(view_count, 0))                                      as total_views,
        argMax(video_id, ifNull(view_count, 0))                         as most_viewed_video_id,
        max(is_shared_video)                                            as has_shared_video
    from {{ ref('bridge_song_video') }}
    group by song_code

),

quality as (

    select song_code, countIf(not is_accepted) as rejected_hits
    from {{ ref('int_song_video_candidates') }}
    group by song_code

)

select
    s.song_code                                         as song_code,
    ifNull(v.video_count, 0)                            as video_count,
    ifNull(v.official_channel_video_count, 0)           as official_channel_video_count,
    ifNull(v.ugc_video_count, 0)                        as ugc_video_count,
    ifNull(v.licensed_video_count, 0)                   as licensed_video_count,
    ifNull(v.official_mv_count, 0)                      as official_mv_count,
    ifNull(v.official_audio_count, 0)                   as official_audio_count,
    ifNull(v.lyric_video_count, 0)                      as lyric_video_count,
    ifNull(v.live_video_count, 0)                       as live_video_count,
    ifNull(v.cover_video_count, 0)                      as cover_video_count,
    ifNull(v.karaoke_video_count, 0)                    as karaoke_video_count,
    ifNull(v.other_video_count, 0)                      as other_video_count,
    ifNull(v.high_confidence_video_count, 0)            as high_confidence_video_count,
    ifNull(v.total_views, 0)                            as total_views,
    v.most_viewed_video_id                              as most_viewed_video_id,
    ifNull(v.has_shared_video, false)                   as has_shared_video,
    ifNull(q.rejected_hits, 0)                          as rejected_hits,
    se.last_searched_at                                 as youtube_last_searched_at,
    multiIf(
        se.song_code is null,           'pending_quota',
        not se.is_success,              'search_error',
        ifNull(v.video_count, 0) > 0,   'matched',
        'no_match'
    )                                                   as youtube_status
from songs as s
left join searches as se on se.song_code = s.song_code
left join videos   as v  on v.song_code  = s.song_code
left join quality  as q  on q.song_code  = s.song_code
