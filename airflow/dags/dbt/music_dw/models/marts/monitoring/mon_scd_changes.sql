{{ config(
    materialized='table',
    order_by='(changed_at, entity)',
    comment='Every change captured by SCD Type 2 (insert / update / delete) across songs, tracks and videos'
) }}
{# A table (not a view): the BI role can read marts but not the clean layer. #}
with versions as (

    select 'catalog_song' as entity, toString(song_code) as record_key, version_no, valid_from, valid_to,
           is_deleted, batch_id
    from {{ source('clean', 'catalog_songs') }} final

    union all

    select 'spotify_track', spotify_track_id, version_no, valid_from, valid_to, is_deleted, batch_id
    from {{ source('clean', 'spotify_tracks') }} final

    union all

    select 'youtube_video', video_id, version_no, valid_from, valid_to, is_deleted, batch_id
    from {{ source('clean', 'youtube_videos') }} final

)

select
    entity,
    record_key,
    version_no,
    valid_from                                      as changed_at,
    if(version_no = 1, 'insert', 'update')          as change_type,
    batch_id
from versions

union all

select
    entity,
    record_key,
    version_no,
    valid_to                                        as changed_at,
    'delete'                                        as change_type,
    batch_id
from versions
where is_deleted = 1
