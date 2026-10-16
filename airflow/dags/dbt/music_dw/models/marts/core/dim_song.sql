{{ config(
    order_by='song_code',
    comment='Song = musical composition (catalog grain). Current SCD2 version, one row per song code.'
) }}
select
    song_code,
    song_title,
    original_artist,
    is_artist_known,
    title_base,
    title_core,
    artist_tokens,
    is_generic_title,
    is_title_shared_in_catalog,
    version_no                                  as song_version_no,
    version_valid_from                          as song_version_valid_from
from {{ ref('stg_catalog__songs') }}
