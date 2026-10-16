{{ config(
    order_by='song_code',
    comment='Current (open SCD2) version of every catalog song, with the matching keys'
) }}
{#
  Grain   : one row per song_code = the OPEN version of clean.catalog_songs.
  Already : standardised, validated and de-duplicated by pandas (load_catalog);
            rejected rows are in dq.quarantine_records, not here.
  Adds    : the matching keys used by the intermediate models
            (title_norm, title_base, title_core, artist_tokens - macros/normalize.sql).
#}
with current_songs as (

    select
        song_code,
        song_title,
        original_artist,
        source_row_number,
        version_no,
        valid_from,
        batch_id
    from {{ source('clean', 'catalog_songs') }} final
    where is_current = 1
      and is_deleted = 0

)

select
    song_code,
    song_title,
    original_artist,
    {{ normalize_text('song_title') }}                      as title_norm,
    {{ base_title('song_title') }}                          as title_base,
    {{ core_title('song_title') }}                          as title_core,
    original_artist is not null                             as is_artist_known,
    {{ artist_tokens('original_artist') }}                  as artist_tokens,
    -- Short single-word titles ("DOA", "YOU", "KAU") collide with many
    -- unrelated songs; matching asks for stronger evidence for them.
    (lengthUTF8(title_base) <= {{ var('generic_title_max_chars') }}
        or (position(title_base, ' ') = 0
            and lengthUTF8(title_base) <= {{ var('generic_single_word_max_chars') }})
    )                                                       as is_generic_title,
    -- Same title used by several catalog songs (DOA x3, SELAMAT TINGGAL x3 ...)
    count() over (partition by title_base) > 1              as is_title_shared_in_catalog,
    source_row_number,
    version_no,
    valid_from                                              as version_valid_from,
    batch_id                                                as version_batch_id
from current_songs
