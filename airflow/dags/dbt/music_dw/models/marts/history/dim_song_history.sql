{{ config(
    order_by='(song_code, version_no)',
    comment='SCD Type 2 history of the catalog songs: every version with its validity interval'
) }}
{#
  One row per (song_code, version_no). The open version has
  valid_to = 2299-12-31 23:59:59 and is_current_version = true; a song removed
  from the sheet has its last version closed with is_deleted_at_source = true.
  Built from clean.catalog_songs so analysts never need access to the silver layer.
#}
select
    song_code,
    version_no,
    song_title,
    original_artist,
    valid_from,
    valid_to,
    is_current = 1                              as is_current_version,
    is_deleted = 1                              as is_deleted_at_source,
    batch_id                                    as created_by_run
from {{ source('clean', 'catalog_songs') }} final
