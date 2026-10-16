{#
================================================================================
 Text normalisation macros (ClickHouse SQL)
 ------------------------------------------------------------------------------
 These are the AUTHORITATIVE matching rules. SECTION 8 of the DAG file
 (airflow/dags/music_catalog_elt.py) mirrors the version-keyword list in Python
 only to build the API search queries.

   clean_text(x)    NFKC-ish clean: drop zero-width chars, collapse spaces
   normalize_text(x) matching key: lower, no accents, punctuation -> space
   base_title(x)    remove VERSION qualifiers  "Lebih Baik (Remastered)" -> "lebih baik"
   core_title(x)    remove ANY bracket/dash suffix "Warteg (Warga Tegar)" -> "warteg"
   normalize_isrc(x) "id-a12-23-00001" -> "IDA122300001"
   text_similarity(a,b) 1 - editDistance/maxLen, in [0,1]
================================================================================
#}

{% macro version_keywords_regex() -%}
feat|ft|featuring|remaster|remastered|live|acoustic|accoustic|akustik|version|ver|versi|edit|remix|mix|instrumental|karaoke|cover|piano|chill|unplugged|demo|mono|stereo|explicit|clean|sped up|slowed|reverb|from|ost|soundtrack|original|orchestra|orkestra
{%- endmacro %}

{% macro clean_text(col) -%}
trimBoth(replaceRegexpAll(
    replaceRegexpAll(ifNull(toString({{ col }}), ''), '[\\x{200B}-\\x{200F}\\x{2060}-\\x{2064}\\x{FEFF}]', ''),
    '\\s+', ' '))
{%- endmacro %}

{% macro normalize_text(col) -%}
trimBoth(replaceRegexpAll(
    replaceRegexpAll(
        lowerUTF8(replaceRegexpAll(normalizeUTF8NFKD({{ clean_text(col) }}), '\\p{Mn}', '')),
        '[^\\p{L}\\p{N}]+', ' '),
    '\\s+', ' '))
{%- endmacro %}

{% macro base_title(col) -%}
{{ normalize_text(
    "replaceRegexpAll(replaceRegexpAll(" ~ clean_text(col)
    ~ ", '(?i)[\\\\(\\\\[][^\\\\)\\\\]]*\\\\b(" ~ version_keywords_regex() ~ ")\\\\b[^\\\\)\\\\]]*[\\\\)\\\\]]', ' ')"
    ~ ", '(?i)\\\\s[-–—]\\\\s.*\\\\b(" ~ version_keywords_regex() ~ ")\\\\b.*$', ' ')"
) }}
{%- endmacro %}

{% macro core_title(col) -%}
{{ normalize_text(
    "replaceRegexpAll(replaceRegexpAll(" ~ clean_text(col)
    ~ ", '[\\\\(\\\\[][^\\\\)\\\\]]*[\\\\)\\\\]]', ' ')"
    ~ ", '\\\\s[-–—]\\\\s.*$', ' ')"
) }}
{%- endmacro %}

{% macro normalize_isrc(col) -%}
upperUTF8(replaceRegexpAll(ifNull({{ col }}, ''), '[^A-Za-z0-9]', ''))
{%- endmacro %}

{% macro isrc_regex() -%}
^[A-Z]{2}[A-Z0-9]{3}[0-9]{7}$
{%- endmacro %}

{% macro text_similarity(a, b) -%}
if(greatest(lengthUTF8({{ a }}), lengthUTF8({{ b }})) = 0, 0,
   1 - editDistanceUTF8({{ a }}, {{ b }}) / greatest(lengthUTF8({{ a }}), lengthUTF8({{ b }})))
{%- endmacro %}

{#  Split a multi-artist string into normalised tokens:
    'ALSA, CHRIS ANDRIAN YANG' -> ['alsa', 'chris andrian yang'] #}
{% macro artist_tokens(col) -%}
arrayDistinct(arrayFilter(t -> t != '', arrayMap(t -> {{ normalize_text('t') }},
    splitByRegexp('(?i)\\s*(?:,|&|\\bfeat\\.?|\\bft\\.?|\\bwith\\b|\\bx\\b)\\s*', {{ clean_text(col) }}))))
{%- endmacro %}
