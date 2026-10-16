{#
  By default dbt builds "<target_schema>_<custom_schema>" (e.g. staging_marts).
  We want the layer databases to be named exactly staging / intermediate /
  marts / dq, so the custom schema is used verbatim when provided.
#}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- if custom_schema_name is none -%}
        {{ target.schema }}
    {%- else -%}
        {{ custom_schema_name | trim }}
    {%- endif -%}
{%- endmacro %}
