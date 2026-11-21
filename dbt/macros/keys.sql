{# yyyymmdd integer key for dim_date #}
{% macro date_key(col) -%}
    cast(date_format({{ col }}, 'yyyyMMdd') as int)
{%- endmacro %}
