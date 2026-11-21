{# use the schema name as written (ev_staging, ev_silver, ev_gold). the ci target prefixes it, so a pull request
   builds into ci_ev_silver etc. and never touches the real tables #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {%- set name = custom_schema_name if custom_schema_name else target.schema -%}
    {{ 'ci_' ~ name if target.name == 'ci' else name }}
{%- endmacro %}
