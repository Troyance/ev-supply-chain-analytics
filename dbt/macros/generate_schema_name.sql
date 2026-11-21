{# use the schema name as written (ev_core, ev_marts) instead of dbt's default <target>_<custom> prefix #}
{% macro generate_schema_name(custom_schema_name, node) -%}
    {{ custom_schema_name if custom_schema_name else target.schema }}
{%- endmacro %}
