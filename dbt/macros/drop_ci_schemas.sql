{# ci builds into ci_ev_staging / ci_ev_silver / ci_ev_gold. this drops them after the run.
   only works on the ci target, so it can never drop the real schemas #}
{% macro drop_ci_schemas() %}
    {% if target.name != 'ci' %}
        {{ exceptions.raise_compiler_error("drop_ci_schemas only runs on the ci target") }}
    {% endif %}
    {% for schema in ['ci_ev_staging', 'ci_ev_silver', 'ci_ev_gold'] %}
        {% do run_query('drop schema if exists ' ~ target.catalog ~ '.' ~ schema ~ ' cascade') %}
        {% do log('dropped ' ~ schema, info=true) %}
    {% endfor %}
{% endmacro %}
