{# 0 outside the chip shortage, rising to 1 at the worst point, back to 0 at the end. same curve the generators use #}
{% macro shortage_severity(date_col) -%}
    case
        when {{ date_col }} <= date'{{ var("shortage_start") }}' or {{ date_col }} >= date'{{ var("shortage_end") }}' then 0.0
        when {{ date_col }} <= date'{{ var("shortage_peak") }}'
            then datediff({{ date_col }}, date'{{ var("shortage_start") }}') / datediff(date'{{ var("shortage_peak") }}', date'{{ var("shortage_start") }}')
        else datediff(date'{{ var("shortage_end") }}', {{ date_col }}) / datediff(date'{{ var("shortage_end") }}', date'{{ var("shortage_peak") }}')
    end
{%- endmacro %}
