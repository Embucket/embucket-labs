{% macro pct_w_counters(reporting_count, no_reporting_count) -%}

DIV0({{ reporting_count }},({{ reporting_count }}+{{ no_reporting_count }}))

{%- endmacro %}
