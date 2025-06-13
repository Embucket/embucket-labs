{%- macro current_date_schema(base) -%}

    {% set year = var('year', run_started_at.strftime('%Y')) %}
    {% set month = var('month', run_started_at.strftime('%m')) %}
    {% set date_schema = base + '_' + year|string + '_' + month|string %}

    {{ return(date_schema) }}

{%- endmacro -%}

