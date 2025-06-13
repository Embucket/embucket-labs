{% macro max_date_in_bamboo_analyses() %}

    {%- set max_date_in_bamboo_analyses = "date_trunc('week', dateadd(week, 3, CURRENT_DATE))" -%}

    {{ return(max_date_in_bamboo_analyses) }}

 {% endmacro %}
