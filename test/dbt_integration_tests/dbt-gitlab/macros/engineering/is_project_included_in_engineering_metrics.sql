{% macro is_project_included_in_engineering_metrics() %}

    {%- call statement('get_project_ids', fetch_result=True) %}

        SELECT distinct project_id
        FROM {{ref('engineering_productivity_metrics_projects_to_include')}}
        WHERE project_id IS NOT NULL

    {%- endcall -%}

    {%- set value_list = load_result('get_project_ids') -%}

    {%- if value_list and value_list['data'] -%}
      {%- set values = value_list['data'] | map(attribute=0) | join(', ') %}
    {%- endif -%}

    {{ return(values) }}

 {% endmacro %}
