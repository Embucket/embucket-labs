{% macro is_project_part_of_product_ops() %}

    {%- call statement('get_project_ids', fetch_result=True) %}

        SELECT DISTINCT project_id
        FROM {{ref('projects_part_of_product_ops')}}
        WHERE project_id IS NOT NULL

    {%- endcall -%}

    {%- set value_list = load_result('get_project_ids') -%}

    {%- if value_list and value_list['data'] -%}
      {%- set values = value_list['data'] | map(attribute=0) | join(', ') %}
    {%- endif -%}

    {{ return(values) }}

 {% endmacro %}

