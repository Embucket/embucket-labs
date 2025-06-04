{% macro get_internal_parent_namespaces() %}

    {%- call statement('get_namespace_ids', fetch_result=True) %}

        SELECT DISTINCT namespace_id
        FROM {{ ref('internal_gitlab_namespaces') }}
        WHERE namespace_id IS NOT NULL

    {%- endcall -%}

    {%- set value_list = load_result('get_namespace_ids') -%}

    {%- if value_list and value_list['data'] -%}
      {%- set values =  value_list['data'] | map(attribute=0) | join(', ')  %}
    {%- endif -%}

    {{ return( '(' ~ values ~ ')' ) }}

 {% endmacro %}