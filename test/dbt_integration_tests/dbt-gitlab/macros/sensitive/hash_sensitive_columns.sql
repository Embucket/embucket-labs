{% macro hash_sensitive_columns(source_table) %}

    {% set meta_columns = get_meta_columns(source_table, "sensitive") %}

    {%- for column in meta_columns %}

        {%- if config.get("materialized") == "view" and config.get("secure") -%}

            {{ hash_of_column_in_view(column) }}

        {%- else -%}
    
            {{ hash_of_column(column) }}

        {% endif %}
    
    {% endfor %}

    {{ dbt_utils.star(from=ref(source_table), except=meta_columns) }}

{% endmacro %}
