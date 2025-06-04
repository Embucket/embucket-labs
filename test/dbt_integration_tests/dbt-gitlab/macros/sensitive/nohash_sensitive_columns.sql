{% macro nohash_sensitive_columns(source_table, join_key) %}

    {% set meta_columns = get_meta_columns(source_table, "sensitive") %}

        {%- if config.get("materialized") == "view" and config.get("secure") -%}

            {{ hash_of_column_in_view(join_key) }}

        {%- else -%}
    
            {{ hash_of_column(join_key) }}

        {% endif %}
    
    {% for column in meta_columns %}
    
    {{column|lower}}  {% if not loop.last %} , {% endif %}
    
    {% endfor %}

{% endmacro %}
