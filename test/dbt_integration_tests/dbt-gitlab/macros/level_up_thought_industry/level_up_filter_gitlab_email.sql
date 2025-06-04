{%- macro level_up_filter_gitlab_email(column_name) -%}
    CASE
        WHEN LOWER({{ column_name }}) LIKE '%@gitlab.com' THEN {{ column_name }}::VARCHAR
        ELSE NULL
    END
{% endmacro %}
