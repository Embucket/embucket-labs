{% macro dbt_logging_start(message) -%}

    {%- if not execute -%}
        {{ log('Parsing ' + message + '...', info = True) }}
    {% endif %}

{% endmacro %}

{% macro dbt_logging_end(message) -%}

    {%- if not execute -%}
        {{ log('Still working...', info = True) }}
    {% endif %}

{% endmacro %}
