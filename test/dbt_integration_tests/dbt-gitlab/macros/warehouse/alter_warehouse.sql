{% macro resume_warehouse(run, warehouse) %}
    {% if run == true %}
        alter warehouse {{warehouse}} resume
    {% else %}
        select 1 as test
    {% endif %}
{% endmacro %}

{% macro suspend_warehouse(run, warehouse) %}
    {% if run == true %}
        alter warehouse {{warehouse}} suspend
    {% else %}
        select 1 as test
    {% endif %}
{% endmacro %}
