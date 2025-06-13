{% macro get_date_id(column) %}

  TO_NUMBER(TO_CHAR({{ column }}::DATE,'YYYYMMDD'),'99999999')

{% endmacro %}