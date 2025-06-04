{% macro get_date_pt_id(column) %}

  TO_NUMBER(
    TO_CHAR(
      CONVERT_TIMEZONE('America/Los_Angeles', {{ column }})::DATE
      ,'YYYYMMDD'
      )
    ,'99999999'
    )

{% endmacro %}
