{%- macro case_when_boolean_int(value) -%}

  CASE WHEN {{ value }} > 0 THEN 1 END

{%- endmacro -%}
