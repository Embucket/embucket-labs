{%- macro coalesce_to_infinity(value) -%}

  COALESCE( {{ value }}, '9999-12-31'::TIMESTAMP)

{%- endmacro -%}
