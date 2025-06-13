{%- macro null_negative_numbers(value) -%}

  IFF( {{ value }}::NUMBER < 0, NULL, {{ value }}::NUMBER )

{%- endmacro -%}

