{%- macro convert_variant_to_number_field(value) -%}

 TRY_TO_NUMBER({{ value }}::VARCHAR)

{%- endmacro -%}