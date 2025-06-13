{%- macro convert_variant_to_boolean_field(value) -%}

 TRY_TO_BOOLEAN({{ value }}::VARCHAR)

{%- endmacro -%}


