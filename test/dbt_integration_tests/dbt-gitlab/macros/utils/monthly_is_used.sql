{%- macro monthly_is_used(column) -%}

    {{ column }} AS {{ column }}_is_used

{%- endmacro -%}
