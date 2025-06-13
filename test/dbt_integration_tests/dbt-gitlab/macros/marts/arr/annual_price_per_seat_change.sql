{%- macro annual_price_per_seat_change(quantity, previous_quantity, arr, previous_arr) -%}

     ZEROIFNULL(( {{ arr }} / NULLIF({{ quantity }},0) ) - ( {{ previous_arr }} / NULLIF({{ previous_quantity }},0))) AS annual_price_per_seat_change

{%- endmacro -%}
