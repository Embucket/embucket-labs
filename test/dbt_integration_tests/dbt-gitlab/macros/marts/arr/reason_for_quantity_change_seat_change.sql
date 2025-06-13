{%- macro reason_for_quantity_change_seat_change(quantity, previous_quantity) -%}

    CASE
     WHEN {{ previous_quantity }} != {{ quantity }}
     THEN {{ quantity }} - {{ previous_quantity }}
     ELSE 0
    END                AS seat_change_quantity

{%- endmacro -%}
