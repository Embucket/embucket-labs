{%- macro reason_for_arr_change_seat_change(quantity,previous_quantity,arr,previous_arr) -%}

    CASE
      WHEN {{ previous_quantity }} != {{ quantity }} AND {{ previous_quantity }} > 0
        THEN ZEROIFNULL({{ previous_arr }} /NULLIF({{ previous_quantity }},0) * ({{ quantity }} - {{ previous_quantity }}))
      WHEN {{ previous_quantity }} != {{ quantity }} AND {{ previous_quantity }} = 0
        THEN {{ arr }}
      ELSE 0
    END                AS seat_change_arr

{%- endmacro -%}
