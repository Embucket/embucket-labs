{%- macro reason_for_arr_change_tier_change(product_ranking, previous_product_ranking, quantity, previous_quantity, arr, previous_arr) -%}

    CASE
      WHEN {{ previous_product_ranking }} != {{ product_ranking }}
      THEN ZEROIFNULL({{ quantity }} * ({{ arr }}/NULLIF({{ quantity }},0) - {{ previous_arr }}/NULLIF({{ previous_quantity }},0)))
      ELSE 0
    END                   AS tier_change_arr

{%- endmacro -%}
