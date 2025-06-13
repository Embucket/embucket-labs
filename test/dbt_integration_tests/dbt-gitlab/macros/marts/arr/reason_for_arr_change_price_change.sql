{%- macro reason_for_arr_change_price_change(product_category,previous_product_category,quantity,previous_quantity,arr,previous_arr,product_ranking,previous_product_ranking) -%}

ZEROIFNULL(
           CASE
             WHEN {{ previous_product_category }} = {{ product_category }}
               THEN {{ quantity }} * ({{ arr }}/NULLIF({{ quantity }},0) - {{ previous_arr }}/NULLIF({{ previous_quantity }},0))
             WHEN {{ previous_product_category }} != {{ product_category }} AND {{ previous_product_ranking }} = {{ product_ranking }}
               THEN {{ quantity }} * ({{ arr }}/NULLIF({{ quantity }},0) - {{ previous_arr }}/NULLIF({{ previous_quantity }},0))
             ELSE 0
           END
          )               AS price_change_arr

{%- endmacro -%}
