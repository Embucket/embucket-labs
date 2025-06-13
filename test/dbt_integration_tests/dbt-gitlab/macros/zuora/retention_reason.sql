{%- macro retention_reason(original_mrr, original_product_category, original_product_ranking,
                           original_seat_quantity, new_mrr, new_product_category, new_product_ranking,
                           new_seat_quantity) -%}

	CASE
      WHEN {{ original_mrr }} > {{ new_mrr }} AND
          {{ original_product_category }} = {{ new_product_category }} AND
          {{ original_seat_quantity }} > {{ new_seat_quantity }} AND
          {{ original_mrr }} / {{ original_seat_quantity }} > {{ new_mrr }} / {{ new_seat_quantity }}
        THEN 'Price Change/Seat Change Mix'           

      WHEN  {{ original_mrr }} < {{ new_mrr }} AND
          {{ original_product_category }} = {{ new_product_category }} AND
          {{ original_seat_quantity }} < {{ new_seat_quantity }} AND
          {{ original_mrr }} / {{ original_seat_quantity }} < {{ new_mrr }} / {{ new_seat_quantity }}
        THEN 'Price Change/Seat Change Mix'

      WHEN {{ original_mrr }} > {{ new_mrr }} AND
          {{ original_product_category }} = {{ new_product_category }} AND
          {{ original_seat_quantity }} > {{ new_seat_quantity }}
        THEN 'Seat Change'            

      WHEN {{ original_mrr }} < {{ new_mrr }} AND
          {{ original_product_category }} = {{ new_product_category }} AND
          {{ original_seat_quantity }} < {{ new_seat_quantity }}
        THEN 'Seat Change'

      WHEN {{ original_mrr }} > {{ new_mrr }} AND
          ({{ original_product_category }} = {{ new_product_category }} AND
          {{ original_seat_quantity }} <= {{ new_seat_quantity }})
        THEN 'Price Change'           

      WHEN {{ original_mrr }} < {{ new_mrr }} AND
          ({{ original_product_category }} = {{ new_product_category }} AND
          {{ original_seat_quantity }} >= {{ new_seat_quantity }})
        THEN 'Price Change'

      WHEN {{ original_mrr }} > {{ new_mrr }} AND
          {{ original_product_ranking }} < {{ new_product_ranking }} AND 
          {{ original_seat_quantity }} = {{ new_seat_quantity }}
        THEN 'Price Change' 

      WHEN {{ original_mrr }} < {{ new_mrr }} AND
          {{ original_product_ranking }} > {{ new_product_ranking }} AND 
          {{ original_seat_quantity }} = {{ new_seat_quantity }}
        THEN 'Price Change'               

      WHEN {{ original_product_category }} != {{ new_product_category }} AND
          {{ original_seat_quantity }} = {{ new_seat_quantity }}
        THEN 'Product Change'

      WHEN {{ original_product_category }} != {{ new_product_category }} AND
          {{ original_seat_quantity }} != {{ new_seat_quantity }}
        THEN 'Product Change/Seat Change Mix'

      WHEN {{ new_mrr }} = 0 AND {{ original_mrr }} > 0
        THEN 'Cancelled'

      ELSE 'Unknown' 
      END                      AS retention_reason

{%- endmacro -%}
