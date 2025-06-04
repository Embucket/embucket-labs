{%- macro monthly_price_per_seat_change(original_mrr, original_seat_quantity, original_unit_of_measure,
                                new_mrr, new_seat_quantity, new_unit_of_measure) -%}

    CASE 
      WHEN NOT (       -- Only compare prices per seat when the unit of measure of the original and new plans is seats
        {{ original_unit_of_measure }} = ARRAY_CONSTRUCT('Seats') AND
        {{ new_unit_of_measure }} = ARRAY_CONSTRUCT('Seats')
      )
        THEN NULL
      ELSE
      	( {{ new_mrr }} / {{ new_seat_quantity }} ) - ( {{ original_mrr }} / {{ original_seat_quantity }})	
      END AS monthly_price_per_seat_change

{%- endmacro -%}
