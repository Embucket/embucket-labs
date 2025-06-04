{%- macro seat_change(original_seat_quantity, original_unit_of_measure, original_mrr, new_seat_quantity, new_unit_of_measure, new_mrr) -%}

    CASE
        WHEN {{ new_mrr }} = 0 AND {{ original_mrr }} > 0
            THEN 'Cancelled'
        WHEN NOT (       -- Only compare prices per seat when the unit of measure of the original and new plans is seats
            {{ original_unit_of_measure }} = ARRAY_CONSTRUCT('Seats') AND
            {{ new_unit_of_measure }} = ARRAY_CONSTRUCT('Seats')
        )
            THEN 'Not Valid'
        WHEN {{ original_seat_quantity }} = {{ new_seat_quantity }}
            THEN 'Maintained'
        WHEN {{ original_seat_quantity }} > {{ new_seat_quantity }}
            THEN 'Contraction'
        WHEN {{ original_seat_quantity }} < {{ new_seat_quantity }}
            THEN 'Expansion'
        END                                       AS seat_change

{%- endmacro -%}
