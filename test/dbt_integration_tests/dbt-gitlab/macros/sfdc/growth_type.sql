{%- macro growth_type(order_type, arr_basis) -%}

  CASE
    WHEN {{ order_type }} != '1. New - First Order' AND {{ arr_basis }} = 0 THEN 'Add-On Growth'
    WHEN {{ order_type }} NOT IN ('1. New - First Order', '4. Contraction', '5. Churn - Partial', '6. Churn - Final')  AND {{ arr_basis }} != 0 THEN 'Growth on Renewal'
    WHEN {{ order_type }} IN ('4. Contraction', '5. Churn - Partial') AND {{ arr_basis }} != 0 THEN 'Contraction on Renewal'
    WHEN {{ order_type }} IN ('6. Churn - Final') AND {{ arr_basis }} != 0 THEN 'Lost on Renewal'
    ELSE 'Missing growth_type'
  END

{%- endmacro -%}
