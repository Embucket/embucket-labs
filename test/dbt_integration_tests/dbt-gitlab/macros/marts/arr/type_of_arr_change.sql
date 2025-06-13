{%- macro type_of_arr_change(arr, previous_arr, row_number) -%}

   CASE
     WHEN {{ row_number }} = 1
       THEN 'New'
     WHEN {{ arr }} = 0 AND {{ previous_arr }} > 0
       THEN 'Churn'
     WHEN {{ arr }} < {{ previous_arr }} AND {{ arr }} > 0
       THEN 'Contraction'
     WHEN {{ arr }} > {{ previous_arr }} AND {{ row_number }} > 1
       THEN 'Expansion'
     WHEN {{ arr }} = {{ previous_arr }}
       THEN 'No Impact'
     ELSE NULL
   END                 AS type_of_arr_change

{%- endmacro -%}
