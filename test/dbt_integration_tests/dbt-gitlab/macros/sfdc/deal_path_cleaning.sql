{%- macro deal_path_cleaning(column_1) -%}

CASE 
  WHEN {{ column_1 }} = 'Channel' THEN 'Partner'
ELSE {{ column_1 }}
END

{%- endmacro -%}