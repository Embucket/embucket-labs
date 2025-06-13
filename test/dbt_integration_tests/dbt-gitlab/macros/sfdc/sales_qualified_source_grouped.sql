{%- macro sales_qualified_source_grouped(column_1) -%}

CASE
  WHEN {{ column_1 }} = 'BDR Generated' THEN 'SDR Generated'
  WHEN {{ column_1 }} = 'Channel Generated' THEN 'Partner Generated'
  WHEN {{ column_1 }} LIKE ANY ('Web%', 'Missing%', 'Other') OR {{ column_1 }} IS NULL THEN 'Web Direct Generated'
  ELSE {{ column_1 }}
END

{%- endmacro -%}
