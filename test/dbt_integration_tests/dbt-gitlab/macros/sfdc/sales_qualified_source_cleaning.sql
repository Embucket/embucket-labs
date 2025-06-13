{%- macro sales_qualified_source_cleaning(column_1) -%}

  CASE {{column_1}}
    WHEN  'BDR Generated'
      THEN 'SDR Generated'
    WHEN 'Channel Generated'
      THEN 'Partner Generated'
    ELSE {{column_1}}
  END

{%- endmacro -%}
