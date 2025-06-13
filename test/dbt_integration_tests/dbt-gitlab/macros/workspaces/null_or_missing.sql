{%- macro null_or_missing(column, new_column_name) -%}

  IFF( {{ column }} IS NULL OR {{ column }} LIKE 'Missing%', 'Missing {{new_column_name}}', {{ column }} ) AS {{new_column_name}}

{%- endmacro -%}
