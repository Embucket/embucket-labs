{%- macro pad_column(column, string) -%}

INSERT(INSERT({{column}}, 1, 0, '{{string}}'), LEN({{column}})+2, 0, '{{string}}')

{%- endmacro -%}
