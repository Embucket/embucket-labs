{%- macro rolling_window_delete(timestamp_field, period, window) -%}

DELETE FROM {{ this }} WHERE DATE_TRUNC('{{ period }}', {{ timestamp_field }}) < DATEADD('{{ period }}', -{{ window }}, DATE_TRUNC('{{ period }}',CURRENT_DATE))

{%- endmacro -%}