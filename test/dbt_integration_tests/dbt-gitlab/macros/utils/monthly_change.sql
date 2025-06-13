{%- macro monthly_change(column) -%}

    CASE
      WHEN {{ column }} - lag({{ column }}) OVER (PARTITION BY uuid ORDER BY created_at) >= 0
        THEN {{ column }} - lag({{ column }}) OVER (PARTITION BY uuid ORDER BY created_at)
      ELSE {{ column }}
    END AS {{ column }}_change

{%- endmacro -%}
