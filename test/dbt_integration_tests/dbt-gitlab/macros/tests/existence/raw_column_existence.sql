{% macro raw_column_existence(schema, table, column_list) %}

WITH source as (

    SELECT *
    FROM "{{ env_var('SNOWFLAKE_LOAD_DATABASE') }}".information_schema.columns

), counts AS (

    SELECT count(1) as row_count
    FROM source
    WHERE lower(table_schema) = '{{schema|lower}}'
      AND lower(table_name) = '{{table|lower}}'
      AND lower(column_name) in (
        {%- for column in column_list -%}
    
        '{{column|lower}}'{% if not loop.last %},{%- endif -%}
    
        {%- endfor -%}
      )  

)

SELECT row_count
FROM counts
where row_count < array_size(array_construct(
        {%- for column in column_list -%}
    
        '{{column|lower}}'{% if not loop.last %},{%- endif -%}
    
        {%- endfor -%}
      ))

{% endmacro %}
