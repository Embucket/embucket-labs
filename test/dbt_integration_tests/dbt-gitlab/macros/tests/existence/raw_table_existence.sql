{% macro raw_table_existence(schema, table_list) %}

WITH source as (

    SELECT *
    FROM "{{ env_var('SNOWFLAKE_LOAD_DATABASE') }}".information_schema.tables

), counts AS (

    SELECT count(1) as row_count
    FROM source
    WHERE lower(table_schema) = '{{schema|lower}}'
      AND lower(table_name) in (
        {%- for table in table_list -%}
    
        '{{table|lower}}'{% if not loop.last %},{%- endif -%}
    
        {%- endfor -%}
      )  

)

SELECT row_count
FROM counts
where row_count < array_size(array_construct(
        {%- for table in table_list -%}
    
        '{{table|lower}}'{% if not loop.last %},{%- endif -%}
    
        {%- endfor -%}
      ))

{% endmacro %}
