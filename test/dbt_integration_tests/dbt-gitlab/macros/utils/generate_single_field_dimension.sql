{% macro generate_single_field_dimension(model_name, id_column, id_column_name, dimension_column, dimension_column_name, where_clause=None) %}

WITH source_data AS (

    SELECT *
    FROM {{ ref(model_name) }}
    WHERE {{ dimension_column }} IS NOT NULL
    {% if where_clause != None %}
      AND {{ where_clause }}
    {% endif %}

), unioned AS (

    SELECT DISTINCT
      {{ dbt_utils.generate_surrogate_key([id_column]) }}  AS {{ id_column_name }},
      {{  dimension_column }}                     AS {{ dimension_column_name }}
    FROM source_data
    UNION ALL
    SELECT
      MD5('-1')                                   AS {{ id_column_name }},
      'Missing {{dimension_column_name}}'       AS {{ dimension_column_name }}

)

{%- endmacro -%}
