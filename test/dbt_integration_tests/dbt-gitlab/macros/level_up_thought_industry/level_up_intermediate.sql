{%- macro level_up_intermediate(source_table, source_schema='level_up') -%}

  WITH
  source AS (
    SELECT *
    FROM {{ source(source_schema, source_table) }}

      {% if is_incremental() %}
      WHERE uploaded_at > (SELECT MAX(uploaded_at) FROM {{ this }})
      {% endif %}
  ),

  intermediate AS (
    SELECT
      data.value,
      source.uploaded_at
    FROM
      source
    INNER JOIN LATERAL FLATTEN(input => source.jsontext['data']) AS data
  ),

{% endmacro %}
