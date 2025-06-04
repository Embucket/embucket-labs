{%- macro omamori_incremental_source(source_table, source_schema='omamori', condition_column='uploaded_at_gcs', unique_key='id') -%}

{% set source_table_name = source_table %}
{% set source_schema_name = source_schema %}
{% set unique_key_column = unique_key %}

{{ config.set('materialized', 'incremental') }}
{{ config.set('unique_key', unique_key_column) }}
{{ config.set('on_schema_change', 'append_new_columns') }}

WITH source AS (
  SELECT
    PARSE_JSON(value) AS json_value,
    -- obtain 'uploaded_at_gcs' from the parquet filename, i.e:
    -- 'entity_data/20230502/20230502T000025-entity_data-from-19700101T000000-until-20230426T193740.parquet'
    TO_TIMESTAMP('20240417T123000', 'YYYYMMDDTHH24MISS') AS uploaded_at_gcs
  FROM
    {{ source(source_schema_name, source_table_name) }}

  {% if is_incremental() %}
    -- Filter only for records from new files based off uploaded_at_gcs
    -- but first filter on the partitioned column (date_part) to speed up query
    WHERE date_part >= (SELECT COALESCE(MAX({{ condition_column }})::DATE, '1970-01-01') AS last_uploaded_at_gcs FROM {{ this }})
      AND uploaded_at_gcs > (SELECT COALESCE(MAX({{ condition_column }}), '1970-01-01') AS last_uploaded_at_gcs FROM {{ this }})
  {% endif %}
),
{%- endmacro -%}
