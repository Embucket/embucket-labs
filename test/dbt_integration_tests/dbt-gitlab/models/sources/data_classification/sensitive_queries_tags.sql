{{ config({
    "tags": ["data_classification", "mnpi_exception"]
    })
}}

WITH tags AS (

  SELECT tag_name::VARCHAR        AS tag_name,
         tag_value::VARCHAR       AS tag_value,
         tag_schema::VARCHAR      AS tag_schema,
         object_database::VARCHAR AS accessed_database,
         object_schema::VARCHAR   AS accessed_schema,
         object_name::VARCHAR     AS accessed_table,
         column_name::VARCHAR     AS accessed_column
  FROM {{ source('snowflake_account_usage', 'tag_references') }}
  WHERE object_database IN ('RAW', 'PREP', 'PROD')

), mnpi_tags AS (

  SELECT 'MNPI'::VARCHAR   AS classification_type,
         tag_name          AS tag_name,
         tag_value         AS tag_value,
         accessed_database AS accessed_database,
         accessed_schema   AS accessed_schema,
         accessed_table    AS accessed_table,
         NULL              AS accessed_column
  FROM tags
  WHERE tag_name  = 'MNPI_DATA'
  AND tag_value = 'yes'

), pii_tags AS (

SELECT 'PII'::VARCHAR    AS classification_type,
       tag_name          AS tag_name,
       tag_value         AS tag_value,
       accessed_database AS accessed_database,
       accessed_schema   AS accessed_schema,
       accessed_table    AS accessed_table,
       accessed_column   AS accessed_column
  FROM tags
  WHERE tag_schema   = 'CORE'
  AND tag_name IN ('PRIVACY_CATEGORY', 'SEMANTIC_CATEGORY')

), unioned AS (

  SELECT *
  FROM mnpi_tags
  UNION
  SELECT *
  FROM pii_tags

)

  SELECT *
  FROM unioned