WITH source AS (
  SELECT *
  FROM {{ ref('handbook_file_classification_mapping') }}
)

SELECT *
FROM source
