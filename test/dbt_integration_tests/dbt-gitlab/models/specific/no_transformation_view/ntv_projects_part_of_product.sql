WITH source AS (
  SELECT * 
  FROM {{ ref('projects_part_of_product') }}
)

SELECT *
FROM source