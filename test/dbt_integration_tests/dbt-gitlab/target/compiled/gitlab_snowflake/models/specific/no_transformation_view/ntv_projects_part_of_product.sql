WITH source AS (
  SELECT * 
  FROM EMBUCKET.seed_engineering.projects_part_of_product
)

SELECT *
FROM source