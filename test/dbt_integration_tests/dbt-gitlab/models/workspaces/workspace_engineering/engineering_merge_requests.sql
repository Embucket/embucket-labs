WITH base AS (

  SELECT *
  FROM {{ ref('internal_merge_requests_enhanced') }}
  WHERE is_part_of_product = TRUE

)

SELECT * EXCLUDE is_part_of_product
FROM base
