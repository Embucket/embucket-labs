WITH product_mrs AS (

  SELECT *
  FROM {{ ref('open_merge_request_review_time_base') }}
  WHERE is_part_of_product

)

SELECT *
FROM product_mrs
