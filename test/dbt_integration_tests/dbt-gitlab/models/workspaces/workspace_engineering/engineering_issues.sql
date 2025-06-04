WITH engineering_issues AS (

  SELECT *
  FROM {{ ref('internal_issues_enhanced') }}
  WHERE is_part_of_product = TRUE
)

SELECT *
FROM engineering_issues
