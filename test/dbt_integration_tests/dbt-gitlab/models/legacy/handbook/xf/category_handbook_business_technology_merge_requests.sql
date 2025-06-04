{{ config({
    "materialized": "view"
    })
}}

WITH handbook_categories AS (

  SELECT *
  FROM {{ ref('category_handbook_merge_requests') }}

), filtered_to_business_technology AS (

  SELECT *
  FROM handbook_categories
  WHERE ARRAY_CONTAINS('business_technology'::VARIANT, merge_request_department_list)
    OR ARRAY_CONTAINS('procurement'::VARIANT, merge_request_department_list) 

)

SELECT *
FROM filtered_to_business_technology
