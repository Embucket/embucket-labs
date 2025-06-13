{{ config({
    "materialized": "view"
    })
}}

WITH handbook_categories AS (

    SELECT *
    FROM {{ ref('category_handbook_merge_requests') }}

), filtered_to_engineering AS (

    SELECT *
    FROM handbook_categories
    WHERE ARRAY_CONTAINS('engineering'::VARIANT, merge_request_department_list) 
      OR ARRAY_CONTAINS('support'::VARIANT, merge_request_department_list)
      OR ARRAY_CONTAINS('security'::VARIANT, merge_request_department_list)

)
SELECT *
FROM filtered_to_engineering
