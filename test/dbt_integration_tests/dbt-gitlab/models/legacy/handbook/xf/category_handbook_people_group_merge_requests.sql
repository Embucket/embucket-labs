{{ config({
    "materialized": "view"
    })
}}

WITH handbook_categories AS (

    SELECT *
    FROM {{ ref('category_handbook_merge_requests') }}

), filtered_to_people_group AS (

    SELECT *
    FROM handbook_categories
    WHERE ARRAY_CONTAINS('people_group'::VARIANT, merge_request_department_list)

)
SELECT *
FROM filtered_to_people_group
