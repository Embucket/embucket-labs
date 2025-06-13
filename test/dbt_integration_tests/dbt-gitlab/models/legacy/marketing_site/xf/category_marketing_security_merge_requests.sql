{{ config({
    "materialized": "view"
    })
}}

WITH marketing_categories AS (

    SELECT *
    FROM {{ ref('category_marketing_site_merge_requests') }}

), filtered_to_security AS (

    SELECT *
    FROM marketing_categories
    WHERE ARRAY_CONTAINS('security'::VARIANT, merge_request_department_list)

)
SELECT *
FROM filtered_to_security
