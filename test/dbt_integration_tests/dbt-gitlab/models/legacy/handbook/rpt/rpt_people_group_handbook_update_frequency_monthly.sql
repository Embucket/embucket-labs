{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_people_group_merge_requests_count AS (

    SELECT *
    FROM {{ ref('category_handbook_people_group_merge_requests_count') }}

), handbook_people_group_total_count_department AS (

    SELECT
      DATE_TRUNC('MONTH', merge_request_merged_at)    AS month_merged_at,
      SUM(mr_count_people_group)                      AS mr_count_people_group,
      SUM(mr_count_people_group_engineering)          AS mr_count_people_group_engineering
    FROM category_handbook_people_group_merge_requests_count
    WHERE merge_request_state = 'merged'
      AND merge_request_merged_at IS NOT NULL
    GROUP BY 1

)

SELECT *
FROM handbook_people_group_total_count_department
