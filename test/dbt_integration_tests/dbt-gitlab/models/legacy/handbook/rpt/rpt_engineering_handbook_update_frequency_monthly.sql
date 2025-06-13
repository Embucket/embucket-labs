{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_engineering_merge_requests_count AS (

    SELECT *
    FROM {{ ref('category_handbook_engineering_merge_requests_count') }}

), handbook_engineering_total_count_department AS (

    SELECT
      DATE_TRUNC('MONTH', merge_request_merged_at)    AS month_merged_at,
      SUM(mr_count_engineering)                       AS mr_count_engineering,
      SUM(mr_count_ux)                                AS mr_count_ux,
      SUM(mr_count_security)                          AS mr_count_security,
      SUM(mr_count_infrastructure)                    AS mr_count_infrastructure,
      SUM(mr_count_development)                       AS mr_count_development,
      SUM(mr_count_quality)                           AS mr_count_quality,
      SUM(mr_count_support)                           AS mr_count_support,
      SUM(mr_count_incubation)                        AS mr_count_incubation
    FROM category_handbook_engineering_merge_requests_count
    WHERE merge_request_state = 'merged'
      AND merge_request_merged_at IS NOT NULL
    GROUP BY 1

)

SELECT *
FROM handbook_engineering_total_count_department
