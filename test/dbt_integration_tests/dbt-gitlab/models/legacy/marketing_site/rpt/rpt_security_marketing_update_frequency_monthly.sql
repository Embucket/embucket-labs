{{ config({
    "materialized": "view"
    })
}}

WITH category_marketing_security_merge_requests_count AS (

    SELECT *
    FROM {{ ref('category_marketing_security_merge_requests_count') }}

), marketing_security_total_count_department AS (

    SELECT
      DATE_TRUNC('MONTH', merge_request_merged_at)    AS month_merged_at,
      SUM(mr_count_security)                          AS mr_count_security
    FROM category_marketing_security_merge_requests_count
    WHERE merge_request_state = 'merged'
      AND merge_request_merged_at IS NOT NULL
    GROUP BY 1

)

SELECT *
FROM marketing_security_total_count_department