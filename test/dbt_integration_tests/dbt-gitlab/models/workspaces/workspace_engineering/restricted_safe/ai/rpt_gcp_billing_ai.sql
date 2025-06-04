{{ config(
    materialized='table',
    )
}}


WITH
  mapping AS (
  SELECT
    *
  FROM
    {{ ref('gcp_billing_ai_mapping') }}  ),

source as (

  SELECT * FROM {{ ref('gcp_billing_detailed_export_xf') }}
  WHERE
  DATE(usage_start_time) >= '2023-04-01'
  AND project_id = 'unreview-poc-390200e5'

),

subq AS (
SELECT 
    *
  FROM
    source
  CROSS JOIN
    mapping
  WHERE
    source.resource_name LIKE mapping.parsed_resource_name
)


SELECT
  DATE(subq.usage_start_time) AS day,
  CASE 
    WHEN MIN(subq.ai_category) IS NULL THEN 'Other' 
    ELSE subq.ai_category
    END AS ai_category,
  CASE 
    WHEN MIN(subq.ai_subcategory) IS NULL THEN 'Other' 
    ELSE subq.ai_subcategory
    END AS ai_subcategory,
  CASE 
    WHEN MIN(subq.release_status) IS NULL THEN 'Other' 
    ELSE subq.release_status
    END AS release_status,
  SUM(total_cost) as net_cost
FROM
  subq
GROUP BY
  day, subq.ai_category, subq.ai_subcategory, subq.release_status
ORDER BY
  5 DESC