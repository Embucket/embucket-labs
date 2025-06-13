{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_engineering_merge_requests AS (

    SELECT *
    FROM {{ ref('category_handbook_engineering_merge_requests') }}

), handbook_engineering_merge_request_path_count_department AS (

    SELECT
      -- Foreign Keys 
      merge_request_iid,

      -- Logical Information
      merge_request_path,
      merge_request_state,
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/engineering/%' THEN 1 
           WHEN LOWER(merge_request_path) LIKE '%/handbook/support/%' THEN 1 
           ELSE 0 END                                                                     AS path_count_engineering,

      -- Engineering departments 
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/engineering/development/%' THEN 1
           WHEN LOWER(merge_request_path) LIKE '%data/performance_indicators/development_department.yml%' THEN 1
           ELSE 0 END                                                                    AS path_count_development,   
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/engineering/infrastructure/%' THEN 1
           WHEN LOWER(merge_request_path) LIKE '%data/performance_indicators/infrastructure_department.yml%' THEN 1
           ELSE 0 END                                                                    AS path_count_infrastructure,
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/engineering/quality/%' THEN 1
           WHEN LOWER(merge_request_path) LIKE '%data/performance_indicators/quality_department.yml%' THEN 1
           ELSE 0 END                                                                    AS path_count_quality,
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/engineering/security/%' THEN 1
           WHEN LOWER(merge_request_path) LIKE '%data/performance_indicators/security_department.yml%' THEN 1
           WHEN LOWER(merge_request_path) LIKE '%/handbook/security/%' THEN 1
           ELSE 0 END                                                                    AS path_count_security,
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/support/%' THEN 1
           WHEN LOWER(merge_request_path) LIKE '%data/performance_indicators/customer_support_department.yml%' THEN 1
           ELSE 0 END                                                                     AS path_count_support,
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/engineering/ux/%' THEN 1
           WHEN LOWER(merge_request_path) LIKE '%data/performance_indicators/ux_department.yml%' THEN 1
           ELSE 0 END                                                                     AS path_count_ux,
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/engineering/incubation/%' THEN 1
           WHEN LOWER(merge_request_path) LIKE '%data/performance_indicators/incubation_engineering_department.yml%' THEN 1
           ELSE 0 END                                                                     AS path_count_incubation,
      -- Metadata 
      merge_request_created_at,
      merge_request_last_edited_at,
      merge_request_merged_at,
      merge_request_updated_at

    FROM category_handbook_engineering_merge_requests

)

SELECT *
FROM handbook_engineering_merge_request_path_count_department
