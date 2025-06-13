{{ config({
    "materialized": "view"
    })
}}

WITH category_marketing_security_merge_requests_path_count AS (

    SELECT *
    FROM {{ ref('category_marketing_security_merge_requests_path_count') }}

), marketing_security_merge_request_count_department AS (

    SELECT
      -- Foreign Keys
      merge_request_iid,
    
      -- Metadata
      merge_request_created_at,
      merge_request_last_edited_at,
      merge_request_merged_at,
      merge_request_updated_at,

      -- Logical Information
      merge_request_state,
    
      -- Security
      MAX(path_count_security)            AS mr_count_security
        
    FROM category_marketing_security_merge_requests_path_count
    {{ dbt_utils.group_by(n=6) }}

)

SELECT *
FROM marketing_security_merge_request_count_department
