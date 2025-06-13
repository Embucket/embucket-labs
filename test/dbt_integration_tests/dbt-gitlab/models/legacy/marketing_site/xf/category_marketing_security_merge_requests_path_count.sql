{{ config({
    "materialized": "view"
    })
}}

WITH category_marketing_security_merge_requests AS (

    SELECT *
    FROM {{ ref('category_marketing_security_merge_requests') }}

), marketing_security_merge_request_path_count_department AS (

    SELECT
      -- Foreign Keys 
      merge_request_iid,

      -- Logical Information
      merge_request_path,
      merge_request_state,

      -- Security
      IFF(LOWER(merge_request_path) LIKE '%sites/marketing/source/security/%',1,0)      AS path_count_security,    
                
      -- Metadata 
      merge_request_created_at,
      merge_request_last_edited_at,
      merge_request_merged_at,
      merge_request_updated_at

    FROM category_marketing_security_merge_requests

)

SELECT *
FROM marketing_security_merge_request_path_count_department
