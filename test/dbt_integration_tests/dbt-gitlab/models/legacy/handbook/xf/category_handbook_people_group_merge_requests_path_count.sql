{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_people_group_merge_requests AS (

    SELECT *
    FROM {{ ref('category_handbook_people_group_merge_requests') }}

), handbook_people_group_merge_request_path_count_department AS (

    SELECT
      -- Foreign Keys 
      merge_request_iid,

      -- Logical Information
      merge_request_path,
      merge_request_state,
      CASE WHEN LOWER(merge_request_path) LIKE '%/handbook/people-group/%' THEN 1  
           ELSE 0 END                                                                     AS path_count_people_group,

      -- People Group departments 
      IFF(LOWER(merge_request_path) LIKE '%/handbook/people-group/engineering/%',1,0)     AS path_count_people_group_engineering,    

      -- Metadata 
      merge_request_created_at,
      merge_request_last_edited_at,
      merge_request_merged_at,
      merge_request_updated_at

    FROM category_handbook_people_group_merge_requests

)

SELECT *
FROM handbook_people_group_merge_request_path_count_department
