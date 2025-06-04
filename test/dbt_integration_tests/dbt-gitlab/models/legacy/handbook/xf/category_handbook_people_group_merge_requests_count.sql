{{ config({
    "materialized": "view"
    })
}}

WITH category_handbook_people_group_merge_requests_path_count AS (

    SELECT *
    FROM {{ ref('category_handbook_people_group_merge_requests_path_count') }}

), handbook_people_group_merge_request_count_department AS (

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
      MAX(path_count_people_group)         AS mr_count_people_group,
    
      -- People Group departments
      MAX(path_count_people_group_engineering)         AS mr_count_people_group_engineering
        
    FROM category_handbook_people_group_merge_requests_path_count
    {{ dbt_utils.group_by(n=6) }}

)

SELECT *
FROM handbook_people_group_merge_request_count_department
