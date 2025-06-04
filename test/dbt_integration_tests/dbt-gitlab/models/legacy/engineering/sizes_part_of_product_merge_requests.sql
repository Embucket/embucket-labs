WITH source AS (

    SELECT
      added_lines::NUMBER                                       AS product_merge_request_lines_added,
      real_size::VARCHAR                                        AS product_merge_request_files_changed,
      REGEXP_REPLACE(real_size::VARCHAR, '[^0-9]+', '')::NUMBER AS product_merge_request_files_changed_truncated,
      removed_lines::VARCHAR                                    AS product_merge_request_lines_removed,
      product_merge_request_iid,
      product_merge_request_project
    FROM {{ ref('engineering_part_of_product_merge_requests_source') }}

), product_projects AS (

    SELECT *
    FROM {{ ref('projects_part_of_product') }}

), project_id_merged_in AS (

    SELECT
      product_merge_request_lines_added,
      product_merge_request_files_changed,
      product_merge_request_files_changed_truncated,
      product_merge_request_lines_removed,
      product_merge_request_project,
      product_projects.project_id AS product_merge_request_project_id,
      product_merge_request_iid
    FROM source
    INNER JOIN product_projects
      ON product_projects.project_path = source.product_merge_request_project

)
SELECT * 
FROM project_id_merged_in


