WITH merge_requests AS (

    SELECT
      prep_merge_request.*
    FROM {{ ref('prep_merge_request') }}

), mr_files AS (
    
    SELECT 
      marketing_site_file_edited,
      REGEXP_REPLACE(plain_diff_url_path, '[^0-9]+', '')::NUMBER AS merge_request_iid
    FROM {{ ref('marketing_site_merge_requests_files') }}

), file_classifications AS (

    SELECT 
      marketing_site_path,
      file_classification
    FROM {{ ref('marketing_site_file_classification_mapping') }}

), joined_to_mr AS (

    SELECT 
      merge_requests.merge_request_state                               AS merge_request_state,
      merge_requests.updated_at                                        AS merge_request_updated_at,
      merge_requests.created_at                                        AS merge_request_created_at,
      merge_requests.merge_request_last_edited_at                      AS merge_request_last_edited_at,
      merge_requests.merged_at                                         AS merge_request_merged_at,
      mr_files.merge_request_iid                                       AS merge_request_iid,
      mr_files.marketing_site_file_edited                              AS merge_request_path,
      IFNULL(file_classifications.file_classification, 'unclassified') AS file_classification
    FROM mr_files
    INNER JOIN merge_requests
      ON mr_files.merge_request_iid = merge_requests.merge_request_internal_id
        AND merge_requests.project_id = 7764 --marketing site project
    LEFT JOIN file_classifications
      ON LOWER(mr_files.marketing_site_file_edited) LIKE '%' || file_classifications.marketing_site_path || '%'
    WHERE merge_requests.is_merge_to_master 

), renamed AS (

    SELECT
      merge_request_state,
      merge_request_updated_at,
      merge_request_created_at,
      merge_request_last_edited_at,
      merge_request_merged_at,
      merge_request_iid,
      merge_request_path,
      ARRAY_AGG(DISTINCT file_classification) AS merge_request_department_list
    FROM joined_to_mr
    {{ dbt_utils.group_by(n=7) }}

)
SELECT * 
FROM renamed
