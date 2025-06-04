WITH source AS (

    SELECT *
    FROM {{ ref('engineering_part_of_product_merge_requests_source') }}

), mr_information AS (

    SELECT prep_merge_request.*
    FROM {{ ref('prep_merge_request') }}
    LEFT JOIN {{ ref('prep_project') }}
      ON prep_merge_request.dim_project_sk = prep_project.dim_project_sk
    WHERE ARRAY_CONTAINS('database::approved'::VARIANT, labels)
    AND prep_merge_request.merged_at IS NOT NULL
    AND prep_project.project_id = 278964 --where the db schema is

), changes_to_db_structure AS (

    SELECT 
      DISTINCT
      'gitlab.com' || plain_diff_url_path   AS mr_path,
      mr_information.updated_at             AS merge_request_updated_at,
      merged_at
    FROM source
      INNER JOIN LATERAL FLATTEN(INPUT => file_diffs, outer => true) d
      INNER JOIN mr_information
        ON source.product_merge_request_iid = mr_information.merge_request_internal_id
    WHERE target_branch_name = 'master' 
      AND d.value['file_path'] = 'db/structure.sql'

)

SELECT *
FROM changes_to_db_structure
ORDER BY merged_at DESC