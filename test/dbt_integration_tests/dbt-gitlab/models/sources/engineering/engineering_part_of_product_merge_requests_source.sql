{{
    config({
    "schema": "legacy"
    })
}}

WITH flattened AS (
  SELECT
    value,
    uploaded_at
  FROM {{ source('engineering', 'part_of_product_graphql_merge_requests') }}
  INNER JOIN LATERAL FLATTEN(input => jsontext['data']['project']['mergeRequests']['nodes'])
),

dedupped AS (

  SELECT *
  FROM flattened
  QUALIFY ROW_NUMBER() OVER (
    PARTITION BY value['webPath']
    ORDER BY value['updatedAt'] DESC, uploaded_at DESC
  ) = 1

),

renamed AS (

  SELECT
    value['diffStatsSummary']['additions']::NUMBER                                                AS added_lines,
    value['diffStatsSummary']['fileCount']::VARCHAR                                               AS real_size,
    value['diffStatsSummary']['deletions']::NUMBER                                                AS removed_lines,
    value['webPath']::VARCHAR || '.diff'                                                          AS plain_diff_url_path,
    value['createdAt']::TIMESTAMP                                                                 AS merge_request_created_at,
    value['updatedAt']::TIMESTAMP                                                                 AS merge_request_updated_at,
    value['diffStats']::ARRAY                                                                     AS file_diffs,
    value['targetBranch']::VARCHAR                                                                AS target_branch_name,
    value['iid']::NUMBER                                                                          AS product_merge_request_iid,
    value['projectId']::NUMBER                                                                    AS product_merge_request_project_id,
    TRIM(ARRAY_TO_STRING(ARRAY_SLICE(SPLIT(plain_diff_url_path, '-'), 0, -1), '-'), '/')::VARCHAR AS product_merge_request_project,
    uploaded_at
  FROM dedupped
)

SELECT * FROM renamed
