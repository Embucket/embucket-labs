WITH filtered_attributes AS (

    SELECT
      jsontext['diff_files']::ARRAY           AS file_diffs,
      jsontext['branch_name']::VARCHAR        AS source_branch_name,
      jsontext['merge_request_diffs']::ARRAY  AS merge_request_version_diffs,
      jsontext['plain_diff_path']::VARCHAR    AS plain_diff_url_path
    FROM  {{ source('handbook', 'handbook_merge_requests') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY jsontext['plain_diff_path']
     ORDER BY ARRAY_SIZE(jsontext['merge_request_diffs']) DESC, uploaded_at DESC) = 1


)

SELECT * 
FROM filtered_attributes
 