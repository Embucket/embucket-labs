{{ config({
    "materialized": "ephemeral"
    })
}}

WITH base AS (

    SELECT *
    FROM {{ ref('handbook_merge_requests_source') }}

), exploded_file_paths AS ( -- explodes the files in the list of diffs
    
    SELECT 
      file_diffs.value:file_path::VARCHAR AS handbook_file_edited,
      base.plain_diff_url_path            AS plain_diff_url_path,
      base.merge_request_version_diffs    AS merge_request_version_diffs,
      base.source_branch_name             AS source_branch_name
    FROM base
    INNER JOIN TABLE(FLATTEN(INPUT => file_diffs, outer => true)) AS file_diffs
    WHERE LOWER(file_diffs.value:file_path) LIKE '%/handbook/%'
    OR LOWER(file_diffs.value:file_path) LIKE '%data/performance_indicators%'

)
SELECT * 
FROM exploded_file_paths
