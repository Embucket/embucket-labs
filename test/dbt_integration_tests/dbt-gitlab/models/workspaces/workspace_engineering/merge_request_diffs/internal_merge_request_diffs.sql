/*
Currently limited to any internal diff_ids >= 208751592
*/

{{ config(
  materialized="table"
) }}

WITH internal_merge_requests AS (
  SELECT DISTINCT merge_request_id
  FROM
    {{ ref('internal_merge_requests') }}
),

internal_merge_request_diffs AS (
  SELECT mrd.*
  FROM
    {{ ref('gitlab_dotcom_merge_request_diffs') }} AS mrd
  INNER JOIN internal_merge_requests mr ON mr.merge_request_id = mrd.merge_request_id
  WHERE
    -- corresponds to created_at=2021-07-01 00:00:01.446
    mrd.merge_request_diff_id >= 208751592
)

SELECT * FROM internal_merge_request_diffs
