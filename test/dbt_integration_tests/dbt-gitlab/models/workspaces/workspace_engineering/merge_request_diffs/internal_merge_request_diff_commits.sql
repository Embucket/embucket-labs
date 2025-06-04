/*
Currently limited to any commits with diff_id >= 208751592
*/
{{ config(
  materialized="incremental",
  unique_key = ['merge_request_diff_id', 'relative_order'],
  full_refresh= only_force_full_refresh()
) }}

WITH internal_merge_request_diffs AS (

  SELECT *
  FROM {{ ref('internal_merge_request_diffs') }}
),

merge_request_diff_commits_chunked AS (
  SELECT *
  FROM {{ source('gitlab_dotcom', 'merge_request_diff_commits') }}
  {% if is_incremental() %}
    WHERE
      /*
      If there is a passed in backfill_start_id, use that
      Else, handle like a regular incremental model using merge_request_diff_id
      */
      merge_request_diff_id >= (
        SELECT
          {% if var('backfill_start_id', false) != false %}
          '{{ var("backfill_start_id") }}'
        {% else %}
            MAX(merge_request_diff_id) + 1
          FROM {{ this }}
          {% endif %}
      )
      AND
      merge_request_diff_id
      <= (
        SELECT '{{ var("backfill_end_id", 9999999999999) }}'
      )

  {% else %}
    -- Failsafe: on first creation of the table or a full refresh,
    -- only create the table, but never backfill it
    WHERE merge_request_diff_id > 9999999999999

  {% endif %}
),

internal_merge_request_diff_commits AS (
  SELECT merge_request_diff_commits_chunked.* FROM internal_merge_request_diffs
  INNER JOIN merge_request_diff_commits_chunked
    ON internal_merge_request_diffs.merge_request_diff_id
      = merge_request_diff_commits_chunked.merge_request_diff_id

),

internal_merge_request_diff_commits_dedupped AS (
  SELECT * FROM internal_merge_request_diff_commits
  QUALIFY ROW_NUMBER() OVER (PARTITION BY merge_request_diff_id, relative_order ORDER BY _uploaded_at DESC) = 1
),

internal_merge_request_diff_commits_renamed AS (
  SELECT
    authored_date::TIMESTAMP         AS authored_date,
    committed_date::TIMESTAMP        AS committed_date,
    merge_request_diff_id::INT       AS merge_request_diff_id,
    relative_order::INT              AS relative_order,
    REPLACE(sha, '\\x', '')::VARCHAR AS sha,
    commit_author_id::INT            AS commit_author_id,
    committer_id::INT                AS committer_id,
    _uploaded_at::FLOAT              AS _uploaded_at
  FROM internal_merge_request_diff_commits_dedupped
)

SELECT * FROM internal_merge_request_diff_commits_renamed
