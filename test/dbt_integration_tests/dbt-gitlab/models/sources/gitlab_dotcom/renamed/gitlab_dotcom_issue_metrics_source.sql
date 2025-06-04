WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_issue_metrics_dedupe_source') }}
  
), renamed AS (

    SELECT

      id::NUMBER                                               AS issue_metric_id,
      issue_id::NUMBER                                         AS issue_id,
      first_mentioned_in_commit_at::DATE                        AS first_mentioned_in_commit_at,
      first_associated_with_milestone_at::DATE                  AS first_associated_with_milestone_at,
      first_added_to_board_at::DATE                             AS first_added_to_board_at,
      created_at::TIMESTAMP                                     AS created_at,
      updated_at::TIMESTAMP                                     AS updated_at


    FROM source

)

SELECT *
FROM renamed
