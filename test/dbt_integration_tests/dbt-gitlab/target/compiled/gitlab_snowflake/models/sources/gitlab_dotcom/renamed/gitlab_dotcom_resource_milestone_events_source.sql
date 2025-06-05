WITH source AS (

  SELECT *
  FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_resource_milestone_events_dedupe_source

), renamed AS (

    SELECT
      id                                             AS resource_milestone_event_id,
      action::NUMBER                                AS action_type_id,
      CASE  WHEN action::NUMBER = 1 THEN 'added'
        WHEN action::NUMBER = 2 THEN 'removed'
  END     AS action_type,
      user_id::NUMBER                               AS user_id,
      issue_id::NUMBER                              AS issue_id,
      merge_request_id::NUMBER                      AS merge_request_id,
      milestone_id::NUMBER                          AS milestone_id,
      CASE
      WHEN state::NUMBER = 1 THEN 'opened'
      WHEN state::NUMBER = 2 THEN 'closed'
      WHEN state::NUMBER = 3 THEN 'merged'
      WHEN state::NUMBER = 4 THEN 'locked'
      ELSE NULL
    END                    AS milestone_state,
      created_at::TIMESTAMP                          AS created_at
    FROM source

)

SELECT *
FROM renamed