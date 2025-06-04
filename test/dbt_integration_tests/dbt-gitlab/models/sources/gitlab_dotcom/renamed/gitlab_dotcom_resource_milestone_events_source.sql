WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_resource_milestone_events_dedupe_source') }}

), renamed AS (

    SELECT
      id                                             AS resource_milestone_event_id,
      action::NUMBER                                AS action_type_id,
      {{ resource_event_action_type('action') }}     AS action_type,
      user_id::NUMBER                               AS user_id,
      issue_id::NUMBER                              AS issue_id,
      merge_request_id::NUMBER                      AS merge_request_id,
      milestone_id::NUMBER                          AS milestone_id,
      {{ map_state_id('state') }}                    AS milestone_state,
      created_at::TIMESTAMP                          AS created_at
    FROM source

)

SELECT *
FROM renamed
