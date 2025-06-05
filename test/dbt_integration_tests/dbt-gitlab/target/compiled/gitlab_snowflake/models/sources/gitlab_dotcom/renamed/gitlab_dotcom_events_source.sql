WITH source AS (

    SELECT *
    FROM EMBUCKET.gitlab_dotcom.gitlab_dotcom_events_dedupe_source

), renamed AS (

    SELECT
      id                                                              AS event_id,
      project_id::NUMBER                                             AS project_id,
      author_id::NUMBER                                              AS author_id,
      target_id::NUMBER                                              AS target_id,
      target_type::VARCHAR                                            AS target_type,
      created_at::TIMESTAMP                                           AS created_at,
      updated_at::TIMESTAMP                                           AS updated_at,
      action::NUMBER                                                 AS event_action_type_id,
      CASE  WHEN event_action_type_id::NUMBER = 1 THEN 'created'
        WHEN event_action_type_id::NUMBER = 2 THEN 'updated'
        WHEN event_action_type_id::NUMBER = 3 THEN 'closed'
        WHEN event_action_type_id::NUMBER = 4 THEN 'reopened'
        WHEN event_action_type_id::NUMBER = 5 THEN 'pushed'
        WHEN event_action_type_id::NUMBER = 6 THEN 'commented'
        WHEN event_action_type_id::NUMBER = 7 THEN 'merged'
        WHEN event_action_type_id::NUMBER = 8 THEN 'joined'
        WHEN event_action_type_id::NUMBER = 9 THEN 'left'
        WHEN event_action_type_id::NUMBER = 10 THEN 'destroyed'
        WHEN event_action_type_id::NUMBER = 11 THEN 'expired'
        END::VARCHAR AS event_action_type

    FROM source

)

SELECT *
FROM renamed