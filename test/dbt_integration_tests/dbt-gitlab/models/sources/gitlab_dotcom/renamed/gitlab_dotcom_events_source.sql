WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_events_dedupe_source') }}

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
      {{action_type(action_type_id='event_action_type_id')}}::VARCHAR AS event_action_type

    FROM source

)

SELECT *
FROM renamed
