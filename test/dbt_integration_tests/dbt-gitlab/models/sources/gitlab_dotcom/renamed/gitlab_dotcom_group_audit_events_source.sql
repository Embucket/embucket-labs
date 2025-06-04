WITH source AS (

    SELECT *
    FROM {{ ref('gitlab_dotcom_group_audit_events_dedupe_source') }}

), parsed_columns AS (

    SELECT
      id::NUMBER                  AS id,
      created_at::TIMESTAMP       AS created_at,
      group_id::NUMBER            AS group_id,
      target_id::NUMBER           AS target_id,
      author_id::NUMBER           AS author_id,
      event_name::VARCHAR         AS event_name,
      details::VARCHAR            AS details
    FROM source

)

SELECT *
FROM parsed_columns
