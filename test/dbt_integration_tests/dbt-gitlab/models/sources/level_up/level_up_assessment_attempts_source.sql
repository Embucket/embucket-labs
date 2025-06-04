{{ config(
    materialized='incremental',
    unique_key='assessment_attempt_id'
) }}

{{ level_up_intermediate('assessment_attempts') }}

parsed AS (
  SELECT
    value['id']::VARCHAR                  AS assessment_attempt_id,
    value['assessment']['id']::VARCHAR    AS assessment_id,
    value['assessment']['title']::VARCHAR AS assessment_title,
    value['assessment']['type']::VARCHAR  AS assessment_type,
    value['course']['title']::VARCHAR     AS course_title,
    value['courseId']::VARCHAR            AS course_id,
    value['createdAt']::TIMESTAMP         AS created_at,
    value['grade']::INT                   AS grade,
    value['passed']::BOOLEAN              AS has_passed,
    value['status']::VARCHAR              AS status,
    value['timeElapsedInSeconds']::INT    AS time_elapsed_in_seconds,
    value['updatedAt']::TIMESTAMP         AS updated_at,
    value['user']['clientId']::VARCHAR    AS client_id,
    {{ level_up_filter_gitlab_email("value['user']['email']") }} AS username,
    value['user']['id']::VARCHAR          AS user_id,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        assessment_attempt_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
