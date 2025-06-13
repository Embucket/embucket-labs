{{ level_up_intermediate('course_completions') }}

parsed AS (
  SELECT
    value['id']::VARCHAR               AS course_completion_id,
    value['companyId']::VARCHAR        AS company_id,
    value['source']::VARCHAR           AS course_action,
    value['event']::VARCHAR            AS event, -- noqa: RF04
    value['timestamp']::TIMESTAMP      AS event_timestamp,
    value['license']::VARCHAR          AS license,
    value['notifiableId']::VARCHAR     AS notifiable_id,
    value['title']::VARCHAR            AS title,
    value['type']::VARCHAR             AS transaction_type,
    value['updatedAt']::TIMESTAMP      AS updated_at,

    value['userDetail']['id']::VARCHAR AS user_id,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        course_completion_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
