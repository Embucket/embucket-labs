{{ level_up_intermediate('course_actions') }}

parsed AS (
  SELECT
    value['id']::VARCHAR               AS course_action_id,
    value['companyId']::VARCHAR        AS company_id,
    value['source']::VARCHAR           AS course_action,
    value['courseSku']::VARCHAR        AS course_sku,
    value['courseTitle']::VARCHAR      AS course_title,
    value['timestamp']::TIMESTAMP      AS event_timestamp,
    value['notifiableId']::VARCHAR     AS notifiable_id,
    value['type']::VARCHAR             AS transaction_type,
    value['userDetail']['id']::VARCHAR AS user_id,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        course_action_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
