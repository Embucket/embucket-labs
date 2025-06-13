{{ level_up_intermediate('email_captures') }}

users AS (
  SELECT
    user_id,
    username
  FROM {{ ref('level_up_users_source') }}
),

-- used to get the `user_id` in parsed cte
intermediate_users AS (
  SELECT
    intermediate.*,
    users.user_id
  FROM intermediate
  LEFT JOIN users
    ON intermediate.value['user'] = users.username
),

parsed AS (
  SELECT
    value['notifiableType']::VARCHAR                        AS notifiable_type,
    value['courseSku']::VARCHAR                             AS course_sku,
    value['courseId']::VARCHAR                              AS course_id,
    value['companyId']::VARCHAR                             AS company_id,
    value['companyHost']::VARCHAR                           AS company_host,
    value['companySubdomain']::VARCHAR                      AS company_subdomain,
    value['courseTitle']::VARCHAR                           AS course_title,
    value['anonymousId']::VARCHAR                           AS anonymous_id,
    value['source']::VARCHAR                                AS source,
    value['notifiableId']::VARCHAR                          AS notifiable_id,
    value['timestamp']::TIMESTAMP                           AS event_timestamp,

    user_id,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,
    value['event']::VARCHAR                                 AS event, -- noqa: RF04
    SHA2(CONCAT(course_id, value['user'], event_timestamp)) AS email_capture_id,

    uploaded_at
  FROM intermediate_users

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        email_capture_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
