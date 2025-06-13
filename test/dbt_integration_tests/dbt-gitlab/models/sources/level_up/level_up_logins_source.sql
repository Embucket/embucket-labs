{{ level_up_intermediate('logins') }}

parsed AS (
  SELECT
    value['browserInfo']::VARIANT      AS browser_info,
    value['companyHost']::VARCHAR      AS company_host,
    value['companyId']::VARCHAR        AS company_id,
    value['companySubdomain']::VARCHAR AS company_subdomain,
    value['event']::VARCHAR            AS event, -- noqa: RF04
    value['timestamp']::TIMESTAMP      AS event_timestamp,
    value['userAgent']::VARCHAR        AS user_agent,

    value['userDetail']['id']::VARCHAR AS user_id,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id,
        event_timestamp
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
