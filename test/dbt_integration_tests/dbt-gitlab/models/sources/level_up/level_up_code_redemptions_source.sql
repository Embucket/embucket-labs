{{ level_up_intermediate('code_redemptions') }}

parsed AS (
  SELECT
    value['companyHost']::VARCHAR              AS company_host,
    value['companyId']::VARCHAR                AS company_id,
    value['companySubdomain']::VARCHAR         AS company_subdomain,
    value['event']::VARCHAR                    AS event,
    value['redemptionCode']::VARCHAR           AS redemption_code,
    value['redemptionCodeGroupLabel']::VARCHAR AS redemption_code_group_label,
    value['timestamp']::TIMESTAMP              AS event_timestamp,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,
    value['userDetail']['id']::VARCHAR         AS user_id,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        user_id,
        redemption_code_group_label,
        redemption_code,
        event_timestamp
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
