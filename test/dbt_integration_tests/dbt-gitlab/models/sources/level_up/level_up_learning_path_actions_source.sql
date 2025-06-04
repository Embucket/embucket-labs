{{ level_up_intermediate('learning_path_actions') }}

parsed AS (
  SELECT
    value['id']::VARCHAR                        AS learning_path_action_id,
    value['source']::VARCHAR                    AS source,
    value['timestamp']::TIMESTAMP               AS event_timestamp,
    value['type']::VARCHAR                      AS learning_path_action_type,
    {{ level_up_filter_gitlab_email("value['user']") }} AS username,
    value['companyId']::VARCHAR                 AS company_id,
    value['notifiableId']::VARCHAR              AS notifiable_id,
    value['learningPathTitle']::VARCHAR         AS learning_path_title,
    value['learningPathSku']::VARCHAR           AS learning_path_sku,
    value['milestoneTitle']::VARCHAR            AS milestone_title,
    value['userDetail']['id']::VARCHAR          AS user_id,
    value['userDetail']['sfContactId']::VARCHAR AS sf_contact_id,
    value['userDetail']['sfAccountId']::VARCHAR AS sf_account_id,

    uploaded_at
  FROM intermediate

  -- remove dups in case 'raw' is reloaded
  QUALIFY
    ROW_NUMBER() OVER (
      PARTITION BY
        learning_path_action_id
      ORDER BY
        uploaded_at DESC
    ) = 1
)

SELECT * FROM parsed
