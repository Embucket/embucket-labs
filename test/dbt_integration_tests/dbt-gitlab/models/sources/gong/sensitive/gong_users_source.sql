WITH source AS (
  SELECT *
  FROM {{ source('gong','users') }}
),

renamed AS (
  SELECT
    id::NUMBER                                  AS user_id,
    gong_connect_enabled::BOOLEAN               AS gong_connect_enabled,
    meeting_consent_page_url::STRING            AS meeting_consent_page_url,
    prevent_web_conference_recording::BOOLEAN   AS prevent_web_conference_recording,
    phone_number::STRING                        AS phone_number,
    prevent_email_import::BOOLEAN               AS prevent_email_import,
    emails_imported::NUMBER                     AS emails_imported,
    telephony_calls_imported::NUMBER            AS telephony_calls_imported,
    manager_id::NUMBER                          AS manager_id,
    non_recorded_meetings_imported::NUMBER      AS non_recorded_meetings_imported,
    extension::STRING                           AS extension,
    last_name::STRING                           AS last_name,
    first_name::STRING                          AS first_name,
    title::STRING                               AS title,
    web_conferences_recorded::NUMBER            AS web_conferences_recorded,
    email_address::STRING                       AS email_address,
    active::BOOLEAN                             AS active,
    created::TIMESTAMP                          AS created_at,
    _fivetran_deleted::BOOLEAN                  AS _fivetran_deleted,
    _fivetran_synced::TIMESTAMP                 AS _fivetran_synced
  FROM source
)

SELECT * FROM renamed