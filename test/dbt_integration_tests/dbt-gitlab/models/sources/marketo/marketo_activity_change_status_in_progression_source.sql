WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_change_status_in_progression') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_change_status_in_progression_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      old_status_id::NUMBER                     AS old_status_id,
      new_status_id::NUMBER                     AS new_status_id,
      acquired_by::BOOLEAN                      AS acquired_by,
      old_status::TEXT                          AS old_status,
      new_status::TEXT                          AS new_status,
      program_member_id::NUMBER                 AS program_member_id,
      success::BOOLEAN                          AS success,
      registration_code::TEXT                   AS registration_code,
      webinar_url::TEXT                         AS webinar_url,
      reason::TEXT                              AS reason,
      reached_success_date::TIMESTAMP_NTZ       AS reached_success_date

    FROM source

)

SELECT *
FROM renamed
