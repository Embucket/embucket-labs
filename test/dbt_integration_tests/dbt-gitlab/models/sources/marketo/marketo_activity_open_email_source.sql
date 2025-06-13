WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_open_email') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_open_email_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      campaign_run_id::NUMBER                   AS campaign_run_id,
      platform::TEXT                            AS platform,
      is_mobile_device::BOOLEAN                 AS is_mobile_device,
      step_id::NUMBER                           AS step_id,
      device::TEXT                              AS device,
      test_variant::NUMBER                      AS test_variant,
      choice_number::NUMBER                     AS choice_number,
      is_bot_activity::BOOLEAN                  AS is_bot_activity,
      user_agent::TEXT                          AS user_agent,
      bot_activity_pattern::TEXT                AS bot_activity_pattern

    FROM source

)

SELECT *
FROM renamed
