WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_unsubscribe_email') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_unsubscribe_email_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      campaign_run_id::NUMBER                   AS campaign_run_id,
      webform_id::NUMBER                        AS webform_id,
      client_ip_address::TEXT                   AS client_ip_address,
      form_fields::TEXT                         AS form_fields,
      webpage_id::NUMBER                        AS webpage_id,
      user_agent::TEXT                          AS user_agent,
      query_parameters::TEXT                    AS query_parameters,
      referrer_url::TEXT                        AS referrer_url,
      test_variant::NUMBER                      AS test_variant

    FROM source

)

SELECT *
FROM renamed
