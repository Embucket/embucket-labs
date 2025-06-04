WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_fill_out_form') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_fill_out_form_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      form_fields::TEXT                         AS form_fields,
      client_ip_address::TEXT                   AS client_ip_address,
      webpage_id::NUMBER                        AS webpage_id,
      user_agent::TEXT                          AS user_agent,
      query_parameters::TEXT                    AS query_parameters,
      referrer_url::TEXT                        AS referrer_url

    FROM source

)

SELECT *
FROM renamed
