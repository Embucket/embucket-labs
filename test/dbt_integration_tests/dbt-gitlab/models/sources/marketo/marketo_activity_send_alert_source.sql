WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_send_alert') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_send_alert_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      send_to_owner::TEXT                       AS send_to_owner,
      send_to_list::TEXT                        AS send_to_list

    FROM source

)

SELECT *
FROM renamed
