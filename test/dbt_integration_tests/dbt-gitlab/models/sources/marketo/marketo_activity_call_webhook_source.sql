WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_call_webhook') }}

), renamed AS (

    SELECT

      id::NUMBER                          AS marketo_activity_call_webhook_id,
      lead_id::NUMBER                     AS lead_id,
      activity_date::TIMESTAMP_TZ         AS activity_date,
      activity_type_id::NUMBER            AS activity_type_id,
      campaign_id::NUMBER                 AS campaign_id,
      primary_attribute_value_id::NUMBER  AS primary_attribute_value_id,
      primary_attribute_value::TEXT       AS primary_attribute_value,
      response::TEXT                      AS response,
      error_type::NUMBER                  AS error_type

    FROM source
)

SELECT *
FROM renamed
