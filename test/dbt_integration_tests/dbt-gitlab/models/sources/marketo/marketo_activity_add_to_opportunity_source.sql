WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_add_to_opportunity') }}

), renamed AS (

    SELECT

      id::NUMBER                                    AS marketo_activity_add_to_opportunity_id,
      lead_id::NUMBER                               AS lead_id,
      activity_date::TIMESTAMP_TZ                   AS activity_date,
      activity_type_id::NUMBER                      AS activity_type_id,
      campaign_id::NUMBER                           AS campaign_id,
      primary_attribute_value_id::NUMBER            AS primary_attribute_value_id,
      primary_attribute_value::TEXT                 AS primary_attribute_value,
      role::TEXT                                    AS track_id

    FROM source

)

SELECT *
FROM renamed
