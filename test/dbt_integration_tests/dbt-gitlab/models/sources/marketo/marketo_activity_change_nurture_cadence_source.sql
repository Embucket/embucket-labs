WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_change_nurture_cadence') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_change_nurture_cadence_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      new_nurture_cadence::TEXT                 AS new_nurture_cadence,
      previous_nurture_cadence::TEXT            AS previous_nurture_cadence

    FROM source

)

SELECT *
FROM renamed
