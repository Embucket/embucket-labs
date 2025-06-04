WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_change_score') }}

), renamed AS (

    SELECT

      id::NUMBER                                   AS marketo_activity_change_score_id,
      lead_id::NUMBER                              AS lead_id,
      activity_date::TIMESTAMP_TZ                  AS activity_date,
      activity_type_id::NUMBER                     AS activity_type_id,
      campaign_id::NUMBER                          AS campaign_id,
      primary_attribute_value_id::NUMBER           AS primary_attribute_value_id,
      primary_attribute_value::TEXT                AS primary_attribute_value,
      change_value::TEXT                           AS change_value,
      old_value::NUMBER                            AS old_value,
      new_value::NUMBER                            AS new_value,
      reason::TEXT                                 AS reason,
      relative_urgency::NUMBER                     AS relative_urgency,
      priority::NUMBER                             AS priority,
      relative_score::NUMBER                       AS relative_score,
      urgency::FLOAT                               AS urgency

    FROM source

)

SELECT *
FROM renamed
