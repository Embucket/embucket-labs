WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_sfdc_activity_updated') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_sfdc_activity_updated_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      status::TEXT                              AS status,
      description::TEXT                         AS description,
      is_task::BOOLEAN                          AS is_task,
      priority::TEXT                            AS priority,
      activity_owner::TEXT                      AS activity_owner,
      due_date::DATE                            AS due_date

    FROM source

)

SELECT *
FROM renamed
