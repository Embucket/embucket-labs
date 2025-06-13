WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_push_lead_to_marketo') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_push_lead_to_marketo_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      api_method_name::TEXT                     AS api_method_name,
      modifying_user::TEXT                      AS modifying_user,
      request_id::TEXT                          AS request_id,
      source::TEXT                              AS source

    FROM source

)

SELECT *
FROM renamed
