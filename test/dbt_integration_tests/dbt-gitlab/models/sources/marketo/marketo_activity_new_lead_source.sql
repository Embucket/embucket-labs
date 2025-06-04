WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_new_lead') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_new_lead_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      modifying_user::TEXT                      AS modifying_user,
      created_date::DATE                        AS created_date,
      api_method_name::TEXT                     AS api_method_name,
      source_type::TEXT                         AS source_type,
      request_id::TEXT                          AS request_id,
      form_name::TEXT                           AS form_name,
      lead_source::TEXT                         AS lead_source,
      sfdc_type::TEXT                           AS sfdc_type,
      list_name::TEXT                           AS list_name

    FROM source

)

SELECT *
FROM renamed
