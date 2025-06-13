WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_email_bounced_soft') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_email_bounced_soft_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      campaign_run_id::NUMBER                   AS campaign_run_id,
      category::NUMBER                          AS category,
      email::TEXT                               AS email,
      details::TEXT                             AS details,
      subcategory::NUMBER                       AS subcategory,
      step_id::NUMBER                           AS step_id,
      choice_number::NUMBER                     AS choice_number,
      test_variant::NUMBER                      AS test_variant

    FROM source

)

SELECT *
FROM renamed
