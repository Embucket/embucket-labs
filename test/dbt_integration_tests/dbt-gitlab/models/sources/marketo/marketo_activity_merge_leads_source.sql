WITH source AS (

    SELECT *
    FROM {{ source('marketo', 'activity_merge_leads') }}

), renamed AS (

    SELECT

      id::NUMBER                                AS marketo_activity_merge_leads_id,
      lead_id::NUMBER                           AS lead_id,
      activity_date::TIMESTAMP_TZ               AS activity_date,
      activity_type_id::NUMBER                  AS activity_type_id,
      campaign_id::NUMBER                       AS campaign_id,
      primary_attribute_value_id::NUMBER        AS primary_attribute_value_id,
      primary_attribute_value::TEXT             AS primary_attribute_value,
      merge_ids::TEXT                           AS merge_ids,
      merged_in_sales::BOOLEAN                  AS merged_in_sales,
      merge_source::TEXT                        AS merge_source,
      master_updated::BOOLEAN                   AS master_updated

    FROM source

)

SELECT *
FROM renamed
