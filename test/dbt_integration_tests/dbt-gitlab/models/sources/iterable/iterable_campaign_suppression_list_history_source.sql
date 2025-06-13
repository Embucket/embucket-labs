WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','campaign_suppression_list_history') }}
 
), final AS (
 
    SELECT
        campaign_id::VARCHAR             AS iterable_campaign_id,
        suppressed_list_id::VARCHAR      AS iterable_campaign_suppressed_list_id,
        updated_at::TIMESTAMP            AS iterable_campaign_list_updated_at,
        updated_at::DATE                 AS iterable_campaign_list_updated_date
    FROM source

)

SELECT *
FROM final
