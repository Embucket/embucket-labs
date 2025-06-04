WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','campaign_history') }}
 
), final AS (
 
    SELECT
        id::VARCHAR                      AS iterable_campaign_id,
        updated_at::TIMESTAMP            AS iterable_campaign_updated_at,
        updated_at::DATE                 AS iterable_campaign_updated_date,
        template_id::VARCHAR             AS iterable_campaign_template_id,
        name::VARCHAR                    AS iterable_campaign_name,
        recurring_campaign_id::VARCHAR   AS iterable_recurring_campaign_id,
        created_at::TIMESTAMP            AS iterable_campaign_created_at,
        created_at::DATE                 AS iterable_campaign_created_date,
        created_by_user_id::VARCHAR      AS iterable_campaign_created_by_user_id,
        ended_at::TIMESTAMP              AS iterable_campaign_ended_at,
        ended_at::DATE                   AS iterable_campaign_ended_date,
        campaign_state::VARCHAR          AS iterable_campaign_state,
        type::VARCHAR                    AS iterable_campaign_type,
        message_medium::VARCHAR          AS iterable_campaign_message_medium,
        workflow_id::VARCHAR             AS iterable_campaign_workflow_id
    FROM source

)

SELECT *
FROM final
