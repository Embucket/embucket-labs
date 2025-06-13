WITH source AS (
  
   SELECT *
   FROM {{ source('iterable','event') }}
 
), final AS (
 
    SELECT
        _fivetran_id::VARCHAR           AS iterable_event_fivetran_id,
        _fivetran_user_id::VARCHAR      AS iterable_user_fivetran_id,
        campaign_id::VARCHAR            AS iterable_event_campaign_id,
        message_id::VARCHAR             AS iterable_event_message_id,
        content_id::VARCHAR             AS iterable_event_content_id,
        created_at::TIMESTAMP           AS iterable_event_created_at,
        created_at::DATE                AS iterable_event_created_date,
        event_name::VARCHAR             AS iterable_event_name,
        ip::VARCHAR                     AS iterable_event_ip,
        is_custom_event::BOOLEAN        AS is_iterable_custom_event,
        message_bus_id::VARCHAR         AS iterable_event_message_bus_id,
        message_type_id::VARCHAR        AS iterable_event_message_type_id,
        recipient_state::VARCHAR        AS iterable_event_recipient_state,
        status::VARCHAR                 AS iterable_event_status,
        unsub_source::VARCHAR           AS iterable_event_unsubscribe_source,
        user_agent::VARCHAR             AS iterable_event_user_agent,
        user_agent_device::VARCHAR      AS iterable_event_user_agent_device,
        transactional_data::VARCHAR     AS iterable_event_transactional_data,
        additional_properties::ARRAY    AS iterable_event_additional_properties
    FROM source

)

SELECT *
FROM final
