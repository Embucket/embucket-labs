
{{ simple_cte([
    ('iterable_event_source', 'iterable_event_source') 
]) }},

final AS (

  SELECT
    -- Ids
    {{ dbt_utils.generate_surrogate_key(['iterable_event_fivetran_id','iterable_user_fivetran_id']) }} AS iterable_event_sk,
    iterable_event_fivetran_id,
    iterable_user_fivetran_id,
    iterable_event_campaign_id,
    iterable_event_message_id,
    iterable_event_content_id,
    {{ get_date_id('iterable_event_created_at') }} AS iterable_event_created_at_id,
    iterable_event_message_bus_id,
    iterable_event_message_type_id,
    -- Date
    iterable_event_created_at
  FROM iterable_event_source

)

SELECT *
FROM final
