{{ simple_cte([
    ('iterable_event_source', 'iterable_event_source') 
]) }},

final AS (

  SELECT DISTINCT
  -- Ids
    {{ dbt_utils.generate_surrogate_key(['iterable_event_fivetran_id','iterable_user_fivetran_id']) }} AS iterable_event_sk,

    -- Event Info
    iterable_event_name,
    iterable_event_ip,
    is_iterable_custom_event,
    iterable_event_recipient_state,
    iterable_event_status,
    iterable_event_unsubscribe_source,
    iterable_event_user_agent,
    iterable_event_transactional_data
  FROM iterable_event_source

)

SELECT *
FROM final
