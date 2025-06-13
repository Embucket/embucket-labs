

WITH events AS (

    SELECT *
    FROM {{ref('snowplow_unnested_events')}}
    WHERE event = 'struct'
      AND is_staging_event = FALSE


), parsed_timing AS (

    SELECT
      connect_end,
      connect_start,
      dom_complete,
      dom_content_loaded_event_end,
      dom_content_loaded_event_start,
      dom_interactive,
      dom_loading,
      domain_lookup_end,
      domain_lookup_start,
      fetch_start,
      load_event_end,
      load_event_start,
      navigation_start,
      redirect_end,
      redirect_start,
      request_start,
      response_end,
      response_start,
      secure_connection_start,
      unload_event_end,
      unload_event_start,
      event_id                          AS root_id,
      derived_tstamp                    AS root_tstamp
    FROM events
)

SELECT *
FROM parsed_timing