{{ config(

    materialized='incremental',
    unique_key='suggestion_id',
    tags=['mnpi_exception', 'product'],
    on_schema_change='sync_all_columns'
  )

}}

WITH gitlab_ide_extension_events AS (

  SELECT
    {{ dbt_utils.star(from=ref('mart_behavior_structured_event_code_suggestion'), except=["CREATED_BY",
    "UPDATED_BY","CREATED_DATE","UPDATED_DATE","MODEL_CREATED_DATE","MODEL_UPDATED_DATE","DBT_UPDATED_AT","DBT_CREATED_AT"]) }}
  FROM {{ ref('mart_behavior_structured_event_code_suggestion') }}
  WHERE app_id = 'gitlab_ide_extension' --events that can be used to calculate suggestion outcome
    AND event_label IS NOT NULL --required field in order to stitch the events together
    {% if is_incremental() %}

      AND behavior_at >= (SELECT MAX(requested_at) FROM {{ this }})

    {% endif %}

),

--Visual with event sequence here: https://gitlab.com/gitlab-org/editor-extensions/gitlab-language-server-for-code-suggestions/-/blob/main/docs/telemetry.md

requested AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_requested'

),

loaded AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_loaded'

),

shown AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_shown'

),

accepted AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_accepted'

),

rejected AS (

  SELECT gitlab_ide_extension_events.*
  FROM gitlab_ide_extension_events
  LEFT JOIN accepted
    ON gitlab_ide_extension_events.event_label = accepted.event_label
  WHERE gitlab_ide_extension_events.event_action = 'suggestion_rejected'
    AND accepted.event_label IS NULL --suggestion cannot be accepted and rejected, default to accepted if both present: https://gitlab.com/gitlab-data/product-analytics/-/issues/1410#note_1581747408

),

cancelled AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_cancelled'

),

not_provided AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_not_provided'

),

error AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_error'

),

stream_started AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_stream_started'

),

stream_completed AS (

  SELECT *
  FROM gitlab_ide_extension_events
  WHERE event_action = 'suggestion_stream_completed'

),

event_count_per_action AS (

  --get a count of events per suggestion (event_label) and event_action - there should only be one
  SELECT
    event_label,
    event_action,
    COUNT(*) AS suggestion_action_event_count
  FROM gitlab_ide_extension_events
  GROUP BY 1, 2

),

suggestions_with_duplicate_events AS (

  SELECT DISTINCT event_label
  FROM event_count_per_action
  WHERE suggestion_action_event_count > 1 --more than 1 event per event_action (which should not happen)

),

suggestion_level AS (

  SELECT

    --Suggestion attributes
    requested.event_label                                                                           AS suggestion_id,
    --Edge cases where language on the requested event is NULL or blank (''), fall back to other events to maximize coverage
    CASE
      WHEN requested.language != '' THEN requested.language
      WHEN loaded.language != '' THEN loaded.language
      WHEN accepted.language != '' THEN accepted.language
      WHEN rejected.language != '' THEN rejected.language
      WHEN cancelled.language != '' THEN cancelled.language
    END                                                                                             AS language,
    requested.delivery_type,
    requested.prefix_length,
    requested.suffix_length,
    requested.is_streaming,
    requested.extension_name,
    requested.extension_version,
    requested.ide_name,
    requested.ide_vendor,
    requested.ide_version,
    requested.language_server_version,
    requested.is_invoked,
    requested.has_advanced_context,
    requested.gitlab_global_user_id,
    requested.ultimate_parent_namespace_ids,
    requested.dim_installation_ids,
    requested.dim_crm_account_ids,
    requested.crm_account_names,
    requested.namespace_is_internal,
    requested.installation_is_internal,
    requested.product_deployment_type,
    requested.gsc_instance_version,
    requested.total_context_size_bytes,
    requested.content_above_cursor_size_bytes,
    requested.content_below_cursor_size_bytes,
    requested.context_items,
    requested.context_items_count,
    requested.debounce_interval,
    requested.region,
    requested.resolution_strategy,

    --Fields that are not available on requested event. If not limited to a single possible event type, default to loaded event, fall back to others to maximize coverage
    accepted.accepted_option,
    COALESCE(loaded.model_engine, shown.model_engine,
      accepted.model_engine, rejected.model_engine,
      cancelled.model_engine)                                                                       AS model_engine,
    COALESCE(loaded.model_name, shown.model_name,
      accepted.model_name, rejected.model_name,
      cancelled.model_name)                                                                         AS model_name,
    COALESCE(loaded.suggestion_source, shown.suggestion_source,
      accepted.suggestion_source, rejected.suggestion_source,
      cancelled.suggestion_source, not_provided.suggestion_source)                                  AS suggestion_source,
    COALESCE(loaded.options_count, shown.options_count,
      accepted.options_count, rejected.options_count,
      cancelled.options_count, not_provided.options_count)                                          AS options_count,
    COALESCE(loaded.is_direct_connection, shown.is_direct_connection,
      accepted.is_direct_connection, rejected.is_direct_connection,
      cancelled.is_direct_connection)                                                               AS is_direct_connection,
    COALESCE(loaded.input_tokens, shown.input_tokens,
      accepted.input_tokens, rejected.input_tokens,
      cancelled.input_tokens)                                                                       AS input_tokens,
    COALESCE(loaded.output_tokens, shown.output_tokens,
      accepted.output_tokens, rejected.output_tokens,
      cancelled.output_tokens)                                                                      AS output_tokens,
    COALESCE(loaded.context_tokens_sent, shown.context_tokens_sent,
      accepted.context_tokens_sent, rejected.context_tokens_sent,
      cancelled.context_tokens_sent)                                                                AS context_tokens_sent,
    COALESCE(loaded.context_tokens_used, shown.context_tokens_used,
      accepted.context_tokens_used, rejected.context_tokens_used,
      cancelled.context_tokens_used)                                                                AS context_tokens_used,

    --Timestamps
    requested.behavior_at                                                                           AS requested_at,
    loaded.behavior_at                                                                              AS loaded_at,
    shown.behavior_at                                                                               AS shown_at,
    accepted.behavior_at                                                                            AS accepted_at,
    rejected.behavior_at                                                                            AS rejected_at,
    cancelled.behavior_at                                                                           AS cancelled_at,
    not_provided.behavior_at                                                                        AS not_provided_at,
    error.behavior_at                                                                               AS error_at,
    stream_started.behavior_at                                                                      AS stream_started_at,
    stream_completed.behavior_at                                                                    AS stream_completed_at,

    --Time calculations
    DATEDIFF('milliseconds', requested_at, loaded_at)                                               AS load_time_in_ms,
    DATEDIFF('milliseconds', shown_at, COALESCE(accepted_at, rejected_at))                          AS display_time_in_ms,
    DATEDIFF('milliseconds', requested_at, stream_started_at)                                       AS stream_start_time_in_ms,
    DATEDIFF('milliseconds', requested_at, shown_at)                                                AS time_to_show_in_ms,

    --Outcome/end result of suggestion
    COALESCE(accepted.event_action, rejected.event_action,
      cancelled.event_action, not_provided.event_action,
      stream_completed.event_action, error.event_action,
      shown.event_action, loaded.event_action,
      stream_started.event_action, requested.event_action)                                          AS suggestion_outcome,

    --Junk dimensions
    IFF(requested.event_label IS NOT NULL, TRUE, FALSE)                                             AS was_requested,
    IFF(loaded.event_label IS NOT NULL, TRUE, FALSE)                                                AS was_loaded,
    IFF(shown.event_label IS NOT NULL, TRUE, FALSE)                                                 AS was_shown,
    IFF(accepted.event_label IS NOT NULL, TRUE, FALSE)                                              AS was_accepted,
    IFF(rejected.event_label IS NOT NULL, TRUE, FALSE)                                              AS was_rejected,
    IFF(cancelled.event_label IS NOT NULL, TRUE, FALSE)                                             AS was_cancelled,
    IFF(not_provided.event_label IS NOT NULL, TRUE, FALSE)                                          AS was_not_provided,
    IFF(error.event_label IS NOT NULL, TRUE, FALSE)                                                 AS was_error,
    IFF(stream_started.event_label IS NOT NULL, TRUE, FALSE)                                        AS was_stream_started,
    IFF(stream_completed.event_label IS NOT NULL, TRUE, FALSE)                                      AS was_stream_completed
  FROM requested
  LEFT JOIN loaded
    ON requested.event_label = loaded.event_label
  LEFT JOIN shown
    ON requested.event_label = shown.event_label
  LEFT JOIN accepted
    ON requested.event_label = accepted.event_label
  LEFT JOIN rejected
    ON requested.event_label = rejected.event_label
  LEFT JOIN cancelled
    ON requested.event_label = cancelled.event_label
  LEFT JOIN not_provided
    ON requested.event_label = not_provided.event_label
  LEFT JOIN error
    ON requested.event_label = error.event_label
  LEFT JOIN suggestions_with_duplicate_events
    ON requested.event_label = suggestions_with_duplicate_events.event_label
  LEFT JOIN stream_started
    ON stream_started.event_label = requested.event_label
  LEFT JOIN stream_completed
    ON stream_completed.event_label = requested.event_label
  WHERE suggestions_with_duplicate_events.event_label IS NULL --exclude suggestions with duplicate events

)

{{ dbt_audit(
    cte_ref="suggestion_level",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2024-04-09",
    updated_date="2025-01-15"
) }}

