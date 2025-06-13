{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(1)).strftime('%Y')) | int %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(1)).strftime('%m')) | int %}
{% set start_date = modules.datetime.datetime(year_value, month_value, 1) %}
{% set end_date = (start_date + modules.datetime.timedelta(days=31)).strftime('%Y-%m-01') %}

{{config({
    "materialized":"incremental",
    "unique_key":['event_id', 'derived_tstamp_date'],
    "cluster_by":['derived_tstamp_date'],
    "on_schema_change":"sync_all_columns",
    "full_refresh": only_force_full_refresh(),
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

WITH filtered_source as (

    SELECT
        event_id,
        derived_tstamp::DATE AS derived_tstamp_date,
        contexts
    {% if target.name not in ("prod") -%}

    FROM {{ ref('snowplow_gitlab_good_events_sample_source') }}

    {%- else %}

    FROM {{ ref('snowplow_gitlab_good_events_source') }}

    {%- endif %}

    WHERE TRY_TO_TIMESTAMP(derived_tstamp) IS NOT NULL
      AND derived_tstamp >= '{{ start_date }}'
      AND derived_tstamp < '{{ end_date }}'
      AND uploaded_at < '{{ run_started_at }}'
    {% if is_incremental() %}

      AND TRY_TO_TIMESTAMP(derived_tstamp) > (SELECT MAX(derived_tstamp_date) FROM {{this}})

    {% endif %}
)

, base AS (

    SELECT DISTINCT *
    FROM filtered_source

)

, events_with_context_flattened AS (

    SELECT
      base.*,
      f.value['schema']::TEXT     AS context_data_schema,
      f.value['data']             AS context_data
    FROM base,
    lateral flatten(input => TRY_PARSE_JSON(contexts), path => 'data') f

)

, column_selection AS (

    SELECT
      events_with_context_flattened.*,
      -- GitLab Standard Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_standard/jsonschema/%',
          context_name='gitlab_standard',
          field_alias_datatype_list=[
            {'field':'environment'},
            {'field':'extra', 'formula':"TRY_PARSE_JSON(context_data['extra'])", 'data_type':'variant'},
            {'field':'namespace_id', 'data_type':'number'},
            {'field':'plan'},
            {'field':'google_analytics_id'},
            {'field':'project_id', 'data_type':'number'},
            {'field':'user_id', 'alias':'pseudonymized_user_id'},
            {'field':'source'},
            {'field':'is_gitlab_team_member',},
            {'field':'feature_enabled_by_namespace_ids'},
            {'field':'instance_id', 'alias':"gsc_instance_id"},
            {'field':'instance_version'},
            {'field':'host_name', 'alias':"gsc_host_name"},
            {'field':'realm', 'alias':"gsc_realm"},
            {'field':'global_user_id'},
            {'field':'correlation_id'},
            {'field':'interface'},
            {'field':'client_type'},
            {'field':'client_name'},
            {'field':'client_version'},
            {'field':'feature_category'}
            ]
        )
      }},
      IFF(google_analytics_id = '', NULL,
          SPLIT_PART(google_analytics_id, '.', 3) || '.' ||
          SPLIT_PART(google_analytics_id, '.', 4))::VARCHAR                                                                                                   AS google_analytics_client_id,
      CASE
        WHEN gsc_realm IN ('SaaS','saas')
          THEN 'SaaS'
        WHEN gsc_realm IN ('Self-Managed','self-managed')
          THEN 'Self-Managed'
        WHEN gsc_realm IS NULL
          THEN NULL
        ELSE gsc_realm
      END                                                                                                                                                     AS gsc_delivery_type,

      -- Web Page Context Columns
       {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.snowplowanalytics.snowplow/web_page/jsonschema/%',
          context_name='web_page',
          field_alias_datatype_list=[
            {'field':'id', 'alias':'web_page_id'}
            ]
        )
      }},

      -- GitLab Experiment Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_experiment/jsonschema/%',
          context_name='gitlab_experiment',
          field_alias_datatype_list=[
            {'field':'experiment', 'alias':'experiment_name'},
            {'field':'key', 'alias':'experiment_context_key'},
            {'field':'variant', 'alias':'experiment_variant'},
            {'field':'migration_keys', 'formula':"ARRAY_TO_STRING(context_data['migration_keys']::VARIANT, ', ')", 'alias':'experiment_migration_keys'}
            ]
        )
      }},

      -- Code Suggestions Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/code_suggestions_context/jsonschema/%',
          context_name='code_suggestions',
          field_alias_datatype_list=[
            {'field':'model_engine'},
            {'field':'model_name'},
            {'field':'prefix_length', 'data_type':'int'},
            {'field':'suffix_length', 'data_type':'int'},
            {'field':'language'},
            {'field':'user_agent'},
            {'field':'gitlab_realm'},
            {'field':'api_status_code', 'data_type':'int'},
            {'field':'gitlab_saas_namespace_ids', 'alias':'saas_namespace_ids'},
            {'field':'gitlab_saas_duo_pro_namespace_ids', 'alias':'duo_namespace_ids'},
            {'field':'gitlab_instance_id', 'alias':'instance_id'},
            {'field':'gitlab_host_name', 'alias':'host_name'},
            {'field':'is_streaming', 'data_type':'boolean'},
            {'field':'gitlab_global_user_id'},
            {'field':'suggestion_source'},
            {'field':'is_invoked', 'data_type':'boolean'},
            {'field':'options_count', 'formula':"NULLIF(context_data['options_count']::VARCHAR, 'null')", 'data_type':'number', 'alias':'options_count'},
            {'field':'accepted_option', 'data_type':'int'},
            {'field':'has_advanced_context', 'data_type':'boolean'},
            {'field':'is_direct_connection', 'data_type':'boolean'},
            {'field':'gitlab_instance_version'},
            {'field':'total_context_size_bytes', 'data_type':'int'},
            {'field':'content_above_cursor_size_bytes', 'data_type':'int'},
            {'field':'content_below_cursor_size_bytes', 'data_type':'int'},
            {'field':'context_items', 'data_type':'variant'},
            {'field':'input_tokens', 'data_type':'int'},
            {'field':'output_tokens', 'data_type':'int'},
            {'field':'context_tokens_sent', 'data_type':'int'},
            {'field':'context_tokens_used', 'data_type':'int'},
            {'field':'debounce_interval', 'data_type':'int'},
            {'field':'region'},
            {'field': 'resolution_strategy', 'formula':"context_items[0]:resolution_strategy", 'alias': 'resolution_strategy'}
            ]
        )
      }},
      CASE
        WHEN gitlab_realm IN ('SaaS','saas')
          THEN 'SaaS'
        WHEN gitlab_realm IN ('Self-Managed','self-managed')
          THEN 'Self-Managed'
        WHEN gitlab_realm IS NULL
          THEN NULL
        ELSE gitlab_realm
      END                                                                                                                                                     AS delivery_type,
      COALESCE(
        IFF(duo_namespace_ids = '[]', NULL, duo_namespace_ids),
        IFF(saas_namespace_ids = '[]', NULL, saas_namespace_ids)
        )                                                                                                                                                     AS namespace_ids,

      -- IDE Extension Version Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/ide_extension_version/jsonschema/%',
          context_name='ide_extension_version',
          field_alias_datatype_list=[
            {'field':'extension_name'},
            {'field':'extension_version'},
            {'field':'ide_name'},
            {'field':'ide_vendor'},
            {'field':'ide_version'},
            {'field':'language_server_version'}
            ]
        )
      }},

      -- Service Ping Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:com.gitlab/gitlab_service_ping/jsonschema/%',
          context_name='gitlab_service_ping',
          field_alias_datatype_list=[
            {'field':'event_name', 'alias':'redis_event_name'},
            {'field':'key_path'},
            {'field':'data_source'}
            ]
        )
      }},

      -- Performance Timing Context Columns
      {{
        snowplow_schema_field_aliasing(
          schema='iglu:org.w3/PerformanceTiming/jsonschema/%',
          context_name='performance_timing',
          field_alias_datatype_list=[
            {'field':'connectEnd', 'data_type':'int', 'alias':'connect_end'},
            {'field':'connectStart', 'data_type':'int', 'alias':'connect_start'},
            {'field':'domComplete', 'data_type':'int', 'alias':'dom_complete'},
            {'field':'domContentLoadedEventEnd', 'data_type':'int', 'alias':'dom_content_loaded_event_end'},
            {'field':'domContentLoadedEventStart', 'data_type':'int', 'alias':'dom_content_loaded_event_start'},
            {'field':'domInteractive', 'data_type':'int', 'alias':'dom_interactive'},
            {'field':'domLoading', 'data_type':'int', 'alias':'dom_loading'},
            {'field':'domainLookupEnd', 'data_type':'int', 'alias':'domain_lookup_end'},
            {'field':'domainLookupStart', 'data_type':'int', 'alias':'domain_lookup_start'},
            {'field':'fetchStart', 'data_type':'int', 'alias':'fetch_start'},
            {'field':'loadEventEnd', 'data_type':'int', 'alias':'load_event_end'},
            {'field':'loadEventStart', 'data_type':'int', 'alias':'load_event_start'},
            {'field':'navigationStart', 'data_type':'int', 'alias':'navigation_start'},
            {'field':'redirectEnd', 'data_type':'int', 'alias':'redirect_end'},
            {'field':'redirectStart', 'data_type':'int', 'alias':'redirect_start'},
            {'field':'requestStart', 'data_type':'int', 'alias':'request_start'},
            {'field':'responseEnd', 'data_type':'int', 'alias':'response_end'},
            {'field':'responseStart', 'data_type':'int', 'alias':'response_start'},
            {'field':'secureConnectionStart', 'data_type':'int', 'alias':'secure_connection_start'},
            {'field':'unloadEventEnd', 'data_type':'int', 'alias':'unload_event_end'},
            {'field':'unloadEventStart', 'data_type':'int', 'alias':'unload_event_start'}
            ]
        )
      }}

    FROM events_with_context_flattened

)

SELECT

  column_selection.event_id,
  column_selection.derived_tstamp_date,

  MAX(column_selection.gitlab_standard_context)               AS gitlab_standard_context,
  MAX(column_selection.gitlab_standard_context_schema)        AS gitlab_standard_context_schema,
  MAX(column_selection.has_gitlab_standard_context)           AS has_gitlab_standard_context,
  MAX(column_selection.environment)                           AS environment,
  MAX(column_selection.extra)                                 AS extra,
  MAX(column_selection.namespace_id)                          AS namespace_id,
  MAX(column_selection.plan)                                  AS plan,
  MAX(column_selection.google_analytics_id)                   AS google_analytics_id,
  MAX(column_selection.google_analytics_client_id)            AS google_analytics_client_id,
  MAX(column_selection.project_id)                            AS project_id,
  MAX(column_selection.pseudonymized_user_id)                 AS pseudonymized_user_id,
  MAX(column_selection.source)                                AS source,
  MAX(column_selection.is_gitlab_team_member)                 AS is_gitlab_team_member,
  MAX(column_selection.feature_enabled_by_namespace_ids)      AS feature_enabled_by_namespace_ids,
  COALESCE(MAX(column_selection.instance_version), MAX(column_selection.gitlab_instance_version))
                                                              AS instance_version,
  MAX(column_selection.correlation_id)                        AS correlation_id,
  MAX(column_selection.interface)                             AS interface,
  MAX(column_selection.client_type)                           AS client_type,
  MAX(column_selection.client_name)                           AS client_name,
  MAX(column_selection.client_version)                        AS client_version,
  MAX(column_selection.feature_category)                      AS feature_category,

  MAX(column_selection.web_page_context)                      AS web_page_context,
  MAX(column_selection.web_page_context_schema)               AS web_page_context_schema,
  MAX(column_selection.has_web_page_context)                  AS has_web_page_context,
  MAX(column_selection.web_page_id)                           AS web_page_id,

  MAX(column_selection.gitlab_experiment_context)             AS gitlab_experiment_context,
  MAX(column_selection.gitlab_experiment_context_schema)      AS gitlab_experiment_context_schema,
  MAX(column_selection.has_gitlab_experiment_context)         AS has_gitlab_experiment_context,
  MAX(column_selection.experiment_name)                       AS experiment_name,
  MAX(column_selection.experiment_context_key)                AS experiment_context_key,
  MAX(column_selection.experiment_variant)                    AS experiment_variant,
  MAX(column_selection.experiment_migration_keys)             AS experiment_migration_keys,

  MAX(column_selection.code_suggestions_context)              AS code_suggestions_context,
  MAX(column_selection.code_suggestions_context_schema)       AS code_suggestions_context_schema,
  MAX(column_selection.has_code_suggestions_context)          AS has_code_suggestions_context,
  MAX(column_selection.model_engine)                          AS model_engine,
  MAX(column_selection.model_name)                            AS model_name,
  MAX(column_selection.prefix_length)                         AS prefix_length,
  MAX(column_selection.suffix_length)                         AS suffix_length,
  MAX(column_selection.language)                              AS language,
  MAX(column_selection.user_agent)                            AS user_agent,
  COALESCE(MAX(column_selection.gsc_delivery_type),MAX(column_selection.delivery_type))
                                                              AS delivery_type,
  MAX(column_selection.api_status_code)                       AS api_status_code,
  MAX(column_selection.duo_namespace_ids)                     AS duo_namespace_ids,
  MAX(column_selection.saas_namespace_ids)                    AS saas_namespace_ids,
  MAX(column_selection.namespace_ids)                         AS namespace_ids,
  COALESCE(MAX(column_selection.gsc_instance_id),MAX(column_selection.instance_id))
                                                              AS instance_id,
  COALESCE(MAX(column_selection.gsc_host_name), MAX(column_selection.host_name))
                                                              AS host_name,
  MAX(column_selection.is_streaming)                          AS is_streaming,
  COALESCE(MAX(column_selection.global_user_id), MAX(column_selection.gitlab_global_user_id))
                                                              AS gitlab_global_user_id,
  MAX(column_selection.suggestion_source)                     AS suggestion_source,
  MAX(column_selection.is_invoked)                            AS is_invoked,
  MAX(column_selection.options_count)                         AS options_count,
  MAX(column_selection.accepted_option)                       AS accepted_option,
  MAX(column_selection.has_advanced_context)                  AS has_advanced_context,
  MAX(column_selection.is_direct_connection)                  AS is_direct_connection,
  MAX(column_selection.total_context_size_bytes)              AS total_context_size_bytes,
  MAX(column_selection.content_above_cursor_size_bytes)       AS content_above_cursor_size_bytes,
  MAX(column_selection.content_below_cursor_size_bytes)       AS content_below_cursor_size_bytes,
  MAX(column_selection.context_items)                         AS context_items,
  ARRAY_SIZE(MAX(column_selection.context_items))             AS context_items_count,
  MAX(column_selection.input_tokens)                          AS input_tokens,
  MAX(column_selection.output_tokens)                         AS output_tokens,
  MAX(column_selection.context_tokens_sent)                   AS context_tokens_sent,
  MAX(column_selection.context_tokens_used)                   AS context_tokens_used,
  MAX(column_selection.debounce_interval)                     AS debounce_interval,
  MAX(column_selection.region)                                AS region,
  MAX(column_selection.resolution_strategy)                   AS resolution_strategy,

  MAX(column_selection.ide_extension_version_context)         AS ide_extension_version_context,
  MAX(column_selection.ide_extension_version_context_schema)  AS ide_extension_version_context_schema,
  MAX(column_selection.has_ide_extension_version_context)     AS has_ide_extension_version_context,
  MAX(column_selection.extension_name)                        AS extension_name,
  MAX(column_selection.extension_version)                     AS extension_version,
  MAX(column_selection.ide_name)                              AS ide_name,
  MAX(column_selection.ide_vendor)                            AS ide_vendor,
  MAX(column_selection.ide_version)                           AS ide_version,
  MAX(column_selection.language_server_version)               AS language_server_version,

  MAX(column_selection.gitlab_service_ping_context)           AS gitlab_service_ping_context,
  MAX(column_selection.gitlab_service_ping_context_schema)    AS gitlab_service_ping_context_schema,
  MAX(column_selection.has_gitlab_service_ping_context)       AS has_gitlab_service_ping_context,
  MAX(column_selection.redis_event_name)                      AS redis_event_name,
  MAX(column_selection.key_path)                              AS key_path,
  MAX(column_selection.data_source)                           AS data_source,

  MAX(column_selection.performance_timing_context)            AS performance_timing_context,
  MAX(column_selection.performance_timing_context_schema)     AS performance_timing_context_schema,
  MAX(column_selection.has_performance_timing_context)        AS has_performance_timing_context,
  MAX(column_selection.connect_end)                           AS connect_end,
  MAX(column_selection.connect_start)                         AS connect_start,
  MAX(column_selection.dom_complete)                          AS dom_complete,
  MAX(column_selection.dom_content_loaded_event_end)          AS dom_content_loaded_event_end,
  MAX(column_selection.dom_content_loaded_event_start)        AS dom_content_loaded_event_start,
  MAX(column_selection.dom_interactive)                       AS dom_interactive,
  MAX(column_selection.dom_loading)                           AS dom_loading,
  MAX(column_selection.domain_lookup_end)                     AS domain_lookup_end,
  MAX(column_selection.domain_lookup_start)                   AS domain_lookup_start,
  MAX(column_selection.fetch_start)                           AS fetch_start,
  MAX(column_selection.load_event_end)                        AS load_event_end,
  MAX(column_selection.load_event_start)                      AS load_event_start,
  MAX(column_selection.navigation_start)                      AS navigation_start,
  MAX(column_selection.redirect_end)                          AS redirect_end,
  MAX(column_selection.redirect_start)                        AS redirect_start,
  MAX(column_selection.request_start)                         AS request_start,
  MAX(column_selection.response_end)                          AS response_end,
  MAX(column_selection.response_start)                        AS response_start,
  MAX(column_selection.secure_connection_start)               AS secure_connection_start,
  MAX(column_selection.unload_event_end)                      AS unload_event_end,
  MAX(column_selection.unload_event_start)                    AS unload_event_start

FROM column_selection
GROUP BY 1,2

