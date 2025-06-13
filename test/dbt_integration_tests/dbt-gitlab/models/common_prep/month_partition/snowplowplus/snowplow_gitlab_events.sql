{% set year_value = var('year', (run_started_at - modules.datetime.timedelta(1)).strftime('%Y')) | int %}
{% set month_value = var('month', (run_started_at - modules.datetime.timedelta(1)).strftime('%m')) | int %}
{% set start_date = modules.datetime.datetime(year_value, month_value, 1) %}
{% set end_date = (start_date + modules.datetime.timedelta(days=31)).strftime('%Y-%m-01') %}

{{config({
    "materialized":"incremental",
    "unique_key":"event_id",
    "on_schema_change":"sync_all_columns",
    "full_refresh": only_force_full_refresh(),
    "snowflake_warehouse": generate_warehouse_name('XL')
  })
}}

{% set change_form = ['formId','elementId','nodeName','type','elementClasses','value'] %}
{% set submit_form = ['formId','formClasses','elements'] %}
{% set focus_form = ['formId','elementId','nodeName','elementType','elementClasses','value'] %}
{% set link_click = ['elementId','elementClasses','elementTarget','targetUrl','elementContent'] %}
{% set track_timing = ['category','variable','timing','label'] %}


WITH filtered_source as (

    SELECT
      app_id,
      base_currency,
      br_colordepth,
      br_cookies,
      br_family,
      br_features_director,
      br_features_flash,
      br_features_gears,
      br_features_java,
      br_features_pdf,
      br_features_quicktime,
      br_features_realplayer,
      br_features_silverlight,
      br_features_windowsmedia,
      br_lang,
      br_name,
      br_renderengine,
      br_type,
      br_version,
      br_viewheight,
      br_viewwidth,
      collector_tstamp,
      contexts,
      derived_contexts,
      -- correcting bugs on ruby tracker which was sending wrong timestamp
      -- https://gitlab.com/gitlab-data/analytics/issues/3097
      IFF(DATE_PART('year', TRY_TO_TIMESTAMP(derived_tstamp)) > 1970,
            derived_tstamp, collector_tstamp) AS derived_tstamp,
      doc_charset,
      try_to_numeric(doc_height)              AS doc_height,
      try_to_numeric(doc_width)               AS doc_width,
      domain_sessionid,
      domain_sessionidx,
      domain_userid,
      dvce_created_tstamp,
      dvce_ismobile,
      dvce_screenheight,
      dvce_screenwidth,
      dvce_sent_tstamp,
      dvce_type,
      etl_tags,
      etl_tstamp,
      event,
      event_fingerprint,
      event_format,
      event_id,
      event_name,
      event_vendor,
      event_version,
      geo_city,
      geo_country,
      geo_region,
      geo_region_name,
      geo_timezone,
      ip_domain,
      ip_isp,
      ip_netspeed,
      ip_organization,
      mkt_campaign,
      mkt_clickid,
      mkt_content,
      mkt_medium,
      mkt_network,
      mkt_source,
      mkt_term,
      name_tracker,
      network_userid,
      os_family,
      os_manufacturer,
      os_name,
      os_timezone,
      page_referrer,
      page_title,
      page_url,
      page_urlfragment,
      page_urlhost,
      page_urlpath,
      page_urlport,
      page_urlquery,
      page_urlscheme,
      platform,
      try_to_numeric(pp_xoffset_max)          AS pp_xoffset_max,
      try_to_numeric(pp_xoffset_min)          AS pp_xoffset_min,
      try_to_numeric(pp_yoffset_max)          AS pp_yoffset_max,
      try_to_numeric(pp_yoffset_min)          AS pp_yoffset_min,
      refr_domain_userid,
      refr_dvce_tstamp,
      refr_medium,
      refr_source,
      refr_term,
      refr_urlfragment,
      refr_urlhost,
      refr_urlpath,
      refr_urlport,
      refr_urlquery,
      refr_urlscheme,
      se_action,
      se_category,
      se_label,
      se_property,
      se_value,
      ti_category,
      ti_currency,
      ti_name,
      ti_orderid,
      ti_price,
      ti_price_base,
      ti_quantity,
      ti_sku,
      tr_affiliation,
      tr_city,
      tr_country,
      tr_currency,
      tr_orderid,
      tr_shipping,
      tr_shipping_base,
      tr_state,
      tr_tax,
      tr_tax_base,
      tr_total,
      tr_total_base,
      true_tstamp,
      txn_id,
      unstruct_event,
      user_fingerprint,
      user_id,
      useragent,
      v_collector,
      v_etl,
      v_tracker,
      uploaded_at,
      'GitLab' AS infra_source
    {% if target.name not in ("prod") -%}

    FROM {{ ref('snowplow_gitlab_good_events_sample_source') }} -- The sample is not always from the current month so given then WHERE conditions this may be a blank tabel

    {%- else %}

    FROM {{ ref('snowplow_gitlab_good_events_source') }}

    {%- endif %}

    WHERE app_id IS NOT NULL
      AND TRY_TO_TIMESTAMP(derived_tstamp) IS NOT NULL
      AND derived_tstamp >= '{{ start_date }}'
      AND derived_tstamp < '{{ end_date }}'
      AND uploaded_at < '{{ run_started_at }}'
      AND
        (
          (
            -- js backend tracker
            v_tracker LIKE 'js%'
            AND COALESCE(lower(page_url), '') NOT LIKE 'http://localhost:%'
          )

          OR

          (
            -- ruby backend tracker
            v_tracker LIKE 'rb%'
          )

          OR

          (
            -- code suggestions events
            v_tracker LIKE 'py%'
          )

          OR

          (
            -- jetbrains plugin events
            v_tracker LIKE 'java%'
          )
        )
        -- removing it after approval from @rparker2 in this issue: https://gitlab.com/gitlab-data/analytics/-/issues/9112

        AND IFF(event_name IN ('submit_form', 'focus_form', 'change_form') AND TRY_TO_TIMESTAMP(derived_tstamp) < '2021-05-26'
            , FALSE
            , TRUE)
    {% if is_incremental() %}

      AND TRY_TO_TIMESTAMP(derived_tstamp) > (SELECT MAX(derived_tstamp) FROM {{this}})

    {% endif %}

)

, base AS (

    SELECT
      *,
      derived_tstamp::DATE AS derived_tstamp_date
    FROM filtered_source fe1
    WHERE NOT EXISTS (
      SELECT 1
      FROM filtered_source fe2
      WHERE fe1.event_id = fe2.event_id
      GROUP BY event_id
      HAVING COUNT(*) > 1
    )

), events_with_flattened_context AS (

    SELECT *
    FROM {{ ref('snowplow_gitlab_events_context_flattened') }}


), base_with_sorted_columns AS (

    SELECT
      base.app_id,
      base.base_currency,
      base.br_colordepth,
      base.br_cookies,
      base.br_family,
      base.br_features_director,
      base.br_features_flash,
      base.br_features_gears,
      base.br_features_java,
      base.br_features_pdf,
      base.br_features_quicktime,
      base.br_features_realplayer,
      base.br_features_silverlight,
      base.br_features_windowsmedia,
      base.br_lang,
      base.br_name,
      base.br_renderengine,
      base.br_type,
      base.br_version,
      base.br_viewheight,
      base.br_viewwidth,
      base.collector_tstamp,
      base.contexts,
      base.derived_contexts,
      base.derived_tstamp,
      base.doc_charset,
      base.doc_height,
      base.doc_width,
      base.domain_sessionid,
      base.domain_sessionidx,
      base.domain_userid,
      base.dvce_created_tstamp,
      base.dvce_ismobile,
      base.dvce_screenheight,
      base.dvce_screenwidth,
      base.dvce_sent_tstamp,
      base.dvce_type,
      base.etl_tags,
      base.etl_tstamp,
      base.event,
      base.event_fingerprint,
      base.event_format,
      base.event_id,
      base.event_name,
      base.event_vendor,
      base.event_version,
      base.geo_city,
      base.geo_country,
      base.geo_region,
      base.geo_region_name,
      base.geo_timezone,
      base.ip_domain,
      base.ip_isp,
      base.ip_netspeed,
      base.ip_organization,
      base.mkt_campaign,
      base.mkt_clickid,
      base.mkt_content,
      base.mkt_medium,
      base.mkt_network,
      base.mkt_source,
      base.mkt_term,
      base.name_tracker,
      base.network_userid,
      base.os_family,
      base.os_manufacturer,
      base.os_name,
      base.os_timezone,
      base.page_referrer,
      base.page_title,
      base.page_url,
      base.page_urlfragment,
      base.page_urlhost,
      base.page_urlpath,
      base.page_urlport,
      base.page_urlquery,
      base.page_urlscheme,
      base.platform,
      base.pp_xoffset_max,
      base.pp_xoffset_min,
      base.pp_yoffset_max,
      base.pp_yoffset_min,
      base.refr_domain_userid,
      base.refr_dvce_tstamp,
      base.refr_medium,
      base.refr_source,
      base.refr_term,
      base.refr_urlfragment,
      base.refr_urlhost,
      base.refr_urlpath,
      base.refr_urlport,
      base.refr_urlquery,
      base.refr_urlscheme,
      base.se_action,
      base.se_category,
      base.se_label,
      base.se_property,
      base.se_value,
      base.ti_category,
      base.ti_currency,
      base.ti_name,
      base.ti_orderid,
      base.ti_price,
      base.ti_price_base,
      base.ti_quantity,
      base.ti_sku,
      base.tr_affiliation,
      base.tr_city,
      base.tr_country,
      base.tr_currency,
      base.tr_orderid,
      base.tr_shipping,
      base.tr_shipping_base,
      base.tr_state,
      base.tr_tax,
      base.tr_tax_base,
      base.tr_total,
      base.tr_total_base,
      base.true_tstamp,
      base.txn_id,
      base.unstruct_event,
      base.user_fingerprint,
      base.user_id,
      base.useragent,
      base.v_collector,
      base.v_etl,
      base.v_tracker,
      base.uploaded_at,
      base.infra_source,
      CASE
        WHEN app_id = 'gitlab-staging' THEN TRUE
        WHEN LOWER(page_url) LIKE 'https://staging.gitlab.com/%' THEN TRUE
        WHEN LOWER(page_url) LIKE 'https://customers.stg.gitlab.com/%' THEN TRUE
        ELSE FALSE
      END AS is_staging_event,
      events_with_flattened_context.web_page_context,
      events_with_flattened_context.has_web_page_context,
      events_with_flattened_context.web_page_id,
      events_with_flattened_context.gitlab_standard_context,
      events_with_flattened_context.has_gitlab_standard_context,
      events_with_flattened_context.environment                AS gsc_environment,
      events_with_flattened_context.extra                      AS gsc_extra,
      events_with_flattened_context.namespace_id               AS gsc_namespace_id,
      events_with_flattened_context.plan                       AS gsc_plan,
      events_with_flattened_context.google_analytics_client_id AS gsc_google_analytics_client_id,
      events_with_flattened_context.project_id                 AS gsc_project_id,
      events_with_flattened_context.pseudonymized_user_id      AS gsc_pseudonymized_user_id,
      events_with_flattened_context.source                     AS gsc_source,
      events_with_flattened_context.is_gitlab_team_member      AS gsc_is_gitlab_team_member,
      events_with_flattened_context.feature_enabled_by_namespace_ids AS gsc_feature_enabled_by_namespace_ids,
      events_with_flattened_context.gitlab_experiment_context,
      events_with_flattened_context.has_gitlab_experiment_context,
      events_with_flattened_context.experiment_name,
      events_with_flattened_context.experiment_context_key,
      events_with_flattened_context.experiment_variant,
      events_with_flattened_context.experiment_migration_keys,
      events_with_flattened_context.ide_extension_version_context,
      events_with_flattened_context.has_ide_extension_version_context,
      events_with_flattened_context.extension_name,
      events_with_flattened_context.extension_version,
      events_with_flattened_context.ide_name,
      events_with_flattened_context.ide_vendor,
      events_with_flattened_context.ide_version,
      events_with_flattened_context.language_server_version,
      events_with_flattened_context.code_suggestions_context,
      events_with_flattened_context.has_code_suggestions_context,
      events_with_flattened_context.model_engine,
      events_with_flattened_context.model_name,
      events_with_flattened_context.prefix_length,
      events_with_flattened_context.suffix_length,
      events_with_flattened_context.language,
      events_with_flattened_context.user_agent,
      events_with_flattened_context.delivery_type,
      events_with_flattened_context.api_status_code,
      events_with_flattened_context.duo_namespace_ids,
      events_with_flattened_context.saas_namespace_ids,
      events_with_flattened_context.namespace_ids,
      events_with_flattened_context.instance_id,
      events_with_flattened_context.host_name,
      events_with_flattened_context.is_streaming,
      events_with_flattened_context.gitlab_global_user_id,
      events_with_flattened_context.suggestion_source,
      events_with_flattened_context.is_invoked,
      events_with_flattened_context.options_count,
      events_with_flattened_context.accepted_option,
      events_with_flattened_context.has_advanced_context,
      events_with_flattened_context.is_direct_connection,
      events_with_flattened_context.gitlab_service_ping_context,
      events_with_flattened_context.has_gitlab_service_ping_context,
      events_with_flattened_context.redis_event_name,
      events_with_flattened_context.key_path,
      events_with_flattened_context.data_source,
      events_with_flattened_context.performance_timing_context,
      events_with_flattened_context.performance_timing_context_schema,
      events_with_flattened_context.has_performance_timing_context,
      events_with_flattened_context.connect_end,
      events_with_flattened_context.connect_start,
      events_with_flattened_context.dom_complete,
      events_with_flattened_context.dom_content_loaded_event_end,
      events_with_flattened_context.dom_content_loaded_event_start,
      events_with_flattened_context.dom_interactive,
      events_with_flattened_context.dom_loading,
      events_with_flattened_context.domain_lookup_end,
      events_with_flattened_context.domain_lookup_start,
      events_with_flattened_context.fetch_start,
      events_with_flattened_context.load_event_end,
      events_with_flattened_context.load_event_start,
      events_with_flattened_context.navigation_start,
      events_with_flattened_context.redirect_end,
      events_with_flattened_context.redirect_start,
      events_with_flattened_context.request_start,
      events_with_flattened_context.response_end,
      events_with_flattened_context.response_start,
      events_with_flattened_context.secure_connection_start,
      events_with_flattened_context.unload_event_end,
      events_with_flattened_context.unload_event_start,
      events_with_flattened_context.instance_version,
      events_with_flattened_context.correlation_id,
      events_with_flattened_context.total_context_size_bytes,
      events_with_flattened_context.content_above_cursor_size_bytes,
      events_with_flattened_context.content_below_cursor_size_bytes,
      events_with_flattened_context.context_items,
      events_with_flattened_context.context_items_count,
      events_with_flattened_context.input_tokens,
      events_with_flattened_context.output_tokens,
      events_with_flattened_context.context_tokens_sent,
      events_with_flattened_context.context_tokens_used,
      events_with_flattened_context.debounce_interval,
      events_with_flattened_context.interface,
      events_with_flattened_context.client_type,
      events_with_flattened_context.client_name,
      events_with_flattened_context.client_version,
      events_with_flattened_context.feature_category,
      events_with_flattened_context.region,
      events_with_flattened_context.resolution_strategy

    FROM base
    LEFT JOIN events_with_flattened_context
      ON base.derived_tstamp_date = events_with_flattened_context.derived_tstamp_date
        AND base.event_id = events_with_flattened_context.event_id
    WHERE NOT EXISTS (
      SELECT event_id
      FROM events_with_flattened_context web_page_events
      WHERE events_with_flattened_context.web_page_id IS NOT NULL
        AND events_with_flattened_context.event_id = web_page_events.event_id
      GROUP BY event_id HAVING COUNT(1) > 1

    )

), unnested_unstruct as (

    SELECT *,
    {{dbt_utils.get_url_parameter(field='page_urlquery', url_parameter='glm_source')}} AS glm_source,
    CASE
      WHEN LENGTH(unstruct_event) > 0 AND TRY_PARSE_JSON(unstruct_event) IS NULL
        THEN TRUE
      ELSE FALSE END AS is_bad_unstruct_event,
    {{ unpack_unstructured_event(change_form, 'change_form', 'cf') }},
    {{ unpack_unstructured_event(submit_form, 'submit_form', 'sf') }},
    {{ unpack_unstructured_event(focus_form, 'focus_form', 'ff') }},
    {{ unpack_unstructured_event(link_click, 'link_click', 'lc') }},
    {{ unpack_unstructured_event(track_timing, 'track_timing', 'tt') }}
    FROM base_with_sorted_columns


)


SELECT *
FROM unnested_unstruct
