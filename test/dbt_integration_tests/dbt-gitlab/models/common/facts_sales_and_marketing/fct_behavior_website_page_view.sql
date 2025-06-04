{{ config(
        materialized = "incremental",
        unique_key = "fct_behavior_website_page_view_sk",
        on_schema_change='sync_all_columns',
        post_hook=["{{ rolling_window_delete('behavior_at','month',25) }}"],
        tags=['product'],
        snowflake_warehouse=generate_warehouse_name('XL')
) }}

{{
    simple_cte([
    ('page_views', 'prep_snowplow_page_views_all')
    ])
}}

, page_views_w_clean_url AS (

    SELECT
      {{ clean_url('page_url_path') }}                                              AS clean_url_path,
      page_url_path,
      app_id,
      page_url_host,
      gsc_namespace_id                                                              AS dim_namespace_id,
      gsc_project_id                                                                AS dim_project_id,
      session_id,
      user_snowplow_domain_id,
      page_view_id                                                                  AS event_id,
      'page_view'                                                                   AS event_name,
      gsc_environment,
      gsc_extra,
      gsc_google_analytics_client_id,
      gsc_namespace_id,
      gsc_plan,
      gsc_project_id,
      gsc_pseudonymized_user_id,
      gsc_source,
      gsc_is_gitlab_team_member,
      min_tstamp                                                                    AS page_view_start_at,
      max_tstamp                                                                    AS page_view_end_at,
      time_engaged_in_s                                                             AS engaged_seconds,
      total_time_in_ms                                                              AS page_load_time_in_ms,
      page_view_index,
      page_view_in_session_index,
      referer_url_path,
      page_url_original                                                             AS page_url,
      page_referrer                                                                 AS referrer_url,
      page_url_scheme,
      referer_url_scheme,
      IFNULL(geo_city, 'Unknown')::VARCHAR                                          AS user_city,
      IFNULL(geo_country, 'Unknown')::VARCHAR                                       AS user_country,
      IFNULL(geo_region, 'Unknown')::VARCHAR                                        AS user_region,
      IFNULL(geo_region_name, 'Unknown')::VARCHAR                                   AS user_region_name,
      IFNULL(geo_timezone, 'Unknown')::VARCHAR                                      AS user_timezone_name,
      REGEXP_REPLACE(page_url_original, '^https?:\/\/')                             AS page_url_host_path,
      REGEXP_REPLACE(page_referrer, '^https?:\/\/')                                 AS referrer_url_host_path,
    FROM page_views

    {% if is_incremental() %}

    WHERE max_tstamp > (SELECT max(page_view_end_at) FROM {{ this }})

    {% endif %}

), page_views_w_dim AS (

    SELECT
      -- Primary Key
      {{ dbt_utils.generate_surrogate_key(['event_id','page_view_end_at']) }}                               AS fct_behavior_website_page_view_sk,

      -- Foreign Keys
      {{ dbt_utils.generate_surrogate_key(['page_url', 'app_id', 'page_url_scheme']) }}                     AS dim_behavior_website_page_sk,
      {{ dbt_utils.generate_surrogate_key(['referrer_url', 'app_id', 'referer_url_scheme']) }}              AS dim_behavior_referrer_page_sk,
      page_views_w_clean_url.dim_namespace_id,
      page_views_w_clean_url.dim_project_id,

      --Time Attributes
      page_views_w_clean_url.page_view_start_at,
      page_views_w_clean_url.page_view_end_at,
      page_views_w_clean_url.page_view_start_at                                                             AS behavior_at,

      -- Natural Keys
      page_views_w_clean_url.session_id,
      page_views_w_clean_url.event_id,
      page_views_w_clean_url.user_snowplow_domain_id,

      -- GitLab Standard Context
      page_views_w_clean_url.gsc_environment,
      page_views_w_clean_url.gsc_extra,
      page_views_w_clean_url.gsc_google_analytics_client_id,
      page_views_w_clean_url.gsc_plan,
      page_views_w_clean_url.gsc_pseudonymized_user_id,
      page_views_w_clean_url.gsc_source,
      page_views_w_clean_url.gsc_is_gitlab_team_member,
      page_views_w_clean_url.gsc_namespace_id,
      page_views_w_clean_url.gsc_project_id,

      -- User Location Attributes
      page_views_w_clean_url.user_city,
      page_views_w_clean_url.user_country,
      page_views_w_clean_url.user_region,
      page_views_w_clean_url.user_region_name,
      page_views_w_clean_url.user_timezone_name,

      -- Attributes
      page_views_w_clean_url.page_url_path,
      page_views_w_clean_url.page_url,
      page_views_w_clean_url.page_url_host,
      page_views_w_clean_url.referrer_url,
      page_views_w_clean_url.referer_url_path,
      page_views_w_clean_url.event_name,
      NULL                                                                                       AS sf_formid,
      page_views_w_clean_url.engaged_seconds,
      page_views_w_clean_url.page_load_time_in_ms,
      page_views_w_clean_url.page_view_index,
      page_views_w_clean_url.page_view_in_session_index,
      page_views_w_clean_url.page_url_host_path,
      page_views_w_clean_url.clean_url_path
    FROM page_views_w_clean_url

)

{{ dbt_audit(
    cte_ref="page_views_w_dim",
    created_by="@chrissharp",
    updated_by="@michellecooper",
    created_date="2022-07-22",
    updated_date="2024-08-27"
) }}
