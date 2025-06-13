{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "table"
) }}

{{ simple_cte([
    ('dim_date', 'dim_date'),
    ('prep_ping_instance', 'prep_ping_instance'),
    ('current_flags', 'prep_ping_instance_date_flags')
    ])

}},

usage_data_w_date AS (

  SELECT
    prep_ping_instance.dim_ping_instance_id                                                                       AS dim_ping_instance_id,
    dim_date.date_id                                                                                              AS dim_ping_date_id,
    prep_ping_instance.dim_host_id                                                                                AS dim_host_id,
    prep_ping_instance.dim_instance_id                                                                            AS dim_instance_id,
    prep_ping_instance.dim_installation_id                                                                        AS dim_installation_id,
    prep_ping_instance.ping_created_at                                                                            AS ping_created_at,
    DATEADD('days', -28, dim_date.date_actual) AS ping_created_date_28_days_earlier,
    dim_date.first_day_of_year AS ping_created_date_year,
    dim_date.first_day_of_month AS ping_created_date_month,
    dim_date.first_day_of_week AS ping_created_date_week,
    dim_date.date_actual AS ping_created_date,
    prep_ping_instance.ip_address_hash                                                                            AS ip_address_hash,
    prep_ping_instance.version                                                                                    AS version,
    prep_ping_instance.instance_user_count                                                                        AS instance_user_count,
    prep_ping_instance.license_md5                                                                                AS license_md5,
    prep_ping_instance.license_sha256                                                                             AS license_sha256,
    prep_ping_instance.historical_max_users                                                                       AS historical_max_users,
    prep_ping_instance.license_user_count                                                                         AS license_user_count,
    prep_ping_instance.license_starts_at                                                                          AS license_starts_at,
    prep_ping_instance.license_expires_at                                                                         AS license_expires_at,
    prep_ping_instance.license_add_ons                                                                            AS license_add_ons,
    prep_ping_instance.recorded_at                                                                                AS recorded_at,
    prep_ping_instance.updated_at                                                                                 AS updated_at,
    prep_ping_instance.mattermost_enabled                                                                         AS mattermost_enabled,
    prep_ping_instance.main_edition                                                                               AS ping_edition,
    prep_ping_instance.hostname                                                                                   AS host_name,
    prep_ping_instance.product_tier                                                                               AS product_tier,
    prep_ping_instance.license_trial                                                                              AS license_trial,
    prep_ping_instance.raw_usage_data_payload['license_trial_ends_on']::DATE                                      AS license_trial_ends_on,
    IFF(prep_ping_instance.ping_created_at < license_trial_ends_on, TRUE, FALSE)                                  AS is_trial,
    prep_ping_instance.source_license_id                                                                          AS source_license_id,
    prep_ping_instance.installation_type                                                                          AS installation_type,
    prep_ping_instance.license_plan                                                                               AS license_plan,
    prep_ping_instance.database_adapter                                                                           AS database_adapter,
    prep_ping_instance.database_version                                                                           AS database_version,
    prep_ping_instance.git_version                                                                                AS git_version,
    prep_ping_instance.gitlab_pages_enabled                                                                       AS gitlab_pages_enabled,
    prep_ping_instance.gitlab_pages_version                                                                       AS gitlab_pages_version,
    prep_ping_instance.container_registry_enabled                                                                 AS container_registry_enabled,
    prep_ping_instance.elasticsearch_enabled                                                                      AS elasticsearch_enabled,
    prep_ping_instance.geo_enabled                                                                                AS geo_enabled,
    prep_ping_instance.gitlab_shared_runners_enabled                                                              AS gitlab_shared_runners_enabled,
    prep_ping_instance.gravatar_enabled                                                                           AS gravatar_enabled,
    prep_ping_instance.ldap_enabled                                                                               AS ldap_enabled,
    prep_ping_instance.omniauth_enabled                                                                           AS omniauth_enabled,
    prep_ping_instance.reply_by_email_enabled                                                                     AS reply_by_email_enabled,
    prep_ping_instance.signup_enabled                                                                             AS signup_enabled,
    prep_ping_instance.prometheus_metrics_enabled                                                                 AS prometheus_metrics_enabled,
    prep_ping_instance.usage_activity_by_stage                                                                    AS usage_activity_by_stage,
    prep_ping_instance.usage_activity_by_stage_monthly                                                            AS usage_activity_by_stage_monthly,
    prep_ping_instance.gitaly_clusters                                                                            AS gitaly_clusters,
    prep_ping_instance.gitaly_version                                                                             AS gitaly_version,
    prep_ping_instance.gitaly_servers                                                                             AS gitaly_servers,
    prep_ping_instance.gitaly_filesystems                                                                         AS gitaly_filesystems,
    prep_ping_instance.gitpod_enabled                                                                             AS gitpod_enabled,
    prep_ping_instance.object_store                                                                               AS object_store,
    prep_ping_instance.is_dependency_proxy_enabled                                                                AS is_dependency_proxy_enabled,
    prep_ping_instance.recording_ce_finished_at                                                                   AS recording_ce_finished_at,
    prep_ping_instance.recording_ee_finished_at                                                                   AS recording_ee_finished_at,
    prep_ping_instance.is_ingress_modsecurity_enabled                                                             AS is_ingress_modsecurity_enabled,
    prep_ping_instance.topology                                                                                   AS topology,
    prep_ping_instance.is_grafana_link_enabled                                                                    AS is_grafana_link_enabled,
    prep_ping_instance.analytics_unique_visits                                                                    AS analytics_unique_visits,
    prep_ping_instance.raw_usage_data_id                                                                          AS raw_usage_data_id,
    prep_ping_instance.container_registry_vendor                                                                  AS container_registry_vendor,
    prep_ping_instance.container_registry_version                                                                 AS container_registry_version,
    IFF(prep_ping_instance.license_expires_at >= prep_ping_instance.ping_created_at
        OR prep_ping_instance.license_expires_at IS NULL, prep_ping_instance.main_edition, 'EE Free')             AS cleaned_edition,
    prep_ping_instance.is_dedicated_metric                                                                        AS is_dedicated_metric,
    prep_ping_instance.is_dedicated_hostname                                                                      AS is_dedicated_hostname,
    prep_ping_instance.is_saas_dedicated                                                                          AS is_saas_dedicated,
    prep_ping_instance.ping_delivery_type                                                                         AS ping_delivery_type,
    prep_ping_instance.ping_deployment_type                                                                       AS ping_deployment_type,
    prep_ping_instance.is_internal                                                                                AS is_internal,
    prep_ping_instance.is_staging                                                                                 AS is_staging,
    -- date flags

    current_flags.last_ping_of_month_flag,
    current_flags.last_ping_of_week_flag,
    current_flags.last_ping_of_month_flag                                                                         AS is_last_ping_of_month,
    current_flags.last_ping_of_week_flag                                                                          AS is_last_ping_of_week,
    current_flags.next_ping_uploaded_at,
    current_flags.uploaded_group,

    REPLACE(REPLACE(REPLACE(
                        LOWER((prep_ping_instance.raw_usage_data_payload['settings']['collected_data_categories']::VARCHAR)),
                        '"', ''), '[', ''), ']', '')                                                              AS collected_data_categories,
    prep_ping_instance.raw_usage_data_payload,

    -- versions
    REGEXP_REPLACE(NULLIF(prep_ping_instance.version, ''), '[^0-9.]+')                                            AS cleaned_version,
    IFF(prep_ping_instance.version ILIKE '%-pre', True, False)                                                    AS version_is_prerelease,
    SPLIT_PART(cleaned_version, '.', 1)::NUMBER                                                                   AS major_version,
    SPLIT_PART(cleaned_version, '.', 2)::NUMBER                                                                   AS minor_version,
    major_version || '.' || minor_version                                                                         AS major_minor_version,
    major_version * 100 + minor_version                                                                           AS major_minor_version_id, -- legacy field - to be replaced with major_minor_version_ num
    major_version * 100 + minor_version                                                                           AS major_minor_version_num

  FROM prep_ping_instance
  LEFT JOIN dim_date
    ON {{ get_date_id('prep_ping_instance.ping_created_at') }} = dim_date.date_id
  LEFT JOIN current_flags
    ON current_flags.dim_ping_instance_id = prep_ping_instance.dim_ping_instance_id

)

{{ dbt_audit(
    cte_ref="usage_data_w_date",
    created_by="@icooper-acp",
    updated_by="@mdrussell",
    created_date="2022-03-08",
    updated_date="2025-01-16"
) }}
