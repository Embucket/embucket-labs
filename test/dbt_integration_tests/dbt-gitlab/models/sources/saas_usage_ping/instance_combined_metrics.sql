{{ config({
    "materialized": "table",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

WITH source AS (

  SELECT
    *
  FROM {{ source('saas_usage_ping', 'instance_combined_metrics')}}

),

cleaned AS (

  SELECT
    ping_date::DATE AS ping_date,
    run_id::VARCHAR AS run_id,
    recorded_at::TIMESTAMP AS recorded_at,
    edition::VARCHAR AS edition,
    recording_ce_finished_at::TIMESTAMP AS recording_ce_finished_at,
    recording_ee_finished_at::TIMESTAMP AS recording_ee_finished_at,
    uuid::VARCHAR AS uuid,
    source::VARCHAR AS ping_source,
    PARSE_JSON(query_map) AS query_map,
    PARSE_JSON(run_results) AS run_results,
    DATEADD('s', _uploaded_at, '1970-01-01')::TIMESTAMP AS uploaded_at
  FROM source
  WHERE ping_source = 'combined'

),

final AS (
  SELECT
    run_id::VARCHAR AS id,
    NULL AS source_ip, --currently missing from the Automated Service Ping, will be added later
    recorded_at::TIMESTAMP AS recorded_at,
    uploaded_at::TIMESTAMP AS created_at,
    uploaded_at::TIMESTAMP AS updated_at,
    uuid::VARCHAR AS uuid,
    run_results['historical_max_users']::NUMBER AS historical_max_users,
    edition::VARCHAR AS edition,
    run_id::VARCHAR AS raw_usage_data_id,
    run_results::VARIANT AS raw_usage_data_payload,
    run_results['version']::VARCHAR AS version,
    run_results['active_user_count']::NUMBER AS instance_user_count,
    run_results['license_md5']::VARCHAR AS license_md5,
    run_results['license_sha256']::VARCHAR AS license_sha256,
    run_results['license_user_count']::VARCHAR AS license_user_count,
    run_results['license_starts_at']::TIMESTAMP AS license_starts_at,
    run_results['license_expires_at']::TIMESTAMP AS license_expires_at,
    PARSE_JSON(NULL) AS license_add_ons,
    632::NUMBER AS host_id, -- this is the GitLab host_id
    run_results['mattermost_enabled']::BOOLEAN AS mattermost_enabled,
    run_results['hostname']::VARCHAR AS hostname,
    run_results['license_trial']::BOOLEAN AS license_trial,
    run_results['source_license_id']::NUMBER AS source_license_id,
    run_results['installation_type']::VARCHAR AS installation_type,
    run_results['database']['version']::VARCHAR AS database_version,
    run_results['license_plan']::VARCHAR AS license_plan,
    run_results['database']['adapter']::VARCHAR AS database_adapter,
    CONCAT(
      run_results['git']['version']['major'],
      '.',
      run_results['git']['version']['minor'],
      '.',
      run_results['git']['version']['patch'],
      '.',
      run_results['git']['version']['suffix_s'])::VARCHAR AS git_version,
    run_results['gitlab_pages']['enabled']::BOOLEAN AS gitlab_pages_enabled,
    run_results['gitlab_pages']['version']::VARCHAR AS gitlab_pages_version,
    run_results['container_registry_enabled']::BOOLEAN AS container_registry_enabled,
    run_results['elasticsearch_enabled']::BOOLEAN AS elasticsearch_enabled,
    run_results['geo_enabled']::BOOLEAN AS geo_enabled,
    run_results['gitlab_shared_runners_enabled']::BOOLEAN AS gitlab_shared_runners_enabled,
    run_results['gravatar_enabled']::BOOLEAN AS gravatar_enabled,
    run_results['ldap_enabled']::BOOLEAN AS ldap_enabled,
    run_results['omniauth_enabled']::BOOLEAN AS omniauth_enabled,
    run_results['reply_by_email_enabled']::BOOLEAN AS reply_by_email_enabled,
    run_results['signup_enabled']::BOOLEAN AS signup_enabled,
    run_results['prometheus_metrics_enabled']::BOOLEAN AS prometheus_metrics_enabled,
    run_results['gitaly']['clusters']::NUMBER AS gitaly_clusters,
    PARSE_JSON(run_results['usage_activity_by_stage'])::VARIANT AS usage_activity_by_stage,
    run_results['gitaly']['servers']::NUMBER AS gitaly_servers,
    PARSE_JSON(run_results['usage_activity_by_stage_monthly'])::VARIANT AS usage_activity_by_stage_monthly,
    't'::VARCHAR AS gitpod_enabled, -- matching the data type of prep_ping_instance
    run_results['gitaly']['version']::VARCHAR AS gitaly_version,
    run_results['gitaly']['filesystems']::VARCHAR AS gitaly_filesystems,
    PARSE_JSON(run_results['object_store'])::VARIANT AS object_store,
    run_results['dependency_proxy_enabled']::BOOLEAN AS is_dependency_proxy_enabled,
    run_results['recording_ce_finished_at']::TIMESTAMP AS recording_ce_finished_at,
    run_results['recording_ee_finished_at']::TIMESTAMP AS recording_ee_finished_at,
    PARSE_JSON(run_results['counts'])::VARIANT AS stats_used,
    PARSE_JSON(run_results['counts'])::VARIANT AS counts,
    run_results['ingress_modsecurity_enabled']::BOOLEAN AS is_ingress_modsecurity_enabled,
    PARSE_JSON(run_results['topology'])::VARIANT AS topology,
    run_results['grafana_link_enabled']::BOOLEAN AS is_grafana_link_enabled,
    run_results['container_registry_server']['vendor']::VARCHAR AS container_registry_vendor,
    PARSE_JSON(run_results['analytics_unique_visits'])::VARIANT AS analytics_unique_visits,
    run_results['container_registry_server']['version']::VARCHAR AS container_registry_version
  FROM cleaned
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@utkarsh060",
    created_date="2022-11-09",
    updated_date="2024-08-01"
) }}
