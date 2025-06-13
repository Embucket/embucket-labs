WITH source AS (

    SELECT *
    FROM {{ source('snowflake_imports', 'usage_ping_payloads') }}

), parsed AS (

    SELECT
      jsontext['active_user_count']::NUMBER                     AS active_user_count,
      jsontext['avg_cycle_analytics']::VARIANT                  AS avg_cycle_analytics,
      jsontext['container_registry_enabled']::BOOLEAN           AS is_container_registry_enabled,
      jsontext['counts']::VARIANT                               AS counts,
      jsontext['database']['adapter']::VARCHAR                  AS database_adapter, 
      jsontext['database']['version']::VARCHAR                  AS database_version,
      jsontext['edition']::VARCHAR                              AS edition, 
      jsontext['elasticsearch_enabled']::BOOLEAN                AS is_elasticsearch_enabled,
      jsontext['geo_enabled']::BOOLEAN                          AS is_geo_enabled,
      jsontext['gitaly']['filesystems']::VARCHAR                AS gitaly_filesystems, 
      jsontext['gitaly']['servers']::NUMBER                     AS gitaly_servers,
      jsontext['gitaly']['version']::VARCHAR                    AS gitaly_version,
      jsontext['gitlab_pages']['enabled']::BOOLEAN              AS is_gitlab_pages_enabled,
      jsontext['gitlab_pages']['version']::VARCHAR              AS gitlab_pages_version, 
      jsontext['gitlab_shared_runners_enabled']::BOOLEAN        AS is_gitlab_shared_runners_enabled,
      COALESCE(
        jsontext['git_version']::VARCHAR, 
        jsontext['git']['version']::VARCHAR
      )                                                         AS git_version,
      jsontext['gravatar_enabled']::BOOLEAN                     AS is_gravatar_enabled,
      jsontext['historical_max_users']::NUMBER                  AS historical_max_users,
      jsontext['hostname']::VARCHAR                             AS hostname,
      jsontext['installation_type']::VARCHAR                    AS installation_type,
      jsontext['ldap_enabled']::BOOLEAN                         AS is_ldap_enabled,
      jsontext['licensee']::VARIANT                             AS licensee,
      jsontext['license_add_ons']::VARIANT                      AS license_add_ons,
      jsontext['license_expires_at']::TIMESTAMP                 AS license_expires_at,
      jsontext['license_md5']::VARCHAR                          AS license_md5,
      jsontext['license_sha256']::VARCHAR                       AS license_sha256,
      jsontext['license_plan']::VARCHAR                         AS license_plan,
      jsontext['license_starts_at']::TIMESTAMP                  AS license_starts_at,
      jsontext['license_trial']::BOOLEAN                        AS is_license_trial,
      jsontext['license_user_count']::NUMBER                    AS license_user_count,
      jsontext['mattermost_enabled']::BOOLEAN                   AS is_mattermost_enabled,
      jsontext['omniauth_enabled']::BOOLEAN                     AS is_omniauth_enabled,
      jsontext['prometheus_metrics_enabled']::BOOLEAN           AS is_prometheus_metrics_enabled,
      jsontext['recorded_at']::TIMESTAMP                        AS recorded_at,
      jsontext['reply_by_email_enabled']::BOOLEAN               AS is_reply_by_email_enabled,
      jsontext['signup_enabled']::BOOLEAN                       AS is_signup_enabled,
      jsontext['usage_activity_by_stage']::VARIANT              AS usage_activity_by_stage,
      jsontext['uuid']::VARCHAR                                 AS uuid,
      jsontext['version']::VARCHAR                              AS version,
      jsontext['license_trial_ends_on']::TIMESTAMP              AS license_trial_ends_on,
      jsontext['web_ide_clientside_preview_enabled']::BOOLEAN   AS is_web_ide_clientside_preview_enabled,
      jsontext['ingress_modsecurity_enabled']::BOOLEAN          AS is_ingress_modsecurity_enabled,
      jsontext['dependency_proxy_enabled']::BOOLEAN             AS is_dependency_proxy_enabled
    FROM source

)

SELECT *
FROM parsed