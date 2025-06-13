{{ config({
    "materialized": "incremental",
    "unique_key": "id",
    "snowflake_warehouse": generate_warehouse_name('XL')
    })
}}

{%- set columns = adapter.get_columns_in_relation( source('version', 'usage_data') ) -%}

WITH source AS (

    SELECT *
    FROM {{ source('version', 'usage_data') }}
    {% if is_incremental() %}
    WHERE _uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
    {% endif %}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY _uploaded_at DESC) = 1

), raw_usage_data_payload AS (

    SELECT
      *,
      OBJECT_CONSTRUCT(
        {% for column in columns %}
          '{{ column.name | lower }}', COALESCE(TRY_PARSE_JSON({{ column.name | lower }}), {{ column.name | lower }}::VARIANT)
          {% if not loop.last %}
            ,
          {% endif %}
        {% endfor %}
      ) AS raw_usage_data_payload_reconstructed
    FROM source

), renamed AS (

    SELECT
        id::VARCHAR                                  AS id,
        source_ip::VARCHAR                           AS source_ip,
        version::VARCHAR                             AS version,
        active_user_count::NUMBER                    AS instance_user_count, -- See issue #4872.
        license_md5::VARCHAR                         AS license_md5,
        license_sha256::VARCHAR                      AS license_sha256,
        historical_max_users::NUMBER                 AS historical_max_users,
        --licensee // removed for PII
        license_user_count::NUMBER                   AS license_user_count,
        TRY_CAST(license_starts_at AS TIMESTAMP)     AS license_starts_at,
        CASE
            WHEN license_expires_at IS NULL                               THEN NULL::TIMESTAMP
            WHEN SPLIT_PART(license_expires_at, '-', 1)::NUMBER > 9999    THEN '9999-12-30 00:00:00.000 +00'::TIMESTAMP
                                                                          ELSE license_expires_at::TIMESTAMP END
                                                     AS license_expires_at,
        PARSE_JSON(license_add_ons)                  AS license_add_ons,
        recorded_at::TIMESTAMP                       AS recorded_at,
        created_at::TIMESTAMP                        AS created_at,
        updated_at::TIMESTAMP                        AS updated_at,
        true::BOOLEAN                  AS mattermost_enabled,
        uuid::VARCHAR                                AS uuid,
        edition::VARCHAR                             AS edition,
        hostname::VARCHAR                            AS hostname,
        host_id::NUMBER                              AS host_id,
        true::BOOLEAN                       AS license_trial,
        source_license_id::NUMBER                    AS source_license_id,
        installation_type::VARCHAR                   AS installation_type,
        license_plan::VARCHAR                        AS license_plan,
        database_adapter::VARCHAR                    AS database_adapter,
        database_version::VARCHAR                    AS database_version,
        git_version::VARCHAR                         AS git_version,
        true::BOOLEAN                AS gitlab_pages_enabled,
        gitlab_pages_version::VARCHAR                AS gitlab_pages_version,
        true::BOOLEAN          AS container_registry_enabled,
        true::BOOLEAN               AS elasticsearch_enabled,
        true::BOOLEAN                         AS geo_enabled,
        true::BOOLEAN       AS gitlab_shared_runners_enabled,
        true::BOOLEAN                    AS gravatar_enabled,
        true::BOOLEAN                        AS ldap_enabled,
        true::BOOLEAN                    AS omniauth_enabled,
        true::BOOLEAN              AS reply_by_email_enabled,
        true::BOOLEAN                      AS signup_enabled,
        --web_ide_commits // was implemented as both a column and in `counts`
        true::BOOLEAN          AS prometheus_metrics_enabled,
        PARSE_JSON(usage_activity_by_stage)          AS usage_activity_by_stage,
        PARSE_JSON(usage_activity_by_stage_monthly)  AS usage_activity_by_stage_monthly,
        gitaly_clusters::NUMBER                      AS gitaly_clusters,
        gitaly_version::VARCHAR                      AS gitaly_version,
        gitaly_servers::NUMBER                       AS gitaly_servers,
        gitaly_filesystems::VARCHAR                  AS gitaly_filesystems,
        gitpod_enabled::VARCHAR                      AS gitpod_enabled,
        PARSE_JSON(object_store)                     AS object_store,
        true::BOOLEAN            AS is_dependency_proxy_enabled,
        recording_ce_finished_at::TIMESTAMP          AS recording_ce_finished_at,
        recording_ee_finished_at::TIMESTAMP          AS recording_ee_finished_at,
        PARSE_JSON(stats)                            AS stats_used,
        stats_used                                   AS counts,
        true::boolean         AS is_ingress_modsecurity_enabled,
        PARSE_JSON(topology)                         AS topology,
        true::BOOLEAN                AS is_grafana_link_enabled,
        PARSE_JSON(analytics_unique_visits)          AS analytics_unique_visits,
        raw_usage_data_id::VARCHAR                   AS raw_usage_data_id,
        container_registry_vendor::VARCHAR           AS container_registry_vendor,
        container_registry_version::VARCHAR          AS container_registry_version,
        raw_usage_data_payload_reconstructed         AS raw_usage_data_payload_reconstructed,
        _uploaded_at::TIMESTAMP                      AS uploaded_at
    FROM raw_usage_data_payload

)

SELECT *
FROM renamed
ORDER BY updated_at
