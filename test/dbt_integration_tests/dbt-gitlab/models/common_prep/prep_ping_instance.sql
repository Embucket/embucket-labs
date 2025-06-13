{{ config(
    tags=["product", "mnpi_exception"],
    materialized = "incremental",
    on_schema_change='sync_all_columns',
    unique_key = "dim_ping_instance_id"
) }}


{{ simple_cte([
    ('raw_usage_data', 'version_raw_usage_data_source'),
    ('automated_instance_service_ping', 'instance_combined_metrics'),
    ('internal_installations', 'internal_gitlab_installations')
    ])

}}

, source AS (

    SELECT
      id                                                                        AS dim_ping_instance_id,
      created_at::TIMESTAMP(0)                                                  AS ping_created_at,
      *,
      {{ nohash_sensitive_columns('version_usage_data_source', 'source_ip') }}  AS ip_address_hash
    FROM {{ ref('version_usage_data_source') }} as usage

  {% if is_incremental() %}
          WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{this}})
  {% endif %}

), usage_data AS (

    SELECT
      dim_ping_instance_id                                                                                                    AS dim_ping_instance_id,
      host_id                                                                                                                 AS dim_host_id,
      uuid                                                                                                                    AS dim_instance_id,
      ping_created_at                                                                                                         AS ping_created_at,
      uploaded_at                                                                                                             AS uploaded_at,
      source_ip_hash                                                                                                          AS ip_address_hash,
      edition                                                                                                                 AS original_edition,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), except=['EDITION', 'CREATED_AT', 'SOURCE_IP','UPLOADED_AT']) }}
    FROM source
    WHERE uuid IS NOT NULL
      AND version NOT LIKE ('%VERSION%')

), joined_ping AS (

    SELECT
      dim_ping_instance_id                                                                                                                        AS dim_ping_instance_id,
      dim_host_id                                                                                                                                 AS dim_host_id,
      usage_data.dim_instance_id                                                                                                                  AS dim_instance_id,
      {{ dbt_utils.generate_surrogate_key(['dim_host_id', 'dim_instance_id'])}}                                                                            AS dim_installation_id,
      ping_created_at                                                                                                                             AS ping_created_at,
      usage_data.uploaded_at                                                                                                                      AS uploaded_at,
      ip_address_hash                                                                                                                             AS ip_address_hash,
      original_edition                                                                                                                            AS original_edition,
      {{ dbt_utils.star(from=ref('version_usage_data_source'), relation_alias='usage_data', except=['EDITION', 'CREATED_AT', 'SOURCE_IP','UPLOADED_AT']) }},
      IFF(original_edition = 'CE', 'CE', 'EE')                                                                                                    AS main_edition,
      CASE
        WHEN original_edition = 'CE'                                     THEN 'Free'
        WHEN original_edition = 'EE Free'                                THEN 'Free'
        WHEN license_expires_at < ping_created_at                        THEN 'Free'
        WHEN original_edition = 'EE'                                     THEN 'Starter'
        WHEN original_edition = 'EES'                                    THEN 'Starter'
        WHEN original_edition = 'EEP'                                    THEN 'Premium'
        WHEN original_edition = 'EEU'                                    THEN 'Ultimate'
        ELSE NULL END                                                                                                                             AS product_tier,
      NULLIF(
        COALESCE(
          raw_usage_data.raw_usage_data_payload,
          usage_data.raw_usage_data_payload_reconstructed
        )['gitlab_dedicated'], -1
      )::BOOLEAN AS is_dedicated_metric,
      IFF(hostname LIKE ANY ('%gitlab-dedicated.us%', '%gitlab-dedicated.com%', -- Production instances
                              '%gitlab-dedicated.systems%', '%testpony.net%', '%gitlab-private.org%') -- beta, sandbox, test
          , TRUE, FALSE)                                                                                                                          AS is_dedicated_hostname,
      IFF(is_dedicated_metric = TRUE OR is_dedicated_hostname = TRUE, TRUE, FALSE)                                                                AS is_saas_dedicated,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'SaaS'
        WHEN is_saas_dedicated = TRUE THEN 'SaaS'
        ELSE 'Self-Managed'
      END                                                                                                                                         AS ping_delivery_type,
      CASE
        WHEN uuid = 'ea8bf810-1d6f-4a6a-b4fd-93e8cbd8b57f' THEN 'GitLab.com'
        WHEN is_saas_dedicated = TRUE THEN 'Dedicated'
        ELSE 'Self-Managed'
      END                                                                                                                                         AS ping_deployment_type,
      COALESCE(raw_usage_data.raw_usage_data_payload, usage_data.raw_usage_data_payload_reconstructed)                                            AS raw_usage_data_payload,
    FROM usage_data
    LEFT JOIN raw_usage_data
      ON usage_data.raw_usage_data_id = raw_usage_data.raw_usage_data_id
    WHERE usage_data.ping_created_at  < (SELECT MAX(created_at) FROM raw_usage_data)
      AND NOT(dim_installation_id = '8b52effca410f0a380b0fcffaa1260e7' AND ping_created_at >= '2023-02-19') --excluding GitLab SaaS pings from 2023-02-19 and after

), automated_service_ping AS (

    SELECT
      id                                                AS dim_ping_instance_id,
      host_id                                           AS dim_host_id,
      uuid                                              AS dim_instance_id,
      {{ dbt_utils.generate_surrogate_key(['host_id', 'uuid'])}} AS dim_installation_id,
      created_at                                        AS ping_created_at,
      created_at                                        AS uploaded_at,
      NULL                                              AS ip_address_hash,
      edition                                           AS original_edition,
      id,
      version,
      instance_user_count,
      license_md5,
      license_sha256,
      historical_max_users,
      license_user_count,
      license_starts_at,
      license_expires_at,
      license_add_ons,
      recorded_at,
      updated_at,
      mattermost_enabled,
      uuid,
      hostname,
      host_id,
      license_trial,
      source_license_id,
      installation_type,
      license_plan,
      database_adapter,
      database_version,
      git_version,
      gitlab_pages_enabled,
      gitlab_pages_version,
      container_registry_enabled,
      elasticsearch_enabled,
      geo_enabled,
      gitlab_shared_runners_enabled,
      gravatar_enabled,
      ldap_enabled,
      omniauth_enabled,
      reply_by_email_enabled,
      signup_enabled,
      prometheus_metrics_enabled,
      usage_activity_by_stage,
      usage_activity_by_stage_monthly,
      gitaly_clusters,
      gitaly_version,
      gitaly_servers,
      gitaly_filesystems,
      gitpod_enabled,
      object_store,
      is_dependency_proxy_enabled,
      recording_ce_finished_at,
      recording_ee_finished_at,
      stats_used,
      counts,
      is_ingress_modsecurity_enabled,
      topology,
      is_grafana_link_enabled,
      analytics_unique_visits,
      raw_usage_data_id,
      container_registry_vendor,
      container_registry_version,
      NULL AS raw_usage_data_payload_reconstructed,
      IFF(edition = 'CE', 'CE', 'EE') AS main_edition,
      CASE
        WHEN edition = 'CE'                   THEN 'Free'
        WHEN edition = 'EE Free'              THEN 'Free'
        WHEN license_expires_at < created_at  THEN 'Free'
        WHEN edition = 'EE'                   THEN 'Starter'
        WHEN edition = 'EES'                  THEN 'Starter'
        WHEN edition = 'EEP'                  THEN 'Premium'
        WHEN edition = 'EEU'                  THEN 'Ultimate'
        ELSE NULL
      END AS product_tier,
      FALSE AS is_dedicated_metric,
      FALSE AS is_dedicated_hostname,
      FALSE AS is_saas_dedicated,
      'SaaS' AS ping_delivery_type,
      'GitLab.com' AS ping_deployment_type,
      raw_usage_data_payload
    FROM automated_instance_service_ping
    WHERE created_at >= '2023-02-19' --start using the automated SaaS Service Ping as of 2023-02-19

), combined_ping AS (

    SELECT * FROM joined_ping

    UNION ALL

    SELECT * FROM automated_service_ping

), final AS (

    SELECT
      combined_ping.*,
      raw_usage_data_payload:license_billable_users::NUMBER                            AS license_billable_users,
      TO_DATE(raw_usage_data_payload:license_trial_ends_on::TEXT)                      AS license_trial_ends_on,
      (raw_usage_data_payload:license_subscription_id::TEXT)                           AS license_subscription_id,
      raw_usage_data_payload:usage_activity_by_stage_monthly.manage.events::NUMBER     AS umau_value,
      raw_usage_data_payload:duo_seats.pro_purchased::NUMBER                           AS duo_pro_purchased_seats,
      raw_usage_data_payload:duo_seats.pro_assigned::NUMBER                            AS duo_pro_assigned_seats,
      raw_usage_data_payload:duo_seats.enterprise_purchased::NUMBER                    AS duo_enterprise_purchased_seats,
      raw_usage_data_payload:duo_seats.enterprise_assigned::NUMBER                     AS duo_enterprise_assigned_seats,
      IFF(internal_installations.dim_installation_id IS NOT NULL, TRUE, FALSE)         AS is_internal,
      CASE
        WHEN combined_ping.hostname ILIKE 'staging.%'
          OR combined_ping.hostname = 'dr.gitlab.com' THEN TRUE
        ELSE FALSE 
      END                                                                              AS is_staging

    FROM combined_ping
    LEFT JOIN internal_installations
      ON combined_ping.dim_installation_id = internal_installations.dim_installation_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@icooper-acp",
    updated_by="@utkarsh060",
    created_date="2022-03-17",
    updated_date="2024-10-04"
) }}
