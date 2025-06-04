{{ config(
    tags=["mnpi_exception","product"]
) }}


{{ simple_cte([
    ('instance_pings', 'dim_ping_instance'),
    ('map_license_account', 'map_license_subscription_account'),
    ('instance_types', 'dim_host_instance_type'),
    ('installations', 'dim_installation')
    ])

}},

/* Filtering data from tables separately on license_md5 & license_sha256 to avoid OR condition due to query performance issues */
   map_license_account_md5 AS (

    SELECT
      license_md5,
      dim_subscription_id,
      dim_crm_account_id,
      dim_parent_crm_account_id
    FROM map_license_account
    WHERE license_md5 IS NOT NULL

), map_license_account_sha256 AS (

    SELECT
      license_sha256,
      dim_subscription_id,
      dim_crm_account_id,
      dim_parent_crm_account_id
    FROM map_license_account
    WHERE license_sha256 IS NOT NULL

), core_instance_pings AS (

    SELECT
      instance_pings.dim_ping_instance_id,
      instance_pings.dim_instance_id,
      instance_pings.dim_host_id,
      instance_pings.dim_installation_id,
      instance_pings.ping_created_at,
      instance_pings.ping_created_date_28_days_earlier,
      instance_pings.ping_created_date_year,
      instance_pings.ping_created_date_month,
      instance_pings.ping_created_date_week,
      instance_pings.ping_created_date,
      installations.installation_creation_date,
      instance_pings.raw_usage_data_id,
      instance_pings.raw_usage_data_payload,
      instance_pings.license_md5,
      instance_pings.license_sha256,
      instance_pings.host_name,
      instance_pings.version,
      instance_pings.ping_edition,
      instance_pings.product_tier,
      instance_pings.installation_type,
      instance_pings.cleaned_version,
      instance_pings.version_is_prerelease,
      instance_pings.major_version,
      instance_pings.minor_version,
      instance_pings.major_minor_version,
      instance_pings.ping_delivery_type,
      instance_pings.ping_deployment_type,
      instance_pings.is_internal,
      instance_pings.is_staging,
      instance_pings.instance_user_count,
      instance_pings.historical_max_users,
      COALESCE(map_license_account_md5.dim_subscription_id, map_license_account_sha256.dim_subscription_id)             AS dim_subscription_id,
      COALESCE(map_license_account_md5.dim_crm_account_id, map_license_account_sha256.dim_crm_account_id)               AS dim_crm_account_id,
      COALESCE(map_license_account_md5.dim_parent_crm_account_id, map_license_account_sha256.dim_parent_crm_account_id) AS dim_parent_crm_account_id
    FROM instance_pings
    LEFT JOIN map_license_account_md5
    ON instance_pings.license_md5 = map_license_account_md5.license_md5
    LEFT JOIN map_license_account_sha256
    ON instance_pings.license_sha256 = map_license_account_sha256.license_sha256
    LEFT JOIN installations
     ON installations.dim_installation_id = instance_pings.dim_installation_id
    WHERE instance_pings.product_tier = 'Free'

), joined AS (

    SELECT

    -- usage ping meta data
    core_instance_pings.dim_ping_instance_id,
    core_instance_pings.ping_created_at,
    core_instance_pings.ping_created_date_28_days_earlier,
    core_instance_pings.ping_created_date_year,
    core_instance_pings.ping_created_date_month,
    core_instance_pings.ping_created_date_week,
    core_instance_pings.ping_created_date,

    -- instance settings
    core_instance_pings.dim_instance_id                                                                                AS uuid,
    core_instance_pings.ping_delivery_type,
    core_instance_pings.ping_deployment_type,
    core_instance_pings.dim_installation_id,
    version                                                                                                            AS instance_version,
    core_instance_pings.cleaned_version,
    core_instance_pings.version_is_prerelease,
    core_instance_pings.major_version,
    core_instance_pings.minor_version,
    core_instance_pings.major_minor_version,
    core_instance_pings.product_tier,
    core_instance_pings.ping_edition,
    core_instance_pings.host_name,
    core_instance_pings.dim_host_id,
    core_instance_pings.installation_type,
    core_instance_pings.is_internal,
    core_instance_pings.is_staging,
    core_instance_pings.installation_creation_date,

    -- instance user statistics
    core_instance_pings.raw_usage_data_payload['license_billable_users']::NUMBER(38,0)                                AS license_billable_users, --does not exist in dim_ping_instance,may need to add it eventually
    instance_user_count,
    historical_max_users,
    license_md5,
    license_sha256,
    instance_types.instance_type,

    core_instance_pings.dim_subscription_id,
    core_instance_pings.dim_crm_account_id,
    core_instance_pings.dim_parent_crm_account_id,

    {{ sales_wave_2_3_metrics() }}

    FROM core_instance_pings
    LEFT JOIN instance_types
      ON core_instance_pings.dim_instance_id = instance_types.instance_uuid
      AND core_instance_pings.host_name = instance_types.instance_hostname
    QUALIFY ROW_NUMBER() OVER (
      PARTITION BY dim_ping_instance_id
        ORDER BY ping_created_at DESC
      ) = 1
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@snalamaru",
    updated_by="@utkarsh060",
    created_date="2023-01-20",
    updated_date="2024-05-10"
) }}
