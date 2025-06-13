{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{
    config({
        "materialized": "table"
    })
}}

{{ simple_cte([
    ('fct_ping_instance', 'fct_ping_instance'),
    ('dim_ping_instance','dim_ping_instance')

]) }}

, health_score_metrics AS (
    SELECT metrics_path
    FROM {{ ref('dim_ping_metric') }}
    WHERE is_health_score_metric = TRUE
)

, fct_ping_instance_metric_with_license  AS (
    SELECT *
    FROM {{ ref('fct_ping_instance_metric') }}
    WHERE (license_md5 IS NOT NULL OR
           license_sha256 IS NOT NULL)
      AND dim_ping_instance_id != '65260503' -- correcting for DQ issue: https://gitlab.com/gitlab-data/analytics/-/merge_requests/10180#note_1935896779
)

, final AS (

    SELECT

    fct_ping_instance_metric_with_license.dim_ping_instance_id                                 AS dim_ping_instance_id,
    fct_ping_instance_metric_with_license.dim_instance_id                                      AS dim_instance_id,
    fct_ping_instance_metric_with_license.dim_host_id                                          AS dim_host_id,
    fct_ping_instance_metric_with_license.license_md5                                          AS license_md5,
    fct_ping_instance_metric_with_license.license_sha256                                       AS license_sha256,
    fct_ping_instance_metric_with_license.dim_subscription_id                                  AS dim_subscription_id,
    fct_ping_instance_metric_with_license.dim_license_id                                       AS dim_license_id,
    fct_ping_instance.dim_crm_account_id                                                       AS dim_crm_account_id,
    fct_ping_instance.dim_parent_crm_account_id                                                AS dim_parent_crm_account_id,
    fct_ping_instance_metric_with_license.dim_location_country_id                              AS dim_location_country_id,
    fct_ping_instance_metric_with_license.dim_product_tier_id                                  AS dim_product_tier_id,
    fct_ping_instance_metric_with_license.dim_ping_date_id                                     AS dim_ping_date_id,
    fct_ping_instance_metric_with_license.dim_installation_id                                  AS dim_installation_id,
    fct_ping_instance_metric_with_license.dim_subscription_license_id                          AS dim_subscription_license_id,
    fct_ping_instance.license_user_count                                                       AS license_user_count,
    fct_ping_instance.installation_creation_date                                               AS installation_creation_date,
    fct_ping_instance.license_billable_users                                                   AS license_billable_users,
    fct_ping_instance.historical_max_user_count                                                AS historical_max_user_count,
    fct_ping_instance.instance_user_count                                                      AS instance_user_count,
    fct_ping_instance_metric_with_license.metrics_path                                         AS metrics_path,
    fct_ping_instance_metric_with_license.metric_value                                         AS metric_value,
    fct_ping_instance_metric_with_license.ping_created_at                                      AS ping_created_at,
    dim_ping_instance.ping_created_date_month                                                  AS ping_created_date_month,
    fct_ping_instance.hostname                                                                 AS hostname,
    fct_ping_instance_metric_with_license.is_license_mapped_to_subscription                    AS is_license_mapped_to_subscription,
    fct_ping_instance_metric_with_license.is_license_mapped_to_subscription                    AS is_license_subscription_id_valid,
    fct_ping_instance_metric_with_license.is_service_ping_license_in_customerDot               AS is_service_ping_license_in_customerDot,
    dim_ping_instance.ping_delivery_type                                                       AS ping_delivery_type,
    dim_ping_instance.ping_deployment_type                                                     AS ping_deployment_type,
    dim_ping_instance.cleaned_version                                                          AS cleaned_version,
    dim_ping_instance.is_dedicated_metric                                                      AS is_dedicated_metric,
    dim_ping_instance.is_dedicated_hostname                                                    AS is_dedicated_hostname

    FROM fct_ping_instance_metric_with_license
    INNER JOIN health_score_metrics
      ON fct_ping_instance_metric_with_license.metrics_path = health_score_metrics.metrics_path
    LEFT JOIN fct_ping_instance
      ON fct_ping_instance_metric_with_license.dim_ping_instance_id =  fct_ping_instance.dim_ping_instance_id
    LEFT JOIN dim_ping_instance
      ON fct_ping_instance_metric_with_license.dim_ping_instance_id =  dim_ping_instance.dim_ping_instance_id
     WHERE fct_ping_instance_metric_with_license.dim_subscription_id IS NOT NULL

), pivoted AS (

    SELECT
      dim_ping_instance_id,
      dim_instance_id,
      ping_created_at,
      ping_created_date_month,
      ping_delivery_type,
      ping_deployment_type,
      dim_host_id,
      dim_subscription_id,
      dim_license_id,
      license_md5,
      license_sha256,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      dim_location_country_id,
      dim_product_tier_id,
      dim_ping_date_id,
      dim_installation_id,
      dim_subscription_license_id,
      license_user_count,
      installation_creation_date,
      license_billable_users,
      historical_max_user_count,
      instance_user_count,
      hostname,
      cleaned_version,
      is_dedicated_metric,
      is_dedicated_hostname,
      is_license_mapped_to_subscription,
      is_license_subscription_id_valid,
      is_service_ping_license_in_customerDot,
      {{ ping_instance_wave_metrics() }}

    FROM final
    {{ dbt_utils.group_by(n=30)}}

)

{{ dbt_audit(
    cte_ref="pivoted",
    created_by="@snalamaru",
    updated_by="@mdrussell",
    created_date="2022-07-06",
    updated_date="2024-09-04"
) }}
