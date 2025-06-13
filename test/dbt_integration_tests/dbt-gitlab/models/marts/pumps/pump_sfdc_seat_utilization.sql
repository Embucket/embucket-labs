{{
  config(
    materialized='table',
    tags=["mnpi_exception"]
  )
}}

{{ simple_cte([
    ('pump_gainsight_metrics_monthly_paid','pump_gainsight_metrics_monthly_paid'),
    ('seat_link','fct_latest_seat_link_installation')
]) }}

, production_installations_namespaces AS (
  SELECT
    delivery_type,
    deployment_type,
    dim_subscription_id_original,
    dim_subscription_id,
    subscription_status,
    ping_created_at,
    cleaned_version AS gitlab_version,
    billable_user_count,
    max_historical_user_count,
    license_user_count,
    hostname,
    uuid,
    dim_installation_id,
    dim_namespace_id
  FROM pump_gainsight_metrics_monthly_paid
  WHERE instance_type = 'Production'
    AND DATE_TRUNC('month', ping_created_at) BETWEEN DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE)) 
      AND DATE_TRUNC('month', GETDATE())
    AND ping_created_at IS NOT NULL
)

, joined AS (
  SELECT 
    production_installations_namespaces.delivery_type,
    production_installations_namespaces.deployment_type,
    production_installations_namespaces.dim_subscription_id_original,
    production_installations_namespaces.dim_subscription_id,
    production_installations_namespaces.subscription_status,
    production_installations_namespaces.ping_created_at,
    production_installations_namespaces.gitlab_version,
    COALESCE(seat_link.active_user_count, production_installations_namespaces.billable_user_count) AS billable_user_count,
    COALESCE(seat_link.max_historical_user_count, production_installations_namespaces.max_historical_user_count) AS max_historical_user_count,
    COALESCE(seat_link.license_user_count, production_installations_namespaces.license_user_count) AS license_user_count
  FROM production_installations_namespaces
  LEFT JOIN seat_link ON seat_link.host_name = production_installations_namespaces.hostname
    AND seat_link.dim_instance_id = production_installations_namespaces.uuid
  QUALIFY ROW_NUMBER() OVER(PARTITION BY production_installations_namespaces.dim_subscription_id_original ORDER BY production_installations_namespaces.billable_user_count DESC) = 1
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2024-03-06",
    updated_date="2024-03-07"
) }}
