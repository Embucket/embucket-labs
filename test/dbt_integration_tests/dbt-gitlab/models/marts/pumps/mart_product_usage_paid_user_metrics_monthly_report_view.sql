{{
  config(
    tags=["product", "mnpi_exception"],
    schema="common_mart_product",
    materialized="view"
  )
}}

WITH prep AS (
  SELECT
    DISTINCT      dim_subscription_id_original,
                  max(dim_subscription_id) AS dim_subscription_id,
                  max(subscription_status) AS subscription_status,
                  max(ping_created_at) AS ping_created_at,
                  max(cleaned_version) AS gitlab_version,
                  max(billable_user_count) AS billable_user_count,
                  max(max_historical_user_count) AS max_historical_user_count,
                  max(license_user_count) AS license_user_count,
                  min(delivery_type) AS delivery_type,
                  min(deployment_type) AS deployment_type
  FROM {{ ref('mart_product_usage_paid_user_metrics_monthly') }}
  WHERE DATE_TRUNC('month', ping_created_at) = DATEADD('month', -1, DATE_TRUNC('month', CURRENT_DATE))
    AND ping_created_at IS NOT NULL
    AND instance_type = 'Production'
  GROUP BY 1
  ORDER BY 1 DESC
)

SELECT
  delivery_type,
  deployment_type,
  dim_subscription_id_original,
  dim_subscription_id,
  subscription_status,
  ping_created_at,
  gitlab_version,
  billable_user_count,
  max_historical_user_count,
  license_user_count
FROM prep
WHERE license_user_count > 0
