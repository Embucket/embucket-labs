{{ config(
    materialized='table',
    tags=["mnpi_exception"]
) }}

WITH final AS (

    SELECT
      latest_seat_link_installation_sk,
      dim_subscription_id,
      subscription_name,
      host_name,
      dim_instance_id,
      order_id,
      report_timestamp,
      report_date,
      license_starts_on,
      created_at,
      updated_at,
      active_user_count,
      license_user_count,
      max_historical_user_count
    FROM {{ ref('prep_latest_seat_link_installation') }}

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2024-03-07",
    updated_date="2024-04-18"
) }}
