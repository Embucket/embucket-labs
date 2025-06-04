{{ config(
    materialized='view',
    tags=["mnpi_exception"]
) }}

WITH product_details AS (

    SELECT DISTINCT
      prep_product_detail.product_rate_plan_id,
      prep_product_detail.dim_product_tier_id,
      prep_product_detail.product_delivery_type,
      prep_product_detail.product_deployment_type
    FROM {{ ref('prep_product_detail') }}
    WHERE prep_product_detail.product_deployment_type IN ('Self-Managed', 'Dedicated')

), seat_link AS (

    SELECT
      {{ dbt_utils.generate_surrogate_key(['hostname', 'uuid']) }}                  AS latest_seat_link_installation_sk,
      {{ dbt_utils.generate_surrogate_key(['prep_host.dim_host_id', 'uuid'])}}      AS dim_installation_id,
      zuora_subscription_id                                                         AS dim_subscription_id,
      zuora_subscription_name                                                       AS subscription_name,
      hostname                                                                      AS host_name,
      prep_host.dim_host_id,
      uuid                                                                          AS dim_instance_id,
      {{ get_keyed_nulls('product_details.dim_product_tier_id') }}                  AS dim_product_tier_id,
      product_delivery_type,
      product_deployment_type,
      order_id,
      report_timestamp,
      report_date,
      license_starts_on,
      created_at,
      updated_at,
      active_user_count,
      license_user_count,
      max_historical_user_count
    FROM {{ ref('customers_db_license_seat_links_source') }}
    INNER JOIN {{ ref('prep_order') }}
      ON customers_db_license_seat_links_source.order_id = prep_order.internal_order_id
    LEFT JOIN {{ ref('prep_host') }}
      ON customers_db_license_seat_links_source.hostname = prep_host.host_name
    LEFT OUTER JOIN product_details
      ON prep_order.product_rate_plan_id = product_details.product_rate_plan_id
    QUALIFY ROW_NUMBER() OVER (PARTITION BY host_name, dim_instance_id ORDER BY report_date DESC, updated_at DESC) = 1

)

{{ dbt_audit(
    cte_ref="seat_link",
    created_by="@mdrussell",
    updated_by="@michellecooper",
    created_date="2024-03-06",
    updated_date="2024-10-07"
) }}
