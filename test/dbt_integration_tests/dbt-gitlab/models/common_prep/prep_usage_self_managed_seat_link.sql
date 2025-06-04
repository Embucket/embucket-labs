{{ config(
    tags=["mnpi_exception"]
) }}

WITH raw_seat_link AS (

    SELECT
      *
    FROM {{ ref('customers_db_license_seat_links_source') }}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY zuora_subscription_id, report_date ORDER BY updated_at DESC) = 1

), seat_links AS (

    SELECT
      order_id,
      zuora_subscription_id                                                 AS order_subscription_id,
      TRIM(zuora_subscription_id)                                           AS dim_subscription_id,
      zuora_subscription_name                                               AS subscription_name,
      hostname,
      uuid                                                                  AS dim_instance_id,
      prep_host.dim_host_id,
      {{ dbt_utils.generate_surrogate_key
        (
          [
          'prep_host.dim_host_id', 
          'dim_instance_id'
          ]
        )
      }}                                                                    AS dim_installation_id,
      report_date,
      active_user_count,
      license_user_count,
      max_historical_user_count,
      IFF(ROW_NUMBER() OVER (
            PARTITION BY order_subscription_id
            ORDER BY report_date DESC) = 1,
          TRUE, FALSE)                                                      AS is_last_seat_link_report_per_subscription,
      IFF(ROW_NUMBER() OVER (
            PARTITION BY order_id
            ORDER BY report_date DESC) = 1,
          TRUE, FALSE)                                                      AS is_last_seat_link_report_per_order
    FROM raw_seat_link
    LEFT JOIN {{ ref('prep_host') }}
      ON raw_seat_link.hostname = prep_host.host_name

), customers_orders AS (

    SELECT *
    FROM {{ ref('prep_order') }}

), subscriptions AS (

    SELECT *
    FROM {{ ref('prep_subscription') }}
    
), product_details AS (

    SELECT DISTINCT
      product_rate_plan_id,
      dim_product_tier_id
    FROM {{ ref('dim_product_detail') }}
    WHERE product_deployment_type IN ('Self-Managed', 'Dedicated')

), joined AS (

    SELECT
      customers_orders.internal_order_id                                    AS customers_db_order_id,
      seat_links.order_subscription_id,
      seat_links.dim_installation_id,
      seat_links.dim_instance_id,
      seat_links.dim_host_id,
      {{ get_keyed_nulls('subscriptions.dim_subscription_id') }}            AS dim_subscription_id,
      {{ get_keyed_nulls('subscriptions.dim_subscription_id_original') }}   AS dim_subscription_id_original,
      {{ get_keyed_nulls('subscriptions.dim_subscription_id_previous') }}   AS dim_subscription_id_previous,
      {{ get_keyed_nulls('subscriptions.subscription_name') }}              AS subscription_name,
      {{ get_keyed_nulls('subscriptions.dim_crm_account_id') }}             AS dim_crm_account_id,
      {{ get_keyed_nulls('subscriptions.dim_billing_account_id') }}         AS dim_billing_account_id,
      {{ get_keyed_nulls('product_details.dim_product_tier_id') }}          AS dim_product_tier_id,
      seat_links.active_user_count                                          AS active_user_count,
      seat_links.license_user_count,
      seat_links.max_historical_user_count                                  AS max_historical_user_count,
      seat_links.report_date,
      seat_links.is_last_seat_link_report_per_subscription,
      seat_links.is_last_seat_link_report_per_order,
      IFF(IFNULL(seat_links.order_subscription_id, '') = subscriptions.dim_subscription_id,
          TRUE, FALSE)                                                      AS is_subscription_in_zuora,
      IFF(product_details.dim_product_tier_id IS NOT NULL, TRUE, FALSE)     AS is_rate_plan_in_zuora,
      IFF(seat_links.active_user_count IS NOT NULL, TRUE, FALSE)            AS is_active_user_count_available
    FROM seat_links 
    INNER JOIN customers_orders
      ON seat_links.order_id = customers_orders.internal_order_id
    LEFT OUTER JOIN subscriptions
      ON seat_links.dim_subscription_id = subscriptions.dim_subscription_id
    LEFT OUTER JOIN product_details
      ON customers_orders.product_rate_plan_id = product_details.product_rate_plan_id
      
)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@ischweickartDD",
    updated_by="@michellecooper",
    created_date="2021-02-02",
    updated_date="2024-09-06"
) }}