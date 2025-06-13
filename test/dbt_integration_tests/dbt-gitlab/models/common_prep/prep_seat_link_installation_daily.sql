{{ config(
    materialized='table',
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
      {{ dbt_utils.generate_surrogate_key(['prep_host.dim_host_id', 'source.uuid'])}}      AS dim_installation_id,
      source.zuora_subscription_id                                                         AS dim_subscription_id,
      source.zuora_subscription_name                                                       AS subscription_name,
      source.hostname                                                                      AS host_name,
      prep_host.dim_host_id,
      source.uuid                                                                          AS dim_instance_id,
      {{ get_keyed_nulls('product_details.dim_product_tier_id') }}                         AS dim_product_tier_id,
      product_details.product_delivery_type,
      product_details.product_deployment_type,
      source.report_date,
      source.created_at,
      source.updated_at,
      source.active_user_count,
      source.license_user_count,
      source.max_historical_user_count,
      source.add_on_metrics_user_count
    FROM {{ ref('customers_db_license_seat_links_source') }} AS source
    INNER JOIN {{ ref('prep_order') }}
      ON source.order_id = prep_order.internal_order_id
    LEFT JOIN {{ ref('prep_host') }}
      ON source.hostname = prep_host.host_name
    LEFT OUTER JOIN product_details
      ON prep_order.product_rate_plan_id = product_details.product_rate_plan_id
    WHERE prep_host.dim_host_id IS NOT NULL AND source.uuid IS NOT NULL

), flattened AS (

  SELECT
    seat_link.*,
    TRIM(unnest.value['add_on_type'], '"') AS add_on_type_original,
    CASE 
      WHEN add_on_type_original = 'code_suggestions'
        THEN 'GitLab Duo Pro'
      WHEN add_on_type_original = 'duo_enterprise'
        THEN 'GitLab Duo Enterprise'
      ELSE add_on_type_original
    END AS add_on_type,
    unnest.value['assigned_seats'] AS addon_assigned_seats,
    unnest.value['purchased_seats'] AS addon_purchased_seats
  FROM seat_link
  LEFT JOIN LATERAL FLATTEN(INPUT => PARSE_JSON(seat_link.add_on_metrics_user_count), OUTER => TRUE) AS unnest

), final AS (

  SELECT
    {{ dbt_utils.generate_surrogate_key(['report_date', 'dim_installation_id', 'add_on_type'])}} AS daily_seat_link_installation_product_sk,
    report_date,

    dim_subscription_id,
    dim_host_id,
    dim_instance_id,
    dim_installation_id,
    dim_product_tier_id,

    host_name,
    subscription_name,
    product_delivery_type,
    product_deployment_type,

    add_on_metrics_user_count,
    add_on_type_original,
    add_on_type,

    license_user_count,
    active_user_count,
    max_historical_user_count,
    addon_purchased_seats,
    addon_assigned_seats,

    created_at,
    updated_at
  FROM flattened
  WHERE add_on_type IS NOT NULL
  QUALIFY ROW_NUMBER() OVER (PARTITION BY report_date, dim_installation_id, add_on_type ORDER BY updated_at DESC, addon_assigned_seats DESC) = 1

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@utkarsh060",
    updated_by="@utkarsh060",
    created_date="2024-10-30",
    updated_date="2024-11-08"
) }}
