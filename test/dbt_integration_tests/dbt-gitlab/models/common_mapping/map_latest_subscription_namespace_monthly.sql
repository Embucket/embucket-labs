{{
  config({
    "materialized": "table",
    "tags": ["mnpi_exception"]
  })
}}

WITH subscriptions AS (

  SELECT *
  FROM {{ ref('dim_subscription') }}

),

months AS (

{{ dbt_utils.date_spine(
        datepart="month",
        start_date="cast('2017-01-01' as date)",
        end_date="DATEADD('month', 1,DATE_TRUNC('month', CURRENT_DATE()))"
       )
    }}

),

product_detail AS (

  SELECT *
  FROM {{ ref('dim_product_detail') }}

),

charges AS (

  SELECT *
  FROM {{ ref('fct_charge') }}

),

joined AS (

  SELECT
    months.date_month,
    subscriptions.term_start_month,
    subscriptions.term_end_month,
    subscriptions.dim_subscription_id,
    subscriptions.dim_subscription_id_original,
    subscriptions.namespace_id AS dim_namespace_id,
    subscriptions.subscription_version,
    subscriptions.subscription_created_date,
    product_detail.product_rate_plan_charge_name,
    charges.charge_type
  FROM subscriptions
  INNER JOIN months
    ON (months.date_month >= subscriptions.term_start_month
        AND months.date_month <= subscriptions.term_end_month)
  LEFT JOIN charges ON charges.dim_subscription_id = subscriptions.dim_subscription_id
  LEFT JOIN product_detail ON product_detail.dim_product_detail_id = charges.dim_product_detail_id

),

final AS (

  SELECT
    date_month,
    dim_subscription_id,
    dim_subscription_id_original,
    dim_namespace_id,
    subscription_version,
    product_rate_plan_charge_name,
    charge_type
  FROM joined
  WHERE product_rate_plan_charge_name NOT IN (
    '1,000 CI Minutes',
    'Gitlab Storage 10GB - 1 Year',
    'Premium Support',
    '1,000 Compute Minutes'
  )
  AND charge_type != 'OneTime'
  --picking most recent subscription version
  QUALIFY
    ROW_NUMBER() OVER(
      PARTITION BY
        dim_namespace_id, date_month
      ORDER BY subscription_created_date DESC, subscription_version DESC
    ) = 1

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mdrussell",
    updated_by="@mdrussell",
    created_date="2022-10-28",
    updated_date="2023-03-22"
) }}
