{{ config(
    tags=["product", "mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_product_tier', 'dim_product_tier'),
    ('dim_date', 'dim_date'),
    ('dim_billing_account', 'dim_billing_account'),
    ('dim_crm_accounts', 'dim_crm_account'),
    ('fct_charge', 'fct_charge'),
    ('dim_license', 'dim_license'),
    ('dim_ping_instance', 'dim_ping_instance')
    ])

}},

dim_product_detail AS (

  SELECT *
  FROM {{ ref('dim_product_detail') }}
  WHERE product_deployment_type IN ('Self-Managed', 'Dedicated')
    AND product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')

),

dim_subscription AS (

  SELECT *
  FROM {{ ref('dim_subscription') }}
  WHERE (
    subscription_name_slugify != zuora_renewal_subscription_name_slugify[0]::TEXT
    OR zuora_renewal_subscription_name_slugify IS NULL
  )
  AND subscription_status NOT IN ('Draft', 'Expired')

),

subscription_source AS (

  SELECT *
  FROM {{ ref('zuora_subscription_source') }}
  WHERE is_deleted = FALSE
    AND exclude_from_analysis IN ('False', '')

),

license_subscriptions AS (

  SELECT
    dim_date.first_day_of_month                                                                   AS reporting_month,
    dim_license.dim_license_id                                                                    AS license_id,
    dim_license.license_md5                                                                       AS license_md5,
    dim_license.license_sha256                                                                    AS license_sha256,
    dim_license.company                                                                           AS license_company_name,
    dim_license.license_expire_date                                                               AS license_expire_date,
    subscription_source.subscription_name_slugify                                                 AS original_subscription_name_slugify,
    dim_subscription.dim_subscription_id                                                          AS dim_subscription_id,
    subscription_source.subscription_name                                                         AS subscription_name,
    dim_subscription.subscription_start_month                                                     AS subscription_start_month,
    dim_subscription.subscription_end_month                                                       AS subscription_end_month,
    dim_subscription.dim_subscription_id_original                                                 AS dim_subscription_id_original,
    dim_billing_account.dim_billing_account_id                                                    AS dim_billing_account_id,
    dim_crm_accounts.dim_crm_account_id                                                           AS dim_crm_account_id,
    dim_crm_accounts.crm_account_name                                                             AS crm_account_name,
    dim_crm_accounts.dim_parent_crm_account_id                                                    AS dim_parent_crm_account_id,
    dim_crm_accounts.parent_crm_account_name                                                      AS parent_crm_account_name,
    dim_crm_accounts.parent_crm_account_upa_country                                               AS parent_crm_account_upa_country,
    dim_crm_accounts.parent_crm_account_sales_segment                                             AS parent_crm_account_sales_segment,
    dim_crm_accounts.parent_crm_account_industry                                                  AS parent_crm_account_industry,
    dim_crm_accounts.parent_crm_account_territory                                                 AS parent_crm_account_territory,
    dim_crm_accounts.technical_account_manager                                                    AS technical_account_manager,
    IFF(MAX(fct_charge.mrr) > 0, TRUE, FALSE)                                                     AS is_paid_subscription,
    MAX(IFF(dim_product_detail.product_rate_plan_name ILIKE ANY ('%edu%', '%oss%'), TRUE, FALSE)) AS is_program_subscription,
    ARRAY_AGG(DISTINCT dim_product_detail.product_tier_name)
    WITHIN GROUP (ORDER BY dim_product_detail.product_tier_name ASC)                              AS product_category_array,
    ARRAY_AGG(DISTINCT dim_product_detail.product_rate_plan_name)
    WITHIN GROUP (ORDER BY dim_product_detail.product_rate_plan_name ASC)                         AS product_rate_plan_name_array
  FROM dim_license
  INNER JOIN subscription_source
    ON dim_license.dim_subscription_id = subscription_source.subscription_id
  LEFT JOIN dim_subscription
    ON subscription_source.subscription_name_slugify = dim_subscription.subscription_name_slugify
  LEFT JOIN subscription_source AS all_subscriptions
    ON subscription_source.subscription_name_slugify = all_subscriptions.subscription_name_slugify
  INNER JOIN fct_charge
    ON all_subscriptions.subscription_id = fct_charge.dim_subscription_id
      AND fct_charge.charge_type = 'Recurring'
  INNER JOIN dim_product_detail
    ON fct_charge.dim_product_detail_id = dim_product_detail.dim_product_detail_id
  LEFT JOIN dim_billing_account
    ON dim_subscription.dim_billing_account_id = dim_billing_account.dim_billing_account_id
  LEFT JOIN dim_crm_accounts
    ON dim_billing_account.dim_crm_account_id = dim_crm_accounts.dim_crm_account_id
  INNER JOIN dim_date
    ON fct_charge.effective_start_month <= dim_date.date_day
      AND fct_charge.effective_end_month > dim_date.date_day
      AND dim_date.date_day = dim_date.first_day_of_month -- This was added to improve the cross join performance
  {{ dbt_utils.group_by(n=22) }}



),

latest_subscription AS (

  SELECT
    dim_subscription_id          AS latest_subscription_id,
    dim_subscription_id_original AS dim_subscription_id_original
  FROM dim_subscription
  WHERE subscription_status IN ('Active', 'Cancelled')

),

license_subscriptions_w_latest_subscription AS (

  SELECT
    license_subscriptions.*,
    latest_subscription.latest_subscription_id
  FROM license_subscriptions
  LEFT JOIN latest_subscription
    ON license_subscriptions.dim_subscription_id_original = latest_subscription.dim_subscription_id_original

)

{{ dbt_audit(
    cte_ref="license_subscriptions_w_latest_subscription",
    created_by="@pempey",
    updated_by="@michellecooper",
    created_date="2024-02-07",
    updated_date="2024-07-17"
) }}
