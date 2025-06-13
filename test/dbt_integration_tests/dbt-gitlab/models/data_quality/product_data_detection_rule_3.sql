{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_amendment', 'dim_amendment'),
    ('fct_mrr', 'fct_mrr'),
    ('dim_product_detail', 'dim_product_detail'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_arr', 'mart_arr')
])}}

, dim_subscription AS ( -- Add a flag to dim_subscription which specify if the subscription is the last version

    SELECT
      IFF(MAX(subscription_version) OVER(partition by subscription_name) = subscription_version, TRUE, FALSE) AS is_last_subscription_version,
      LAST_VALUE(subscription_start_date) OVER(PARTITION BY subscription_name ORDER BY subscription_version) AS last_subscription_start_date,
      LAST_VALUE(subscription_end_date) OVER(PARTITION BY subscription_name ORDER BY subscription_version) AS last_subscription_end_date,
      *
    FROM {{ ref('dim_subscription') }}

), dim_license AS (
    -- Dedup multiple subscription_ids in dim_license. In case of duplicate subscription_ids first take the one in customers portal and then the one with
    -- the latest license_expire_date
    SELECT
      *,
      IFF(environment LIKE 'Customer%Portal', 1, 2) AS environment_order
    FROM {{ ref('dim_license') }}
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_subscription_id ORDER BY environment_order, license_expire_date DESC) = 1
    order by dim_subscription_id

), subscription_amendments_issue_license_mapping AS (

    SELECT
      dim_subscription.subscription_name,
      IFF(MAX(
        IFF(amendment_type IN ('NewProduct', 'RemoveProduct', 'UpdateProduct', 'Renewal'), 1, 0)
        ) = 1, TRUE, FALSE) does_subscription_name_contains_amendments_issue_license
    FROM dim_subscription
    LEFT JOIN dim_amendment
      ON dim_amendment.dim_amendment_id = dim_subscription.DIM_AMENDMENT_ID_SUBSCRIPTION
    GROUP BY 1

), subscription_renewal_mapping AS (

    SELECT DISTINCT
      subscription_name,
      IFF(LEN(TRIM(zuora_renewal_subscription_name)) = 0, NULL, zuora_renewal_subscription_name) AS zuora_renewal_subscription_name
    FROM dim_subscription
    WHERE is_last_subscription_version

), amendments AS (  -- Get subscriptions versions that are the product of the amendments listed in the WHERE clause
    -- These amendments are the ones that should have a license attached to them
    -- In the qualify statement, we get only the latest version that is part of the amendment list, since the latest one is the one we care about

    SELECT DISTINCT
      dim_subscription.dim_subscription_id,
      dim_subscription.dim_crm_account_id,
      dim_amendment.dim_amendment_id,
      dim_amendment.amendment_name,
      dim_subscription.subscription_version,
      dim_subscription.subscription_status,
      dim_subscription.subscription_start_date,
      dim_subscription.subscription_end_date,
      dim_amendment.amendment_type,
      dim_subscription.subscription_name,
      dim_subscription.dim_billing_account_id_invoice_owner_account AS dim_billing_account_id_invoice_owner,
      dim_subscription.last_subscription_start_date,
      dim_subscription.last_subscription_end_date,
      subscription_renewal_mapping.zuora_renewal_subscription_name,
      subscription_amendments_issue_license_mapping.does_subscription_name_contains_amendments_issue_license,
      dim_subscription.dbt_updated_at

    FROM dim_subscription
    LEFT JOIN dim_amendment
      ON dim_amendment.dim_amendment_id = dim_subscription.DIM_AMENDMENT_ID_SUBSCRIPTION
    LEFT JOIN subscription_renewal_mapping
      ON subscription_renewal_mapping.subscription_name = dim_subscription.subscription_name
    LEFT JOIN subscription_amendments_issue_license_mapping
      ON dim_subscription.subscription_name = subscription_amendments_issue_license_mapping.subscription_name
    WHERE ( amendment_type IN ('NewProduct', 'RemoveProduct', 'UpdateProduct', 'Renewal')
      or dim_subscription.subscription_version = 1)

    -- Gets latest subscription_version where the ammendments above happened
    QUALIFY ROW_NUMBER() OVER(PARTITION BY dim_subscription.subscription_name ORDER BY dim_subscription.subscription_version DESC) = 1

), ammendments_and_last_version AS (  -- Pull the latest subscription version and append it to the ammendments found above.
    -- Reason for this is to look for the license id in the last amendment in case it is not in the past CTE

    SELECT *
    FROM amendments

    UNION

    SELECT DISTINCT
      dim_subscription.dim_subscription_id,
      dim_subscription.dim_crm_account_id,
      dim_amendment.dim_amendment_id,
      dim_amendment.amendment_name,
      dim_subscription.subscription_version,
      dim_subscription.subscription_status,
      dim_subscription.subscription_start_date,
      dim_subscription.subscription_end_date,
      dim_amendment.amendment_type,
      dim_subscription.subscription_name,
      dim_subscription.dim_billing_account_id_invoice_owner_account AS dim_billing_account_id_invoice_owner,
      dim_subscription.last_subscription_start_date,
      dim_subscription.last_subscription_end_date,
      subscription_renewal_mapping.zuora_renewal_subscription_name,
      subscription_amendments_issue_license_mapping.does_subscription_name_contains_amendments_issue_license,
      dim_subscription.dbt_updated_at
    FROM dim_subscription
    LEFT JOIN dim_amendment
      ON dim_amendment.dim_amendment_id = dim_subscription.DIM_AMENDMENT_ID_SUBSCRIPTION
    LEFT JOIN subscription_renewal_mapping
      ON subscription_renewal_mapping.subscription_name = dim_subscription.subscription_name
    LEFT JOIN subscription_amendments_issue_license_mapping
      ON dim_subscription.subscription_name = subscription_amendments_issue_license_mapping.subscription_name
    WHERE is_last_subscription_version

), self_managed_subscriptions AS ( -- Get subscription_id from self managed subscriptions

    SELECT DISTINCT fct_mrr.dim_subscription_id
    FROM fct_mrr
    LEFT JOIN dim_product_detail
      ON fct_mrr.dim_product_detail_id = dim_product_detail.dim_product_detail_id
    LEFT JOIN dim_subscription
      ON fct_mrr.dim_subscription_id = dim_subscription.dim_subscription_id
    WHERE subscription_start_date <= CURRENT_DATE
    QUALIFY LAST_VALUE(dim_product_detail.product_delivery_type) OVER(
        PARTITION BY dim_subscription.subscription_name ORDER BY dim_subscription.subscription_version, fct_mrr.dim_date_id
      ) IN ('Self-Managed', 'Dedicated')

), subscriptions_with_arr_in_current_month AS ( -- Get subscriptions names that are currently paying ARR.
     -- If the subscription is not paying ARR no reason to investigate it

    SELECT subscription_name, SUM(arr) AS arr
    FROM mart_arr
    WHERE arr > 0
      AND arr_month = DATE_TRUNC('month', CURRENT_DATE)
    GROUP BY 1

), self_managed_amendments AS ( -- Filter the amendments / subscription_versions to be of self managed

    SELECT ammendments_and_last_version.*
    FROM ammendments_and_last_version
    INNER JOIN self_managed_subscriptions
      ON ammendments_and_last_version.dim_subscription_id = self_managed_subscriptions.dim_subscription_id

), subscription_to_licenses as ( -- Join subscriptions to licenses

    SELECT
      self_managed_amendments.*,
      dim_license.dim_license_id,
      dim_license.license_md5,
      dim_license.license_sha256,
      dim_license.dim_environment_id,
      dim_license.environment,
      dim_license.license_plan,
      dim_license.license_start_date,
      dim_license.license_expire_date
    FROM self_managed_amendments
    LEFT JOIN dim_license
      ON self_managed_amendments.dim_subscription_id = dim_license.dim_subscription_id
    ORDER BY self_managed_amendments.subscription_name, self_managed_amendments.subscription_version

), subscription_to_licenses_final AS ( -- If the latest subscription version or the amendment from the amendment list has a valid license

    SELECT *
    FROM subscription_to_licenses
    QUALIFY ROW_NUMBER() OVER(PARTITION BY subscription_name ORDER BY dim_license_id DESC NULLS LAST, subscription_version DESC) = 1

), licenses_missing_subscriptions AS (

    SELECT *
    FROM subscription_to_licenses_final
    WHERE dim_license_id IS NULL

), licenses_with_subscriptions AS (

    SELECT *
    FROM subscription_to_licenses_final
    WHERE dim_license_id IS NOT NULL

), report AS (

    SELECT
      'Missing license' AS license_status,
      licenses_missing_subscriptions.*
    FROM licenses_missing_subscriptions
    LEFT JOIN licenses_with_subscriptions
      ON licenses_missing_subscriptions.does_subscription_name_contains_amendments_issue_license = FALSE
      AND licenses_missing_subscriptions.last_subscription_start_date = licenses_with_subscriptions.last_subscription_end_date
      AND licenses_missing_subscriptions.subscription_name = licenses_with_subscriptions.zuora_renewal_subscription_name
      AND licenses_missing_subscriptions.dim_billing_account_id_invoice_owner != licenses_with_subscriptions.dim_billing_account_id_invoice_owner
    WHERE licenses_with_subscriptions.dim_subscription_id IS NULL

    UNION

    SELECT
      'Has license' AS license_status,
      licenses_missing_subscriptions.dim_subscription_id,
      licenses_missing_subscriptions.dim_crm_account_id,
      licenses_missing_subscriptions.dim_amendment_id,
      licenses_missing_subscriptions.amendment_name,
      licenses_missing_subscriptions.subscription_version,
      licenses_missing_subscriptions.subscription_status,
      licenses_missing_subscriptions.subscription_start_date,
      licenses_missing_subscriptions.subscription_end_date,
      licenses_missing_subscriptions.amendment_type,
      licenses_missing_subscriptions.subscription_name,
      licenses_missing_subscriptions.dim_billing_account_id_invoice_owner,
      licenses_missing_subscriptions.last_subscription_start_date,
      licenses_missing_subscriptions.last_subscription_end_date,
      licenses_missing_subscriptions.zuora_renewal_subscription_name,
      licenses_missing_subscriptions.does_subscription_name_contains_amendments_issue_license,
      licenses_missing_subscriptions.dbt_updated_at,

      licenses_with_subscriptions.dim_license_id,
      licenses_with_subscriptions.license_md5,
      licenses_with_subscriptions.license_sha256,
      licenses_with_subscriptions.dim_environment_id,
      licenses_with_subscriptions.environment,
      licenses_with_subscriptions.license_plan,
      licenses_with_subscriptions.license_start_date,
      licenses_with_subscriptions.license_expire_date
    FROM licenses_missing_subscriptions
    LEFT JOIN licenses_with_subscriptions
      ON licenses_missing_subscriptions.does_subscription_name_contains_amendments_issue_license = FALSE
      AND licenses_missing_subscriptions.last_subscription_start_date = licenses_with_subscriptions.last_subscription_end_date
      AND licenses_missing_subscriptions.subscription_name = licenses_with_subscriptions.zuora_renewal_subscription_name
      AND licenses_missing_subscriptions.dim_billing_account_id_invoice_owner != licenses_with_subscriptions.dim_billing_account_id_invoice_owner
    WHERE licenses_with_subscriptions.dim_subscription_id IS NOT NULL

    UNION

    SELECT
      'Has license' AS license_status,
      *
    FROM licenses_with_subscriptions

), final AS (

    SELECT report.*
    FROM report
    INNER JOIN subscriptions_with_arr_in_current_month
      ON subscriptions_with_arr_in_current_month.subscription_name = report.subscription_name

)

SELECT *
FROM final
