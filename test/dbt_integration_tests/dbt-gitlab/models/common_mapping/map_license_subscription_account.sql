{{ config(
    tags=["mnpi_exception"]
) }}

{{ config({
        "materialized": "view",
    })
}}

WITH license AS (

    SELECT
      dim_license_id,
      license_md5,
      license_sha256,
      dim_subscription_id
    FROM {{ ref('dim_license') }}

), subscription AS (

    SELECT
      dim_subscription_id,
      dim_crm_account_id
    FROM {{ ref('prep_subscription') }}

), crm_account AS (

    SELECT
      dim_crm_account_id,
      dim_parent_crm_account_id
    FROM {{ ref('dim_crm_account') }}

), license_mapped_to_subscription AS (

    SELECT
      license.dim_license_id,
      license.license_md5,
      license.license_sha256,
      subscription.dim_subscription_id,
      subscription.dim_crm_account_id,
      IFF(license.dim_subscription_id IS NOT NULL, TRUE, FALSE)          AS is_license_mapped_to_subscription, -- does the license table have a value in both license_id and subscription_id
      IFF(subscription.dim_subscription_id IS NULL, FALSE, TRUE)     AS is_license_subscription_id_valid   -- is the subscription_id in the license table valid (does it exist in the dim_subscription table?)
    FROM license
    LEFT JOIN subscription
      ON license.dim_subscription_id = subscription.dim_subscription_id

), subscription_mapped_to_crm_account AS (

    SELECT
      subscription.dim_subscription_id,
      subscription.dim_crm_account_id,
      crm_account.dim_parent_crm_account_id
    FROM subscription
    INNER JOIN crm_account
      ON subscription.dim_crm_account_id = crm_account.dim_crm_account_id

), joined AS (

    SELECT
      license_mapped_to_subscription.dim_license_id,
      license_mapped_to_subscription.license_md5,
      license_mapped_to_subscription.license_sha256,
      license_mapped_to_subscription.is_license_mapped_to_subscription,
      license_mapped_to_subscription.is_license_subscription_id_valid,
      license_mapped_to_subscription.dim_subscription_id,
      license_mapped_to_subscription.dim_crm_account_id,
      subscription_mapped_to_crm_account.dim_parent_crm_account_id
    FROM license_mapped_to_subscription
    INNER JOIN subscription_mapped_to_crm_account
        ON license_mapped_to_subscription.dim_subscription_id = subscription_mapped_to_crm_account.dim_subscription_id

), license_statistics AS (

    SELECT
      dim_license_id,
      COUNT(DISTINCT license_md5)                   AS total_number_md5_per_license,
      COUNT(DISTINCT license_sha256)                AS total_number_sha256_per_license,
      COUNT(DISTINCT dim_subscription_id)           AS total_number_subscription_per_license,
      COUNT(DISTINCT dim_crm_account_id)            AS total_number_crm_account_per_license,
      COUNT(DISTINCT dim_parent_crm_account_id)    AS total_number_ultimate_parent_account_per_license
    FROM joined
    GROUP BY 1

), final AS (

    SELECT
        joined.dim_license_id,
        joined.license_md5,
        joined.license_sha256,
        joined.is_license_mapped_to_subscription,
        joined.is_license_subscription_id_valid,
        joined.dim_subscription_id,
        joined.dim_crm_account_id,
        joined.dim_parent_crm_account_id,
        license_statistics.total_number_md5_per_license,
        license_statistics.total_number_sha256_per_license,
        license_statistics.total_number_subscription_per_license,
        license_statistics.total_number_crm_account_per_license,
        license_statistics.total_number_ultimate_parent_account_per_license
    FROM joined
    INNER JOIN license_statistics
      ON joined.dim_license_id = license_statistics.dim_license_id

)

{{ dbt_audit(
    cte_ref="joined",
    created_by="@kathleentam",
    updated_by="@rbacovic",
    created_date="2021-01-10",
    updated_date="2022-12-01"
) }}
