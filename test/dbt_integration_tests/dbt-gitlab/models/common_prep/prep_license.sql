WITH customers_db_licenses AS (

    SELECT *
    FROM {{ ref('customers_db_licenses_source') }}

), original_subscription AS (

    SELECT *
    FROM {{ ref('zuora_subscription_source') }}

), license_md5_subscription_mapping AS (

    SELECT *
    FROM {{ ref('sheetload_license_md5_to_subscription_mapping') }}

), licenses AS (

    SELECT
      customers_db_licenses.license_id AS dim_license_id,
      customers_db_licenses.license_md5,
      customers_db_licenses.license_sha256,
      COALESCE(customers_db_licenses.zuora_subscription_id, license_md5_subscription_mapping.zuora_subscription_id) AS dim_subscription_id,
      customers_db_licenses.zuora_subscription_name AS subscription_name,
      'Customers Portal' AS environment,
      customers_db_licenses.license_user_count,
      IFF(customers_db_licenses.plan_code IS NULL OR customers_db_licenses.plan_code = '', 'core', customers_db_licenses.plan_code) AS license_plan,
      customers_db_licenses.is_trial,
      IFF(
          LOWER(customers_db_licenses.email) LIKE '%@gitlab.com' AND LOWER(customers_db_licenses.company) LIKE '%gitlab%',
          TRUE, FALSE
         ) AS is_internal,
      customers_db_licenses.company,
      customers_db_licenses.license_start_date,
      customers_db_licenses.license_expire_date,
      customers_db_licenses.created_at,
      customers_db_licenses.updated_at
    FROM customers_db_licenses
    LEFT JOIN license_md5_subscription_mapping
      ON customers_db_licenses.license_md5 = license_md5_subscription_mapping.license_md5

), renamed AS (

    SELECT
      -- Primary Key
      licenses.dim_license_id,

      -- Foreign Keys
      TRIM(licenses.dim_subscription_id, ' ')                          AS dim_subscription_id,
      original_subscription.original_id                                AS dim_subscription_id_original,
      original_subscription.previous_subscription_id                   AS dim_subscription_id_previous,

      -- Descriptive information
      licenses.license_md5,
      licenses.license_sha256,
      licenses.subscription_name,
      licenses.environment,
      licenses.license_user_count,
      licenses.license_plan,
      licenses.is_trial,
      licenses.is_internal,
      licenses.company,
      licenses.license_start_date,
      licenses.license_expire_date,
      licenses.created_at,
      licenses.updated_at

    FROM licenses
    LEFT JOIN original_subscription
       ON licenses.dim_subscription_id = original_subscription.subscription_id

)


{{ dbt_audit(
    cte_ref="renamed",
    created_by="@snalamaru",
    updated_by="@rbacovic",
    created_date="2021-01-08",
    updated_date="2022-12-01"
) }}
