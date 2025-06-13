{{ config(
    tags=["mnpi"]
) }}

WITH zuora_api_sandbox_amendment AS (

  SELECT *
  FROM {{ ref('zuora_api_sandbox_amendment_source') }}
  WHERE is_deleted = FALSE

), base AS (

    SELECT
      --Surrogate Key
      amendment_id                         AS dim_amendment_id,

      --Common Dimension keys
      subscription_id                      AS dim_subscription_id,

      --Information
      amendment_name,
      amendment_type,
      amendment_description,
      auto_renew,
      amendment_code,
      amendment_status,

      --Term information
      term_type,
      current_term,
      current_term_period_type,
      renewal_term,
      renewal_term_period_type,
      renewal_setting,

      --Dates
      term_start_date,
      effective_date,
      service_activation_date,
      customer_acceptance_date,
      contract_effective_date
    FROM zuora_api_sandbox_amendment

    UNION ALL

    SELECT
      --Surrogate Keys
      MD5('-1')                             AS dim_amendment_id,

      --Common Dimension keys
      MD5('-1')                             AS dim_subscription_id,

      --Information
      'Missing amendment_name'              AS amendment_name,
      'Missing amendment_type'              AS amendment_type,
      'Missing amendment_description'       AS amendment_description,
      0                                     AS auto_renew,
      'Missing amendment_code'              AS amendment_code,
      'Missing amendment_status'            AS amendment_status,

      --Term information
      'Missing term_type'                   AS term_type,
      -1                                    AS current_term,
      'Missing current_term_period_type'    AS current_term_period_type,
      -1                                    AS renewal_term,
      'Missing renewal_term_period_type'    AS renewal_term_period_type,
      'Missing renewal_setting'             AS renewal_setting,

      --Dates
      '9999-12-31 00:00:00.000 +0000'       AS term_start_date,
      '9999-12-31 00:00:00.000 +0000'       AS effective_date,
      '9999-12-31 00:00:00.000 +0000'       AS service_activation_date,
      '9999-12-31 00:00:00.000 +0000'       AS customer_acceptance_date,
      '9999-12-31 00:00:00.000 +0000'       AS contract_effective_date

)

{{ dbt_audit(
    cte_ref="base",
    created_by="@ken_aguilar",
    updated_by="@ken_aguilar",
    created_date="2021-08-31",
    updated_date="2021-08-31"
) }}