WITH prep_amendment AS (

    SELECT *
    FROM {{ ref('prep_amendment')}}

), base AS (

    SELECT
      --Surrogate Key
      dim_amendment_id,

      --Common Dimension keys
      dim_subscription_id,

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
    FROM prep_amendment

)

{{ dbt_audit(
    cte_ref="base",
    created_by="@iweeks",
    updated_by="@iweeks",
    created_date="2021-05-10",
    updated_date="2021-05-10"
) }}
