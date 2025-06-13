WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'order_action') }}

), renamed AS(

    SELECT
      -- Keys
      id                                        AS order_action_id,

      order_id                                  AS order_id,
      account_id                                AS account_id,
      subscription_version_amendment_id         AS subscription_version_amendment_id,
      subscription_id                           AS subscription_id,
      default_payment_method_id                 AS default_payment_method_id,
      bill_to_contact_id                        AS bill_to_contact_id,
      sold_to_contact_id                        AS sold_to_contact_id,

      auto_renew                                AS auto_renew,
      cancellation_effective_date               AS cancellation_effective_date,
      cancellation_policy                       AS cancellation_policy,
      contract_effective_date                   AS contract_effective_date,
      current_term                              AS current_term,
      current_term_period_type                  AS current_term_period_type,
      customer_acceptance_date                  AS customer_acceptance_date,
      renewal_term                              AS renewal_term,
      renewal_term_period_type                  AS renewal_term_period_type,
      renew_setting                             AS renew_setting,
      resume_date                               AS resume_date,
      sequence                                  AS sequence,
      service_activation_date                   AS service_activation_date,
      suspend_date                              AS suspend_date,
      term_start_date                           AS term_start_date,
      term_type                                 AS term_type,
      type                                      AS type,

       -- metadata
      updated_by_id                             AS updated_by_id,
      updated_date                              AS updated_date,
      created_by_id                             AS created_by_id,
      created_date                              AS created_date,

      _FIVETRAN_DELETED                         AS is_deleted


    FROM source

)

SELECT *
FROM renamed
