
  
    

create or replace transient table EMBUCKET.restricted_safe_workspace_finance.dim_order_action_central_sandbox
    

    
    as (


WITH base AS (

    SELECT *
    FROM EMBUCKET.zuora_central_sandbox.zuora_central_sandbox_order_action_source
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      
      order_action_id                   AS dim_order_action_id,
      order_id                          AS dim_order_id,
      subscription_id                   AS dim_subscription_id,
      subscription_version_amendment_id AS dim_amendment_id,
      type                              AS order_action_type,
      sequence                          AS order_action_sequence,
      auto_renew                        AS is_auto_renew,
      cancellation_policy,
      term_type,
      created_date                      AS order_action_created_date,
      customer_acceptance_date,
      contract_effective_date,
      service_activation_date,
      current_term,
      current_term_period_type,
      renewal_term,
      renewal_term_period_type,
      renew_setting                     AS renewal_setting,
      term_start_date

    FROM base

)

SELECT
      *,
      '@michellecooper'::VARCHAR       AS created_by,
      '@michellecooper'::VARCHAR       AS updated_by,
      '2022-03-31'::DATE        AS model_created_date,
      '2022-03-31'::DATE        AS model_updated_date,
      CURRENT_TIMESTAMP()               AS dbt_updated_at,

    

        

            CURRENT_TIMESTAMP()               AS dbt_created_at

        
    

    FROM final
    )
;


  