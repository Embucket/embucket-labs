WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'order_action') }}

), renamed AS(

    SELECT

      id                                 AS dim_order_action_id,

      -- keys
      orderid                            AS dim_order_id,
      subscriptionid                     AS dim_subscription_id,
      subscriptionversionamendmentid     AS dim_amendment_id,
  
      -- account info
      type                               AS order_action_type,
      sequence                           AS order_action_sequence,
      autorenew                          AS is_auto_renew,
      cancellationpolicy                 AS cancellation_policy,
      termtype                           AS term_type,
  
      customeracceptancedate::DATE       AS customer_acceptance_date,
      contracteffectivedate::DATE        AS contract_effective_date,
      serviceactivationdate::DATE        AS service_activation_date,
      
      currentterm                        AS current_term,
      currenttermperiodtype              AS current_term_period_type,
      
      renewalterm                        AS renewal_term,
      renewaltermperiodtype              AS renewal_term_period_type,
      renewsetting                       AS renewal_setting,
      
      termstartdate::DATE                AS term_start_date,

      -- metadata
      createddate::DATE                  AS order_action_created_date,
      createdbyid                        AS order_action_created_by_id,
      updateddate::DATE                  AS updated_date,
      updatedbyid                        AS updated_by_id,
      deleted                            AS is_deleted

    FROM source

)

SELECT *
FROM renamed
