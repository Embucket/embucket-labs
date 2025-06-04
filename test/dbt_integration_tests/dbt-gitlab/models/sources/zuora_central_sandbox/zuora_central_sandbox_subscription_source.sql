WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'subscription') }}

), renamed AS (

    SELECT
      id                                            AS subscription_id,
      subscription_version_amendment_id             AS amendment_id,
      name                                          AS subscription_name,
        {{zuora_slugify("name")}}                   AS subscription_name_slugify,
     --keys
      account_id                                    AS account_id,
      creator_account_id                            AS creator_account_id,
      creator_invoice_owner_id                      AS creator_invoice_owner_id,
      invoice_owner_id                              AS invoice_owner_id,
      nullif(opportunity_id_c, '')                  AS sfdc_opportunity_id,
      nullif(opportunity_name_qt, '')               AS crm_opportunity_name,
      nullif(original_id, '')                       AS original_id,
      nullif(previous_subscription_id, '')          AS previous_subscription_id,
      nullif(recurly_id_c, '')                      AS sfdc_recurly_id,
      cpq_bundle_json_id_qt                         AS cpq_bundle_json_id,

      -- info
      status                                        AS subscription_status,
      auto_renew                                    AS auto_renew_native_hist,
      auto_renew_c                                  AS auto_renew_customerdot_hist,
      version                                       AS version,
      term_type                                     AS term_type,
      notes                                         AS notes,
      is_invoice_separate                           AS is_invoice_separate,
      current_term                                  AS current_term,
      current_term_period_type                      AS current_term_period_type,
      end_customer_details_c                        AS sfdc_end_customer_details,
      eoa_starter_bronze_offer_accepted_c           AS eoa_starter_bronze_offer_accepted,
      IFF(LENGTH(TRIM(turn_on_cloud_licensing_c)) > 0, turn_on_cloud_licensing_c, NULL)
                                                    AS turn_on_cloud_licensing,
      turn_on_seat_reconciliation_c                 AS turn_on_seat_reconciliation,
      turn_on_auto_renew_c                          AS turn_on_auto_renewal,
      turn_on_operational_metrics_c                 AS turn_on_operational_metrics,
      contract_operational_metrics_c                AS contract_operational_metrics,

      --key_dates
      cancelled_date                                AS cancelled_date,
      contract_acceptance_date                      AS contract_acceptance_date,
      contract_effective_date                       AS contract_effective_date,
      initial_term                                  AS initial_term,
      initial_term_period_type                      AS initial_term_period_type,
      term_end_date::DATE                           AS term_end_date,
      term_start_date::DATE                         AS term_start_date,
      subscription_end_date::DATE                   AS subscription_end_date,
      subscription_start_date::DATE                 AS subscription_start_date,
      service_activation_date                       AS service_activiation_date,
      opportunity_close_date_qt                     AS opportunity_close_date,
      original_created_date                         AS original_created_date,

      --foreign synced info
      opportunity_name_qt                           AS opportunity_name,
      purchase_order_c                              AS sfdc_purchase_order,
      quote_business_type_qt                        AS quote_business_type,
      quote_number_qt                               AS quote_number,
      quote_type_qt                                 AS quote_type,

      --renewal info
      renewal_setting                               AS renewal_setting,
      renewal_subscription_c_c                      AS zuora_renewal_subscription_name,
      split(nullif({{zuora_slugify("renewal_subscription_c_c")}}, ''), '|')
                                                    AS zuora_renewal_subscription_name_slugify,
      renewal_term                                  AS renewal_term,
      renewal_term_period_type                      AS renewal_term_period_type,
      contract_auto_renew_c                         AS contract_auto_renewal,
      contract_seat_reconciliation_c                AS contract_seat_reconciliation,


      --metadata
      updated_by_id                                 AS updated_by_id,
      updated_date                                  AS updated_date,
      created_by_id                                 AS created_by_id,
      created_date                                  AS created_date,
      _FIVETRAN_DELETED                             AS is_deleted,
      excludefrom_analysis_c                        AS exclude_from_analysis

    FROM source

)

SELECT *
FROM renamed
