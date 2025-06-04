WITH header_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_header_source') }}

), final AS (

    SELECT

      -- ids
      revenue_contract_id                                   AS dim_revenue_contract_id,

      -- dates
      initial_performance_obligation_expiration_date,
      account_updated_date,
      fair_value_date,
      max_schedule_period,
      lifecycle_change_date,
      revenue_contract_modification_end_date,
      old_revenue_contract_modification_end_date,

      -- currency
      transactional_currency,
      functional_currency,

      -- exchange rate
      functional_currency_exchange_rate,
      reporting_currency_exchange_rate,

      -- revenue contract attributes
      customer_name,
      revenue_contract_version,
      revnue_contract_approval_status,
      revenue_contract_allocation_treatment,
      timeline_period,
      lifecycle_state,
      
      -- flags
      is_posted,
      is_allocation_error,
      is_manual_hold,
      is_hold_applied,
      is_schedule_hold,
      is_revrev_hold,
      is_allocation_schedule_hold,
      is_allocation_recognition_hold,
      is_manual_journal_entry_revenue_contract,
      is_netting_pending,
      is_allocation_eligible,
      is_manual_cv,
      is_manual_revenue_contract,
      is_lt_st_manual_journal_entry,
      is_freeze,
      is_skip_allocation,
      is_variable_consideration_allocation,
      is_revenue_contract_closed,
      is_new_revenue_contact_created_by_ct_mod,
      is_conversion,
      is_archive,
      is_stale_group,
      is_initial_allocation,
      is_retro_pros,
      is_exception,
      is_rev_rel_approval,
      is_skip_revenue_contract_modification,
      is_hybrid_revenue_contract,
      is_delinked,
      is_multiple_currency,
      is_multi_functional_currency,
      is_multiple_transactional_currency,
      is_multiple_currency_allocation,
      is_multiple_functional_currency_allocation,
      is_multiple_transactional_currency_allocation,
      is_allocatable,
      is_inter_company,
      is_revenue_contract_manually_created,

      -- metadata
      revenue_contract_created_by,
      revenue_contract_created_date,
      revenue_contract_updated_by,
      revenue_contract_updated_date,
      incremental_update_date,
      security_attribute_value

    FROM header_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}