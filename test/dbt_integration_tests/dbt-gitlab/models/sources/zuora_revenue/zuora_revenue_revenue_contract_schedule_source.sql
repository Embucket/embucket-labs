{{ config(
    tags=["mnpi"]
) }}

WITH zuora_revenue_revenue_contract_schedule AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_revenue_contract_schedule')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY schd_id, acctg_type ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT 

      {{ dbt_utils.generate_surrogate_key(['schd_id', 'acctg_type']) }}  AS primary_key,
      rc_id::VARCHAR                                            AS revenue_contract_id,
      rc_ver::VARCHAR                                           AS revenue_contract_version,
      dist_id::VARCHAR                                          AS dist_id,
      atr1::VARCHAR                                             AS revenue_contract_schedule_attribute_1,
      atr2::VARCHAR                                             AS revenue_contract_schedule_attribute_2,
      atr3::VARCHAR                                             AS revenue_contract_schedule_attribute_3,
      atr4::VARCHAR                                             AS revenue_contract_schedule_attribute_4,
      atr5::VARCHAR                                             AS revenue_contract_schedule_attribute_5,
      pob_id::VARCHAR                                           AS performance_obligation_id,
      amount::FLOAT                                             AS amount,
      curr::VARCHAR                                             AS transactional_currency,
      f_ex_rate::FLOAT                                          AS functional_currency_exchange_rate,
      ex_rate_date::DATETIME                                    AS exchange_rate_date,
      post_date::DATETIME                                       AS post_date,
      CONCAT(prd_id, '01')::VARCHAR                             AS period_id,
      post_prd_id::VARCHAR                                      AS posted_period_id,
      post_batch_id::VARCHAR                                    AS post_batch_id,
      g_ex_rate::FLOAT                                          AS reporting_currency_exchange_rate,
      rel_id::VARCHAR                                           AS release_action_id,
      rel_pct::FLOAT                                            AS release_percent,
      CONCAT(crtd_prd_id,'01')::VARCHAR                         AS revenue_contract_schedule_created_period_id,
      root_line_id::VARCHAR                                     AS root_line_id,
      ref_bill_id::VARCHAR                                      AS reference_revenue_contract_bill_id,
      schd_id::VARCHAR                                          AS revenue_contract_schedule_id,
      line_id::VARCHAR                                          AS revenue_contract_line_id,
      acctg_seg::VARCHAR                                        AS accounting_segment,
      dr_amount::FLOAT                                          AS transactional_debit_amount,
      cr_amount::FLOAT                                          AS transactional_credit_amount,
      f_dr_amount::FLOAT                                        AS functional_debit_amount,
      f_cr_amount::FLOAT                                        AS functional_credit_amount,
      g_dr_amount::FLOAT                                        AS reporting_debit_amount,
      g_cr_amount::FLOAT                                        AS reporting_credit_amount,
      acctg_type::VARCHAR                                       AS accounting_type_id,
      interfaced_flag::VARCHAR                                  AS is_interfaced,
      initial_entry_flag::VARCHAR                               AS is_initial_entry,
      reversal_flag::VARCHAR                                    AS is_reversal,
      fcst_flag::VARCHAR                                        AS is_forecast,
      pp_cl_flag::VARCHAR                                       AS is_previous_period_contract_liability,
      netting_entry_flag::VARCHAR                               AS is_netting_entry,
      reallocation_flag::VARCHAR                                AS is_reallocation,
      account_name::VARCHAR                                     AS account_name,
      schd_type_flag::VARCHAR                                   AS schedule_type,
      initial_rep_entry_flag::VARCHAR                           AS is_initial_reporting_entry,
      period_name::VARCHAR                                      AS period_name,
      client_id::VARCHAR                                        AS client_id,
      book_id::VARCHAR                                          AS book_id,
      sec_atr_val::VARCHAR                                      AS security_attribute_value,
      crtd_by::VARCHAR                                          AS revenue_contract_schedule_created_by,
      crtd_dt::DATETIME                                         AS revenue_contract_schedule_created_date,
      updt_by::VARCHAR                                          AS revenue_contract_schedule_updated_by,
      updt_dt::DATETIME                                         AS revenue_contract_schedule_updated_date,
      incr_updt_dt::DATETIME                                    AS incremental_update_date,
      impact_trans_prc_flag::VARCHAR                            AS is_impact_transaction_price,
      line_type_flag::VARCHAR                                   AS revenue_contract_line_type,
      unbilled_flag::VARCHAR                                    AS is_unbilled,
      bld_fx_dt::DATETIME                                       AS billed_fx_date,
      bld_fx_rate::FLOAT                                        AS billed_fx_rate,
      rord_inv_ref::VARCHAR                                     AS reduction_order_invoice_reference,
      cr_acctg_flag::VARCHAR                                    AS credit_accounting,
      dr_acctg_flag::VARCHAR                                    AS debit_accounting,
      mass_action_flag::VARCHAR                                 AS is_mass_action,
      special_alloc_flag::VARCHAR                               AS is_special_allocation,
      pp_adj_flag::VARCHAR                                      AS is_previous_period_adjustment,
      vc_expiry_schd_flag::VARCHAR                              AS is_variable_consideration_expiry_schedule,
      orig_line_id::VARCHAR                                     AS original_revenue_contract_line_id,
      dr_link_id::VARCHAR                                       AS debit_link_id,
      cr_link_id::VARCHAR                                       AS credit_link_id,
      model_id::VARCHAR                                         AS model_id,
      je_batch_id::VARCHAR                                      AS manual_journal_entry_header_id,
      je_batch_name::VARCHAR                                    AS manual_journal_entry_header_name,
      pq_adj_flag::VARCHAR                                      AS is_previous_quarter_adjustment,
      delink_flag::VARCHAR                                      AS is_delink,
      retro_rvrsl_flag::VARCHAR                                 AS is_retro_reversal,
      pp_amt::FLOAT                                             AS previous_period_amount,
      pq_amt::FLOAT                                             AS previous_quarter_amount,
      py_amt::FLOAT                                             AS previous_year_amount,
      updt_prd_id::VARCHAR                                      AS revenue_contract_schedule_update_period_id,
      pord_flag::VARCHAR                                        AS is_pord,
      unbill_rvrsl_flag::VARCHAR                                AS is_unbilled_reversal,
      rec_evt_act_flag::VARCHAR                                 AS is_recognition_event_account,
      cmro_contra_entry_flag::VARCHAR                           AS is_cmro_contra_entry,
      retro_entry_flag::VARCHAR                                 AS is_retro_entry,
      left_over_entry_flag::VARCHAR                             AS is_left_over_entry,
      revs_posted_inv_flag::VARCHAR                             AS is_revs_posted_invoice,
      cl_dist_entry_flag::VARCHAR                               AS is_contract_liability_dist_entry
      
    FROM zuora_revenue_revenue_contract_schedule

)

SELECT *
FROM renamed