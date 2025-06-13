WITH zuora_revenue_accounting_type AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_accounting_type')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT 
    
      id::VARCHAR                               AS accounting_type_id,
      name::VARCHAR                             AS accounting_type_name,
      wf_type::VARCHAR                          AS waterfall_type,
      wf_summ_type::VARCHAR	                    AS waterfall_summary_type,
      revenue_summary::VARCHAR                  AS revenue_summary_type,
      balance_sheet_acct_flag::VARCHAR          AS is_balance_sheet_account,
      p_l_acct_flag::VARCHAR                    AS is_p_l_account,
      cost_flag::VARCHAR                        AS is_cost,
      vc_acct_flag::VARCHAR                     AS is_variable_consideration_account,
      vc_clr_acct_flag::VARCHAR                 AS is_variable_consideration_clearing_account,
      incl_in_netting_flag::VARCHAR             AS is_include_in_netting,
      incl_in_manual_je_flag::VARCHAR           AS is_include_in_manual_journal_entry,
      waterfall_flag::VARCHAR                   AS is_waterfall_account,
      acct_group::VARCHAR                       AS accounting_group,
      def_rec_flag::VARCHAR                     AS defer_recognition_type,
      client_id::VARCHAR                        AS client_id,
      crtd_by::VARCHAR                          AS accounting_type_created_by,
      crtd_dt::DATETIME                         AS accounting_type_created_date,
      updt_by::VARCHAR                          AS accounting_type_updated_by,
      updt_dt::DATETIME                         AS accounting_type_updated_date,
      incr_updt_dt::DATETIME                    AS incremental_update_date,
      allow_mapping_flag::VARCHAR               AS is_mapping_allowed,
      def_offset_flag::VARCHAR                  AS is_deferred_offset,
      enabled_flag::VARCHAR                     AS is_enabled,
      payables_acct_flag::VARCHAR               AS is_payables_account,
      rev_offset_flag::VARCHAR                  AS is_revenue_offset,
      rev_display_seq::VARCHAR                  AS revenue_display_sequence

    FROM zuora_revenue_accounting_type

)

SELECT *
FROM renamed
