{{ config(
    tags=["mnpi"]
) }}

WITH zuora_revenue_revenue_contract_hold AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_revenue_contract_hold')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rc_hold_id ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT 

      rc_hold_id::VARCHAR                               AS revenue_contract_hold_id,
      rc_id::VARCHAR                                    AS revenue_contract_id,
      rc_hold_applied_by::VARCHAR                       AS revenue_contract_hold_applied_by_id,
      rc_hold_applied_by_name::VARCHAR                  AS revenue_contract_hold_applied_by_name,
      CONCAT(rc_hold_applied_prd_id::VARCHAR, '01')     AS revenue_contract_hold_applied_period_id,
      rc_hold_applied_dt::DATETIME                      AS revenue_contract_hold_applied_date,
      rc_hold_comment::VARCHAR                          AS revenue_contract_hold_comment,
      rc_hold_released_flag::VARCHAR                    AS is_revenue_contract_hold_released,
      rc_hold_release_comment::VARCHAR                  AS revenue_contract_hold_release_comment,
      rc_hold_release_reason::VARCHAR                   AS revenue_contract_hold_release_reason,
      CONCAT(rc_hold_release_prd_id::VARCHAR, '01')     AS revenue_contract_hold_release_period_id,
      rc_hold_release_dt::DATETIME                      AS revenue_contract_hold_release_date,
      rc_hold_release_by::VARCHAR                       AS revenue_contract_hold_release_by_id,
      rc_hold_release_by_name::VARCHAR                  AS revenue_contract_hold_release_by_name,
      hold_id::VARCHAR                                  AS hold_id,
      hold_type::VARCHAR                                AS hold_type,
      hold_name::VARCHAR                                AS hold_name,
      hold_desc::VARCHAR                                AS hold_description,
      rev_schd_hold_flag::VARCHAR                       AS is_revenue_schedule_hold,
      revrec_hold_flag::VARCHAR                         AS is_revenue_recognition_hold,
      alloc_schd_hold_flag::VARCHAR                     AS is_allocation_schedule_hold,
      alloc_rec_hold_flag::VARCHAR                      AS is_allocation_recognition_hold,
      user_releasable_flag::VARCHAR                     AS is_user_releasable,
      sec_atr_val::VARCHAR                              AS security_attribute_value,
      client_id::VARCHAR                                AS client_id,
      book_id::VARCHAR                                  AS book_id,
      hold_crtd_by::VARCHAR                             AS hold_created_by,
      hold_crtd_dt::DATETIME                            AS hold_created_date,
      hold_updt_by::VARCHAR                             AS hold_update_by,
      hold_updt_dt::DATETIME                            AS hold_update_date,
      rc_hold_crtd_by::VARCHAR                          AS revenue_contract_hold_created_by,
      rc_hold_crtd_dt::DATETIME                         AS revenue_contract_hold_created_date,
      rc_hold_updt_by::VARCHAR                          AS revenue_contract_hold_updated_by,
      rc_hold_updt_dt::DATETIME                         AS revenue_contract_hold_updated_date,
      incr_updt_dt::DATETIME                            AS incremental_update_date,
      rc_hold_exp_date::DATETIME                        AS revenue_contract_hold_expiration_date,
      rc_hold_acct_segments::VARCHAR                    AS revenue_contract_hold_accounting_segment,
      allow_manual_apply_flag::VARCHAR                  AS is_allow_manual_apply,
      allow_manual_rel_flag::VARCHAR                    AS is_allow_manual_rel,
      hold_level::VARCHAR                               AS hold_level,
      hold_schd_type::VARCHAR                           AS hold_schedule_type,
      exp_fld_name::VARCHAR                             AS expiry_field_name,
      exp_num_type::VARCHAR                             AS expiry_number_type,
      exp_num::VARCHAR                                  AS expiry_number,
      ln_hold_level_flag::VARCHAR                       AS line_hold_level,
      ln_hold_type_flag::VARCHAR                        AS line_hold_type,
      ln_hold_processed_flag::VARCHAR                   AS is_line_hold_processed,
      rev_hold_start_date::DATETIME                     AS revenue_hold_start_date,
      rev_hold_end_date::DATETIME                       AS revenue_hold_end_date,
      criteria_match_flag::VARCHAR                      AS is_criteria_match,
      remove_hold_flag::VARCHAR                         AS is_remove_hold,
      line_id::VARCHAR                                  AS revenue_contract_line_id,
      event_id::VARCHAR                                 AS event_id,
      manual_hold_flag::VARCHAR                         AS is_manual_hold,
      evnt_hold_appld_manul_flag::VARCHAR               AS is_manual_event_hold_applied,
      override_aprv_flag::VARCHAR                       AS is_override_approval,
      sha_enabled_flag::VARCHAR                         AS is_sha_enabled,
      rc_pob_id::VARCHAR                                AS revenue_contract_performance_obligation_id
      
    FROM zuora_revenue_revenue_contract_hold

)

SELECT *
FROM renamed