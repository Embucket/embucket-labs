WITH zuora_revenue_approval_detail AS (

    SELECT *
    FROM {{source('zuora_revenue','zuora_revenue_approval_detail')}}
    QUALIFY ROW_NUMBER() OVER (PARTITION BY rc_appr_id, approver_sequence, approval_rule_id ORDER BY incr_updt_dt DESC) = 1

), renamed AS (

    SELECT
    
    {{ dbt_utils.generate_surrogate_key(['rc_appr_id', 'approver_sequence', 'approval_rule_id']) }}  AS primary_key,
    rc_appr_id::VARCHAR                                                                     AS revenue_contract_approval_id, 
    rc_id::VARCHAR                                                                          AS revenue_contract_id, 
    approval_obj_type::VARCHAR                                                              AS approval_object_type, 
    approval_status::VARCHAR                                                                AS approval_status, 
    approver_comments::VARCHAR                                                              AS approver_comments, 
    approved_by_user::VARCHAR                                                               AS approved_by_user_id, 
    approved_by_name::VARCHAR                                                               AS approved_by_user_name, 
    approval_date::DATETIME                                                                 AS approval_date, 
    pending_since::DATETIME                                                                 AS pending_since, 
    approver_user::VARCHAR                                                                  AS approver_user_id, 
    approver_name::VARCHAR                                                                  AS approver_user_name, 
    approver_sequence::VARCHAR                                                              AS approver_sequence, 
    approval_rule_id::VARCHAR                                                               AS approver_rule_id, 
    approval_rule_name::VARCHAR                                                             AS approval_rule_name, 
    approval_type::VARCHAR                                                                  AS approval_type, 
    approval_rule_desc::VARCHAR                                                             AS approval_rule_description, 
    book_id::VARCHAR                                                                        AS book_id, 
    sec_atr_val::VARCHAR                                                                    AS security_attribute_value, 
    client_id::VARCHAR                                                                      AS client_id, 
    rc_appr_crtd_by::VARCHAR                                                                AS revenue_contract_approval_created_by, 
    rc_appr_crtd_dt::DATETIME                                                               AS revenue_contract_approval_created_date, 
    rc_appr_updt_by::VARCHAR                                                                AS revenue_contract_approval_updated_by, 
    rc_appr_updt_dt::DATETIME                                                               AS revenue_contract_approval_updated_date, 
    rc_rule_crtd_by::VARCHAR                                                                AS revenue_contract_rule_created__by, 
    rc_rule_crtd_dt::DATETIME                                                               AS revenue_contract_rule_created_date, 
    rc_rule_updt_by::VARCHAR                                                                AS revenue_contract_rule_updated_by, 
    rc_rule_updt_dt::DATETIME                                                               AS revenue_contract_rule_updatd_date, 
    appr_rule_crtd_by::VARCHAR                                                              AS approval_rule_created_by, 
    appr_rule_crtd_dt::DATETIME                                                             AS approval_rule_created_date, 
    appr_rule_updt_by::VARCHAR                                                              AS approval_rule_updated_by, 
    appr_rule_updt_dt::DATETIME                                                             AS approval_rule_updated_date, 
    incr_updt_dt::DATETIME                                                                  AS incremental_update_date, 
    approval_start_date::DATETIME                                                           AS approval_start_date, 
    approval_end_date::DATETIME                                                             AS approval_end_date, 
    rc_rule_id::VARCHAR                                                                     AS revenue_contract_rule_id, 
    rc_rule_rule_id::VARCHAR                                                                AS revenue_contract_rule_rule_id, 
    rc_rule_obj_type::VARCHAR                                                               AS revenue_contract_rule_object_type, 
    rev_schd_flag::VARCHAR                                                                  AS is_revenue_schedule, 
    override_approver_flag::VARCHAR                                                         AS is_override_approve, 
    func_name::VARCHAR                                                                      AS function_name, 
    rule_id::VARCHAR                                                                        AS rule_id, 
    rc_rev_rel_appr_flag::VARCHAR                                                           AS is_revenue_contract_revenue_rel_approver, 
    appr_removal_flag::VARCHAR                                                              AS is_approval_removal, 
    rule_rev_rel_appr_flag::VARCHAR                                                         AS is_rule_revenue_approve, 
    override_aprv_flag::VARCHAR                                                             AS is_override_aprv

    FROM zuora_revenue_approval_detail

)

SELECT *
FROM renamed