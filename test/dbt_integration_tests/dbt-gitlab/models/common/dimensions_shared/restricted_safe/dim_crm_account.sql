{{ config({
    "alias": "dim_crm_account",
    "post-hook": "{{ missing_member_column(primary_key = 'dim_crm_account_id', not_null_test_cols = ['is_reseller']) }}"
}) }}

{{ simple_cte([
    ('prep_crm_account','prep_crm_account'),
    ('prep_charge_mrr', 'prep_charge_mrr'),
    ('prep_date', 'prep_date')
]) }},

cohort_date AS (

  SELECT
    prep_charge_mrr.dim_crm_account_id,
    MIN(prep_date.first_day_of_month)          AS crm_account_arr_cohort_month,
    MIN(prep_date.first_day_of_fiscal_quarter) AS crm_account_arr_cohort_quarter
  FROM prep_charge_mrr
  LEFT JOIN prep_date
    ON prep_charge_mrr.date_id = prep_date.date_id
  WHERE prep_charge_mrr.subscription_status IN ('Active', 'Cancelled')
  GROUP BY 1

),

parent_cohort_date AS (

  SELECT
    prep_crm_account.dim_parent_crm_account_id,
    MIN(prep_date.first_day_of_month)          AS parent_account_arr_cohort_month,
    MIN(prep_date.first_day_of_fiscal_quarter) AS parent_account_arr_cohort_quarter
  FROM prep_charge_mrr
  LEFT JOIN prep_date
    ON prep_charge_mrr.date_id = prep_date.date_id
  LEFT JOIN prep_crm_account
    ON prep_charge_mrr.dim_crm_account_id = prep_crm_account.dim_crm_account_id
  WHERE prep_charge_mrr.subscription_status IN ('Active', 'Cancelled')
  GROUP BY 1

),

final AS (

  SELECT
    --primary key
    prep_crm_account.dim_crm_account_id,

    --surrogate keys
    prep_crm_account.dim_parent_crm_account_id,
    prep_crm_account.dim_crm_user_id,
    prep_crm_account.merged_to_account_id,
    prep_crm_account.record_type_id,
    prep_crm_account.master_record_id,
    prep_crm_account.dim_crm_person_primary_contact_id,

    --account people
    prep_crm_account.crm_account_owner,
    prep_crm_account.proposed_crm_account_owner,
    prep_crm_account.account_owner,
    prep_crm_account.technical_account_manager,
    prep_crm_account.tam_manager,
    prep_crm_account.executive_sponsor,
    prep_crm_account.owner_role,
    prep_crm_account.user_role_type,

    ----ultimate parent crm account info
    prep_crm_account.parent_crm_account_name,
    prep_crm_account.parent_crm_account_sales_segment,
    prep_crm_account.parent_crm_account_sales_segment_legacy,
    prep_crm_account.parent_crm_account_sales_segment_grouped,
    prep_crm_account.parent_crm_account_segment_region_stamped_grouped,
    prep_crm_account.parent_crm_account_industry,
    prep_crm_account.parent_crm_account_business_unit,
    prep_crm_account.parent_crm_account_geo,
    prep_crm_account.parent_crm_account_geo_pubsec_segment,
    prep_crm_account.parent_crm_account_region,
    prep_crm_account.parent_crm_account_area,
    prep_crm_account.parent_crm_account_territory,
    prep_crm_account.parent_crm_account_role_type,
    prep_crm_account.parent_crm_account_max_family_employee,
    prep_crm_account.parent_crm_account_upa_country,
    prep_crm_account.parent_crm_account_upa_country_name,
    prep_crm_account.parent_crm_account_upa_state,
    prep_crm_account.parent_crm_account_upa_city,
    prep_crm_account.parent_crm_account_upa_street,
    prep_crm_account.parent_crm_account_upa_postal_code,

    --descriptive attributes
    prep_crm_account.crm_account_name,
    prep_crm_account.crm_account_employee_count,
    prep_crm_account.crm_account_gtm_strategy,
    prep_crm_account.crm_account_focus_account,
    prep_crm_account.crm_account_owner_user_segment,
    prep_crm_account.crm_account_billing_country,
    prep_crm_account.crm_account_billing_country_code,
    prep_crm_account.crm_account_type,
    prep_crm_account.crm_account_industry,
    prep_crm_account.crm_account_sub_industry,
    prep_crm_account.crm_account_employee_count_band,
    prep_crm_account.partner_vat_tax_id,
    prep_crm_account.account_manager,
    prep_crm_account.crm_business_dev_rep_id,
    prep_crm_account.dedicated_service_engineer,
    prep_crm_account.account_tier,
    prep_crm_account.account_tier_notes,
    prep_crm_account.license_utilization,
    prep_crm_account.support_level,
    prep_crm_account.named_account,
    prep_crm_account.billing_postal_code,
    prep_crm_account.partner_type,
    prep_crm_account.partner_status,
    prep_crm_account.gitlab_customer_success_project,
    prep_crm_account.demandbase_account_list,
    prep_crm_account.demandbase_intent,
    prep_crm_account.demandbase_page_views,
    prep_crm_account.demandbase_score,
    prep_crm_account.demandbase_sessions,
    prep_crm_account.demandbase_trending_offsite_intent,
    prep_crm_account.demandbase_trending_onsite_engagement,
    prep_crm_account.account_domains,
    prep_crm_account.account_domain_1,
    prep_crm_account.account_domain_2,
    prep_crm_account.is_locally_managed_account,
    prep_crm_account.is_strategic_account,
    prep_crm_account.partner_track,
    prep_crm_account.partners_partner_type,
    prep_crm_account.gitlab_partner_program,
    prep_crm_account.zoom_info_company_name,
    prep_crm_account.zoom_info_company_revenue,
    prep_crm_account.zoom_info_company_employee_count,
    prep_crm_account.zoom_info_company_industry,
    prep_crm_account.zoom_info_company_city,
    prep_crm_account.zoom_info_company_state_province,
    prep_crm_account.zoom_info_company_country,
    prep_crm_account.account_phone,
    prep_crm_account.zoominfo_account_phone,
    prep_crm_account.abm_tier,
    prep_crm_account.health_number,
    prep_crm_account.health_score_color,
    prep_crm_account.partner_account_iban_number,
    prep_crm_account.gitlab_com_user,
    prep_crm_account.crm_account_zi_technologies,
    prep_crm_account.crm_account_zoom_info_website,
    prep_crm_account.crm_account_zoom_info_company_other_domains,
    prep_crm_account.crm_account_zoom_info_dozisf_zi_id,
    prep_crm_account.crm_account_zoom_info_parent_company_zi_id,
    prep_crm_account.crm_account_zoom_info_parent_company_name,
    prep_crm_account.crm_account_zoom_info_ultimate_parent_company_zi_id,
    prep_crm_account.crm_account_zoom_info_ultimate_parent_company_name,
    prep_crm_account.forbes_2000_rank,
    prep_crm_account.crm_sales_dev_rep_id,
    prep_crm_account.admin_manual_source_number_of_employees,
    prep_crm_account.admin_manual_source_account_address,
    prep_crm_account.eoa_sentiment,
    prep_crm_account.gs_health_user_engagement,
    prep_crm_account.gs_health_cd,
    prep_crm_account.gs_health_devsecops,
    prep_crm_account.gs_health_ci,
    prep_crm_account.gs_health_scm,
    prep_crm_account.risk_impact,
    prep_crm_account.risk_reason,
    prep_crm_account.last_timeline_at_risk_update,
    prep_crm_account.last_at_risk_update_comments,
    prep_crm_account.bdr_prospecting_status,
    prep_crm_account.is_focus_partner,
    prep_crm_account.gs_health_csm_sentiment,
    prep_crm_account.bdr_next_steps,
    prep_crm_account.bdr_account_research,
    prep_crm_account.bdr_account_strategy,
    prep_crm_account.account_bdr_assigned_user_role,
    prep_crm_account.gs_csm_compensation_pool,
    prep_crm_account.groove_notes,
    prep_crm_account.groove_engagement_status,
    prep_crm_account.groove_inferred_status,
    prep_crm_account.compensation_target_account,
    prep_crm_account.pubsec_type,

    --measures (maintain for now to not break reporting)
    prep_crm_account.parent_crm_account_lam,
    prep_crm_account.parent_crm_account_lam_dev_count,
    prep_crm_account.carr_account_family,
    prep_crm_account.carr_this_account,

    --D&B Fields
    prep_crm_account.dnb_match_confidence_score,
    prep_crm_account.dnb_match_grade,
    prep_crm_account.dnb_connect_company_profile_id,
    prep_crm_account.dnb_duns,
    prep_crm_account.dnb_global_ultimate_duns,
    prep_crm_account.dnb_domestic_ultimate_duns,
    prep_crm_account.dnb_exclude_company,

    --6 sense fields
    prep_crm_account.has_six_sense_6_qa,
    prep_crm_account.risk_rate_guid,
    prep_crm_account.six_sense_account_profile_fit,
    prep_crm_account.six_sense_account_reach_score,
    prep_crm_account.six_sense_account_profile_score,
    prep_crm_account.six_sense_account_buying_stage,
    prep_crm_account.six_sense_account_numerical_reach_score,
    prep_crm_account.six_sense_account_update_date,
    prep_crm_account.six_sense_account_6_qa_end_date,
    prep_crm_account.six_sense_account_6_qa_age_days,
    prep_crm_account.six_sense_account_6_qa_start_date,
    prep_crm_account.six_sense_account_intent_score,
    prep_crm_account.six_sense_segments,

    --Qualified Fields
    prep_crm_account.qualified_days_since_last_activity,
    prep_crm_account.qualified_signals_active_session_time,
    prep_crm_account.qualified_signals_bot_conversation_count,
    prep_crm_account.qualified_condition,
    prep_crm_account.qualified_score,
    prep_crm_account.qualified_trend,
    prep_crm_account.qualified_meetings_booked,
    prep_crm_account.qualified_signals_rep_conversation_count,
    prep_crm_account.qualified_signals_research_state,
    prep_crm_account.qualified_signals_research_score,
    prep_crm_account.qualified_signals_session_count,
    prep_crm_account.qualified_visitors_count,

    --degenerative dimensions
    prep_crm_account.is_sdr_target_account,
    prep_crm_account.is_key_account,
    prep_crm_account.is_reseller,
    prep_crm_account.is_jihu_account,
    prep_crm_account.is_first_order_available,
    prep_crm_account.is_zi_jenkins_present,
    prep_crm_account.is_zi_svn_present,
    prep_crm_account.is_zi_tortoise_svn_present,
    prep_crm_account.is_zi_gcp_present,
    prep_crm_account.is_zi_atlassian_present,
    prep_crm_account.is_zi_github_present,
    prep_crm_account.is_zi_github_enterprise_present,
    prep_crm_account.is_zi_aws_present,
    prep_crm_account.is_zi_kubernetes_present,
    prep_crm_account.is_zi_apache_subversion_present,
    prep_crm_account.is_zi_apache_subversion_svn_present,
    prep_crm_account.is_zi_hashicorp_present,
    prep_crm_account.is_zi_aws_cloud_trail_present,
    prep_crm_account.is_zi_circle_ci_present,
    prep_crm_account.is_zi_bit_bucket_present,
    prep_crm_account.is_excluded_from_zoom_info_enrich,
    prep_crm_account.is_base_prospect_account,

    --dates
    prep_crm_account.crm_account_created_date,
    prep_crm_account.abm_tier_1_date,
    prep_crm_account.abm_tier_2_date,
    prep_crm_account.abm_tier_3_date,
    prep_crm_account.gtm_acceleration_date,
    prep_crm_account.gtm_account_based_date,
    prep_crm_account.gtm_account_centric_date,
    prep_crm_account.partners_signed_contract_date,
    prep_crm_account.technical_account_manager_date,
    prep_crm_account.customer_since_date,
    prep_crm_account.next_renewal_date,
    prep_crm_account.gs_first_value_date,
    prep_crm_account.gs_last_csm_activity_date,
    prep_crm_account.bdr_recycle_date,
    prep_crm_account.actively_working_start_date,
    cohort_date.crm_account_arr_cohort_month,
    cohort_date.crm_account_arr_cohort_quarter,
    parent_cohort_date.parent_account_arr_cohort_month,
    parent_cohort_date.parent_account_arr_cohort_quarter,

    --metadata
    prep_crm_account.created_by_name,
    prep_crm_account.last_modified_by_name,
    prep_crm_account.last_modified_date,
    prep_crm_account.last_activity_date,
    prep_crm_account.is_deleted,
    prep_crm_account.pte_score,
    prep_crm_account.pte_decile,
    prep_crm_account.pte_score_group,
    prep_crm_account.ptc_score,
    prep_crm_account.ptc_decile,
    prep_crm_account.ptc_score_group,
    prep_crm_account.ptp_score,
    prep_crm_account.ptp_score_value,
    prep_crm_account.ptp_insights
  FROM prep_crm_account
  LEFT JOIN cohort_date
    ON prep_crm_account.dim_crm_account_id = cohort_date.dim_crm_account_id
  LEFT JOIN parent_cohort_date
    ON prep_crm_account.dim_parent_crm_account_id = parent_cohort_date.dim_parent_crm_account_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@msendal",
    updated_by="@fmcwilliams",
    created_date="2020-06-01",
    updated_date="2024-11-21"
) }}
