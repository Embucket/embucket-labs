{{ config(
    tags=["six_hourly"]
) }}

{{ config({
    "post-hook": "{{ missing_member_column(primary_key = 'dim_crm_opportunity_id') }}"
    })
}}

WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    WHERE is_live = 1

), layered AS (

    SELECT
      -- keys
      prep_crm_opportunity.dim_crm_account_id,
      prep_crm_opportunity.dim_crm_opportunity_id,
      prep_crm_opportunity.opportunity_name,
      prep_crm_opportunity.dim_crm_user_id,
      prep_crm_opportunity.dim_parent_crm_opportunity_id,
      prep_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,

      -- logistical information
      prep_crm_opportunity.generated_source,
      prep_crm_opportunity.lead_source,
      prep_crm_opportunity.merged_opportunity_id,
      prep_crm_opportunity.duplicate_opportunity_id,
      prep_crm_opportunity.contract_reset_opportunity_id,
      prep_crm_opportunity.net_new_source_categories,
      prep_crm_opportunity.primary_campaign_source_id,
      prep_crm_opportunity.sales_path,
      prep_crm_opportunity.subscription_type,
      prep_crm_opportunity.source_buckets,
      prep_crm_opportunity.crm_sales_dev_rep_id,
      prep_crm_opportunity.crm_business_dev_rep_id,
      prep_crm_opportunity.opportunity_development_representative,
      prep_crm_opportunity.sdr_or_bdr,
      prep_crm_opportunity.iqm_submitted_by_role,
      prep_crm_opportunity.sdr_pipeline_contribution,
      prep_crm_opportunity.stage_name,
      prep_crm_opportunity.sa_tech_evaluation_close_status,
      prep_crm_opportunity.deal_path,

      -- opportunity information
      prep_crm_opportunity.product_category,
      prep_crm_opportunity.product_details,
      prep_crm_opportunity.products_purchased,
      prep_crm_opportunity.critical_deal_flag,
      prep_crm_opportunity.forecast_category_name,
      prep_crm_opportunity.invoice_number,
      prep_crm_opportunity.is_refund,
      prep_crm_opportunity.is_downgrade,
      prep_crm_opportunity.is_risky,
      prep_crm_opportunity.is_swing_deal,
      prep_crm_opportunity.is_edu_oss,
      prep_crm_opportunity.is_won,
      prep_crm_opportunity.is_ps_opp,
      prep_crm_opportunity.probability,
      prep_crm_opportunity.professional_services_value,
      prep_crm_opportunity.edu_services_value,
      prep_crm_opportunity.investment_services_value,
      prep_crm_opportunity.reason_for_loss,
      prep_crm_opportunity.reason_for_loss_details,
      prep_crm_opportunity.reason_for_loss_staged,
      prep_crm_opportunity.reason_for_loss_calc,
      prep_crm_opportunity.sales_qualified_source,
      prep_crm_opportunity.sales_qualified_source_grouped,
      prep_crm_opportunity.solutions_to_be_replaced,
      prep_crm_opportunity.is_web_portal_purchase,
      prep_crm_opportunity.partner_initiated_opportunity,
      prep_crm_opportunity.user_segment,
      prep_crm_opportunity.order_type,
      prep_crm_opportunity.order_type_current,
      prep_crm_opportunity.opportunity_category,
      prep_crm_opportunity.opportunity_health,
      prep_crm_opportunity.risk_type,
      prep_crm_opportunity.risk_reasons,
      prep_crm_opportunity.downgrade_reason,
      prep_crm_opportunity.tam_notes,
      prep_crm_opportunity.churn_contraction_type,
      prep_crm_opportunity.churn_contraction_net_arr_bucket,
      prep_crm_opportunity.payment_schedule,
      prep_crm_opportunity.opportunity_term,
      prep_crm_opportunity.primary_solution_architect,
      prep_crm_opportunity.growth_type,
      prep_crm_opportunity.opportunity_deal_size,
      prep_crm_opportunity.deployment_preference,
      prep_crm_opportunity.stage_name_3plus,
      prep_crm_opportunity.stage_name_4plus,
      prep_crm_opportunity.stage_category,
      prep_crm_opportunity.deal_category,
      prep_crm_opportunity.deal_group,
      prep_crm_opportunity.deal_size,
      prep_crm_opportunity.calculated_deal_size,
      prep_crm_opportunity.deal_path_engagement,
      prep_crm_opportunity.opportunity_owner,
      prep_crm_opportunity.opportunity_owner_manager,
      prep_crm_opportunity.opportunity_owner_department,
      prep_crm_opportunity.opportunity_owner_role,
      prep_crm_opportunity.opportunity_owner_title,
      prep_crm_opportunity.sqs_bucket_engagement,
      prep_crm_opportunity.sa_tech_evaluation_end_date,
      prep_crm_opportunity.sa_tech_evaluation_start_date,
      prep_crm_opportunity.calculated_partner_track,
      prep_crm_opportunity.quote_start_date,
      prep_crm_opportunity.subscription_start_date,
      prep_crm_opportunity.subscription_end_date,
      prep_crm_opportunity.subscription_renewal_date,
      prep_crm_opportunity.resale_partner_name,
      prep_crm_opportunity.record_type_name,
      prep_crm_opportunity.next_steps,
      prep_crm_opportunity.auto_renewal_status,
      prep_crm_opportunity.qsr_notes,
      prep_crm_opportunity.qsr_status,
      prep_crm_opportunity.manager_confidence,
      prep_crm_opportunity.renewal_risk_category,
      prep_crm_opportunity.renewal_swing_arr,
      prep_crm_opportunity.renewal_manager, 
      prep_crm_opportunity.renewal_forecast_health,
      prep_crm_opportunity.startup_type,
      prep_crm_opportunity.sales_segment,

      --account people attributes
      prep_crm_opportunity.crm_account_owner_sales_segment,
      prep_crm_opportunity.crm_account_owner_geo,
      prep_crm_opportunity.crm_account_owner_region,
      prep_crm_opportunity.crm_account_owner_area,
      prep_crm_opportunity.crm_account_owner_sales_segment_geo_region_area,

      -- Competitors
      prep_crm_opportunity.competitors,
      prep_crm_opportunity.competitors_other_flag,
      prep_crm_opportunity.competitors_gitlab_core_flag,
      prep_crm_opportunity.competitors_none_flag,
      prep_crm_opportunity.competitors_github_enterprise_flag,
      prep_crm_opportunity.competitors_bitbucket_server_flag,
      prep_crm_opportunity.competitors_unknown_flag,
      prep_crm_opportunity.competitors_github_flag,
      prep_crm_opportunity.competitors_gitlab_flag,
      prep_crm_opportunity.competitors_jenkins_flag,
      prep_crm_opportunity.competitors_azure_devops_flag,
      prep_crm_opportunity.competitors_svn_flag,
      prep_crm_opportunity.competitors_bitbucket_flag,
      prep_crm_opportunity.competitors_atlassian_flag,
      prep_crm_opportunity.competitors_perforce_flag,
      prep_crm_opportunity.competitors_visual_studio_flag,
      prep_crm_opportunity.competitors_azure_flag,
      prep_crm_opportunity.competitors_amazon_code_commit_flag,
      prep_crm_opportunity.competitors_circleci_flag,
      prep_crm_opportunity.competitors_bamboo_flag,
      prep_crm_opportunity.competitors_aws_flag,

      -- Command Plan fields
      prep_crm_opportunity.cp_partner,
      prep_crm_opportunity.cp_paper_process,
      prep_crm_opportunity.cp_help,
      prep_crm_opportunity.cp_review_notes,
      prep_crm_opportunity.cp_use_cases,
      prep_crm_opportunity.cp_champion,
      prep_crm_opportunity.cp_close_plan,
      prep_crm_opportunity.cp_decision_criteria,
      prep_crm_opportunity.cp_decision_process,
      prep_crm_opportunity.cp_economic_buyer,
      prep_crm_opportunity.cp_identify_pain,
      prep_crm_opportunity.cp_metrics,
      prep_crm_opportunity.cp_risks,
      prep_crm_opportunity.cp_value_driver,
      prep_crm_opportunity.cp_why_do_anything_at_all,
      prep_crm_opportunity.cp_why_gitlab,
      prep_crm_opportunity.cp_why_now,
      prep_crm_opportunity.cp_score,

      -- stamped fields
      prep_crm_opportunity.crm_opp_owner_stamped_name,
      prep_crm_opportunity.crm_account_owner_stamped_name,
      prep_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped,
      prep_crm_opportunity.sao_crm_opp_owner_sales_segment_stamped_grouped,
      prep_crm_opportunity.sao_crm_opp_owner_geo_stamped,
      prep_crm_opportunity.sao_crm_opp_owner_region_stamped,
      prep_crm_opportunity.sao_crm_opp_owner_area_stamped,
      prep_crm_opportunity.sao_crm_opp_owner_segment_region_stamped_grouped,
      prep_crm_opportunity.sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
      prep_crm_opportunity.crm_opp_owner_user_role_type_stamped,
      prep_crm_opportunity.crm_opp_owner_sales_segment_geo_region_area_stamped,

      -- Pipeline Velocity Account and Opp Owner Fields and Key Reporting Fields
      prep_crm_opportunity.opportunity_owner_user_segment,

      -- channel reporting
      prep_crm_opportunity.dr_partner_deal_type,
      prep_crm_opportunity.dr_partner_engagement,
      prep_crm_opportunity.aggregate_partner,

      -- vsa reporting
      prep_crm_opportunity.vsa_readout,
      prep_crm_opportunity.vsa_start_date,
      prep_crm_opportunity.vsa_url,
      prep_crm_opportunity.vsa_status,
      prep_crm_opportunity.vsa_end_date,

      -- PS related
      prep_crm_opportunity.intended_product_tier,

      prep_crm_opportunity.downgrade_details,

      -- PTC related fields
      prep_crm_opportunity.ptc_predicted_arr,
      prep_crm_opportunity.ptc_predicted_renewal_risk_category,

      -- metadata
      prep_crm_opportunity._last_dbt_run

    FROM prep_crm_opportunity

)

{{ dbt_audit(
    cte_ref="layered",
    created_by="@iweeks",
    updated_by="@rkohnke",
    created_date="2020-11-20",
    updated_date="2024-09-20"
) }}
