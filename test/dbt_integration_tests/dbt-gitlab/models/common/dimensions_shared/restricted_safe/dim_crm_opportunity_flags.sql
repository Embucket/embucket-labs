{{ config(
    tags=["six_hourly"]
) }}

WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    
), distinct_values AS (

    SELECT DISTINCT -- note that we get the distinct combinations of flags to prevent fanning when this table is joined upon
      -- Opportunity Status/Stage
      is_won,
      is_closed,
      is_lost,
      is_open,
      is_active,
      is_stage_1_plus,
      is_stage_3_plus,
      is_stage_4_plus,
      is_closed_won,

      -- Deal Type Classification
      is_edu_oss,
      is_ps_opp, 
      is_public_sector_opp,
      is_sao,
      is_new_logo_first_order,
      is_renewal,
      is_refund,
      is_web_portal_purchase,
      is_registration_from_portal,
      is_credit,
      is_downgrade,
      is_contract_reset,
      fpa_master_bookings_flag,

      -- Pipeline and Revenue Tracking
      is_net_arr_pipeline_created,
      is_net_arr_closed_deal,
      is_booked_net_arr,
      is_win_rate_calc,

      -- Deal Quality/Risk Indicators
      is_risky,
      critical_deal_flag,
      valid_deal_count,

      -- Account Based Marketing (ABM)
      is_abm_tier_sao,
      is_abm_tier_closed_won,

      -- Data Quality/Exclusions
      is_deleted,
      is_duplicate,
      is_excluded_from_pipeline_created,
      is_comp_new_logo_override,

      -- Eligibility
      is_eligible_open_pipeline,
      is_eligible_asp_analysis,
      is_eligible_age_analysis,
      is_eligible_churn_contraction,

      -- Competitor Flags
      competitors_other_flag,
      competitors_gitlab_core_flag,
      competitors_none_flag,
      competitors_github_enterprise_flag,
      competitors_bitbucket_server_flag,
      competitors_unknown_flag,
      competitors_github_flag,
      competitors_gitlab_flag,
      competitors_jenkins_flag,
      competitors_azure_devops_flag,
      competitors_svn_flag,
      competitors_bitbucket_flag,
      competitors_atlassian_flag,
      competitors_perforce_flag,
      competitors_visual_studio_flag,
      competitors_azure_flag,
      competitors_amazon_code_commit_flag,
      competitors_circleci_flag,
      competitors_bamboo_flag,
      competitors_aws_flag

    FROM prep_crm_opportunity

)
SELECT 
  {{ dbt_utils.generate_surrogate_key(get_opportunity_flag_fields()) }} AS dim_crm_opportunity_flags_sk,
  *
FROM distinct_values