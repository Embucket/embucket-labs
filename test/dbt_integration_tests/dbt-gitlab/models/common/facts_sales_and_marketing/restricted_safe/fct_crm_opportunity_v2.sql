{{ config(
    tags=["six_hourly"]
) }}

{{ simple_cte([

    ('dim_sales_qualified_source', 'dim_sales_qualified_source'),
    ('dim_order_type','dim_order_type'),
    ('dim_dr_partner_engagement', 'dim_dr_partner_engagement'),
    ('dim_deal_path', 'dim_deal_path'),
    ('dim_alliance_type', 'dim_alliance_type_scd'),
    ('dim_channel_type','dim_channel_type')

]) }},

prep_crm_opportunity AS (

    SELECT 
      *,
      -- We create the surrogate keys while only considering prep_crm_opportunity because there are ambiguous fields if we do so after joining to other models
      {{ dbt_utils.generate_surrogate_key(get_opportunity_flag_fields()) }}             AS dim_crm_opportunity_flags_sk,
      {{ dbt_utils.generate_surrogate_key(get_opportunity_command_plan_fields()) }}     AS dim_crm_command_plan_sk,
      {{ dbt_utils.generate_surrogate_key(get_opportunity_deal_fields()) }}             AS dim_crm_opportunity_deal_sk,
      {{ dbt_utils.generate_surrogate_key(get_opportunity_source_and_path_fields()) }}  AS dim_crm_opportunity_source_and_path_sk,
      {{ dbt_utils.generate_surrogate_key(get_opportunity_partner_fields()) }}          AS dim_crm_opportunity_partner_sk  
    FROM {{ref('prep_crm_opportunity')}}
    WHERE is_live = TRUE

), final AS (

  SELECT 

    -- Primary Key 
    prep_crm_opportunity.dim_crm_opportunity_id,    

    -- Foreign Keys
    prep_crm_opportunity.merged_opportunity_id                                          AS merged_crm_opportunity_id,
    prep_crm_opportunity.dim_parent_crm_opportunity_id,
    prep_crm_opportunity.duplicate_opportunity_id,
    prep_crm_opportunity.dim_crm_account_id,
    prep_crm_opportunity.dim_parent_crm_account_id,
    prep_crm_opportunity.dim_crm_user_id,
    prep_crm_opportunity.dim_crm_person_id,
    prep_crm_opportunity.sfdc_contact_id,
    prep_crm_opportunity.crm_sales_dev_rep_id,
    prep_crm_opportunity.crm_business_dev_rep_id,
    prep_crm_opportunity.record_type_id,
    prep_crm_opportunity.ssp_id,
    prep_crm_opportunity.ga_client_id,
    prep_crm_opportunity.dim_crm_current_account_set_hierarchy_sk,
    prep_crm_opportunity.contract_reset_opportunity_id,
    prep_crm_opportunity.invoice_number,                                                                                                                                                                                                                                       
    {{ get_keyed_nulls('dim_sales_qualified_source.dim_sales_qualified_source_id') }}   AS dim_sales_qualified_source_id,
    {{ get_keyed_nulls('dim_order_type.dim_order_type_id') }}                           AS dim_order_type_id,
    {{ get_keyed_nulls('dim_order_type_current.dim_order_type_id') }}                   AS dim_order_type_current_id,
    {{ get_keyed_nulls('dim_dr_partner_engagement.dim_dr_partner_engagement_id') }}     AS dim_dr_partner_engagement_id,
    {{ get_keyed_nulls('dim_alliance_type.dim_alliance_type_id') }}                     AS dim_alliance_type_id,
    {{ get_keyed_nulls('dim_alliance_type_current.dim_alliance_type_id') }}             AS dim_alliance_type_current_id,
    {{ get_keyed_nulls('dim_channel_type.dim_channel_type_id') }}                       AS dim_channel_type_id,
    prep_crm_opportunity.dim_crm_opportunity_flags_sk,
    prep_crm_opportunity.dim_crm_command_plan_sk,
    prep_crm_opportunity.dim_crm_opportunity_deal_sk,
    prep_crm_opportunity.dim_crm_opportunity_source_and_path_sk,
    prep_crm_opportunity.dim_crm_opportunity_partner_sk, 

    -- Key Process Dates
    created_date_id,
    sales_accepted_date_id,
    sales_qualified_date_id,
    close_date_id,
    arr_created_date_id,
    last_activity_date_id,
    sales_last_activity_date_id,
    subscription_start_date_id,
    subscription_end_date_id,
    {{ get_date_id('subscription_renewal_date') }}                                     AS subscription_renewal_date_id,
    {{ get_date_id('quote_start_date') }}                                              AS quote_start_date_id,

    -- Stage info 
    stage_name,
    stage_name_3plus,
    stage_name_4plus,
    stage_category,

    -- Stage Progression Dates
    stage_0_pending_acceptance_date_id,
    stage_1_discovery_date_id,
    stage_2_scoping_date_id,
    stage_3_technical_evaluation_date_id,
    stage_4_proposal_date_id,
    stage_5_negotiating_date_id,
    stage_6_awaiting_signature_date_id,
    stage_6_closed_won_date_id,
    stage_6_closed_lost_date_id,

    -- Stage Duration Metrics
    days_in_0_pending_acceptance,
    days_in_1_discovery,
    days_in_2_scoping,
    days_in_3_technical_evaluation,
    days_in_4_proposal,
    days_in_5_negotiating,
    days_in_stage,
    cycle_time_in_days,
    calculated_age_in_days,
    days_in_sao,
    days_since_last_activity,

    -- Technical Evaluation
    sa_tech_evaluation_close_status,
    sa_tech_evaluation_end_date,
    sa_tech_evaluation_start_date,
    technical_evaluation_date_id,

    -- PTC related fields
    ptc_predicted_arr,
    ptc_predicted_renewal_risk_category,

    -- Financial Amounts
    amount,
    recurring_amount,
    true_up_amount,
    proserv_amount,
    other_non_recurring_amount,
    renewal_amount,
    total_contract_value,
    calculated_discount,

    -- ARR Metrics
    arr,
    arr_basis,
    created_arr,
    raw_net_arr,
    net_arr,
    net_arr_stage_1,
    enterprise_agile_planning_net_arr,
    duo_net_arr,
    vsa_start_date_net_arr,
    open_1plus_net_arr,
    open_3plus_net_arr,
    open_4plus_net_arr,
    booked_net_arr,
    positive_open_net_arr,
    positive_booked_net_arr,
    closed_net_arr,
    created_and_won_same_quarter_net_arr,

    -- Deal & Logo Counts
    new_logo_count,
    calculated_deal_count,
    open_1plus_deal_count,
    open_3plus_deal_count,
    open_4plus_deal_count,
    booked_deal_count,
    positive_booked_deal_count,
    positive_open_deal_count,
    closed_deals,
    closed_won_opps,
    closed_opps,
    created_deals,

    -- Churn AND Contraction Metrics
    churned_contraction_deal_count,
    churned_contraction_net_arr,
    booked_churned_contraction_deal_count,
    booked_churned_contraction_net_arr,
    forecasted_churn_for_clari,

    -- Incremental AND Ratio Metrics
    incremental_acv                                                                    AS iacv,
    net_incremental_acv                                                                AS net_iacv,
    opportunity_based_iacv_to_net_arr_ratio,
    segment_order_type_iacv_to_net_arr_ratio,
    calculated_from_ratio_net_arr,
    weighted_linear_iacv,
    closed_buckets,
    count_campaigns,
    count_crm_attribution_touchpoints,

  -- SA Specific Metrics
    number_of_sa_activity_tasks,

    -- XDR Specific Metrics
    xdr_net_arr_stage_1,
    xdr_net_arr_stage_3,

    -- Clari Specific Fields
    won_arr_basis_for_clari,
    arr_basis_for_clari,
    override_arr_basis_clari  

  FROM prep_crm_opportunity
  LEFT JOIN dim_sales_qualified_source
    ON prep_crm_opportunity.sales_qualified_source = dim_sales_qualified_source.sales_qualified_source_name
  LEFT JOIN dim_order_type 
    ON prep_crm_opportunity.order_type = dim_order_type.order_type_name
  LEFT JOIN dim_order_type as dim_order_type_current
    ON prep_crm_opportunity.order_type_current = dim_order_type_current.order_type_name
  LEFT JOIN dim_dr_partner_engagement
    ON prep_crm_opportunity.dr_partner_engagement = dim_dr_partner_engagement.dr_partner_engagement_name
  LEFT JOIN dim_alliance_type
    ON prep_crm_opportunity.alliance_type = dim_alliance_type.alliance_type_name
  LEFT JOIN dim_alliance_type AS dim_alliance_type_current
    ON prep_crm_opportunity.alliance_type_current = dim_alliance_type_current.alliance_type_name
  LEFT JOIN dim_channel_type
    ON prep_crm_opportunity.channel_type = dim_channel_type.channel_type_name
)
SELECT *
FROM final
