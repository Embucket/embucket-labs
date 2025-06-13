{{ config(
    tags=["six_hourly"]
) }}

{{ simple_cte([
    ('crm_account_dimensions', 'map_crm_account'),
    ('prep_crm_account', 'prep_crm_account'),
    ('order_type', 'prep_order_type'),
    ('sales_qualified_source', 'prep_sales_qualified_source'),
    ('deal_path', 'prep_deal_path'),
    ('sales_rep', 'prep_crm_user'),
    ('sales_segment', 'prep_sales_segment'),
    ('sfdc_campaigns', 'prep_campaign'),
    ('dr_partner_engagement', 'prep_dr_partner_engagement'),
    ('alliance_type', 'prep_alliance_type_scd'),
    ('channel_type', 'prep_channel_type'),
    ('prep_crm_opportunity', 'prep_crm_opportunity'),
    ('prep_crm_user_hierarchy', 'prep_crm_user_hierarchy'),
    ('prep_date', 'prep_date')

]) }}

, sfdc_opportunity AS (

    SELECT *
    FROM prep_crm_opportunity
    WHERE is_live = 1

), final_opportunities AS (

    SELECT

      -- opportunity and person ids
      sfdc_opportunity.dim_crm_opportunity_id,
      sfdc_opportunity.merged_opportunity_id                                                                              AS merged_crm_opportunity_id,
      sfdc_opportunity.dim_crm_account_id,
      crm_account_dimensions.dim_parent_crm_account_id,
      sfdc_opportunity.dim_crm_person_id,
      sfdc_opportunity.sfdc_contact_id,
      sfdc_opportunity.record_type_id,

      -- dates
      sfdc_opportunity.created_date,
      sfdc_opportunity.created_date_id,
      sfdc_opportunity.sales_accepted_date,
      sfdc_opportunity.sales_accepted_date_id,
      sfdc_opportunity.close_date,
      sfdc_opportunity.close_date_id,
      sfdc_opportunity.arr_created_date_id,
      sfdc_opportunity.arr_created_date,
      sfdc_opportunity.stage_0_pending_acceptance_date,
      sfdc_opportunity.stage_0_pending_acceptance_date_id,
      sfdc_opportunity.stage_1_discovery_date,
      sfdc_opportunity.stage_1_discovery_date_id,
      sfdc_opportunity.stage_2_scoping_date,
      sfdc_opportunity.stage_2_scoping_date_id,
      sfdc_opportunity.stage_3_technical_evaluation_date,
      sfdc_opportunity.stage_3_technical_evaluation_date_id,
      sfdc_opportunity.stage_4_proposal_date,
      sfdc_opportunity.stage_4_proposal_date_id,
      sfdc_opportunity.stage_5_negotiating_date,
      sfdc_opportunity.stage_5_negotiating_date_id,
      sfdc_opportunity.stage_6_awaiting_signature_date,
      sfdc_opportunity.stage_6_awaiting_signature_date_id,
      sfdc_opportunity.stage_6_closed_won_date,
      sfdc_opportunity.stage_6_closed_won_date_id,
      sfdc_opportunity.stage_6_closed_lost_date,
      sfdc_opportunity.stage_6_closed_lost_date_id,
      sfdc_opportunity.days_in_0_pending_acceptance,
      sfdc_opportunity.days_in_1_discovery,
      sfdc_opportunity.days_in_2_scoping,
      sfdc_opportunity.days_in_3_technical_evaluation,
      sfdc_opportunity.days_in_4_proposal,
      sfdc_opportunity.days_in_5_negotiating,
      sfdc_opportunity.subscription_start_date_id,
      sfdc_opportunity.subscription_end_date_id,
      sfdc_opportunity.sales_qualified_date_id,
      sfdc_opportunity.last_activity_date,
      sfdc_opportunity.last_activity_date_id,
      sfdc_opportunity.sales_last_activity_date,
      sfdc_opportunity.sales_last_activity_date_id,
      sfdc_opportunity.technical_evaluation_date,
      sfdc_opportunity.technical_evaluation_date_id,

      -- common dimension keys
      {{ get_keyed_nulls('sfdc_opportunity.dim_crm_user_id') }}                                                                   AS dim_crm_user_id,
      {{ get_keyed_nulls('prep_crm_account.dim_crm_user_id') }}                                                                   AS dim_crm_account_user_id,
      {{ get_keyed_nulls('order_type.dim_order_type_id') }}                                                                       AS dim_order_type_id,
      {{ get_keyed_nulls('order_type_current.dim_order_type_id') }}                                                                  AS dim_order_type_current_id,
      {{ get_keyed_nulls('dr_partner_engagement.dim_dr_partner_engagement_id') }}                                                 AS dim_dr_partner_engagement_id,
      {{ get_keyed_nulls('alliance_type.dim_alliance_type_id') }}                                                                 AS dim_alliance_type_id,
      {{ get_keyed_nulls('alliance_type_current.dim_alliance_type_id') }}                                                         AS dim_alliance_type_current_id,
      {{ get_keyed_nulls('channel_type.dim_channel_type_id') }}                                                                   AS dim_channel_type_id,
      {{ get_keyed_nulls('sales_qualified_source.dim_sales_qualified_source_id') }}                                               AS dim_sales_qualified_source_id,
      {{ get_keyed_nulls('deal_path.dim_deal_path_id') }}                                                                         AS dim_deal_path_id,
      {{ get_keyed_nulls('crm_account_dimensions.dim_parent_sales_segment_id,sales_segment.dim_sales_segment_id') }}              AS dim_parent_sales_segment_id,
      crm_account_dimensions.dim_parent_sales_territory_id,
      crm_account_dimensions.dim_parent_industry_id,
      {{ get_keyed_nulls('crm_account_dimensions.dim_account_sales_segment_id,sales_segment.dim_sales_segment_id') }}             AS dim_account_sales_segment_id,
      crm_account_dimensions.dim_account_sales_territory_id,
      crm_account_dimensions.dim_account_industry_id,
      crm_account_dimensions.dim_account_location_country_id,
      crm_account_dimensions.dim_account_location_region_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_hierarchy_id') }}                                                  AS dim_crm_opp_owner_user_hierarchy_id,
      sfdc_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_business_unit_id') }}                                              AS dim_crm_opp_owner_business_unit_stamped_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_sales_segment_id') }}                                              AS dim_crm_opp_owner_sales_segment_stamped_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_geo_id') }}                                                        AS dim_crm_opp_owner_geo_stamped_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_region_id') }}                                                     AS dim_crm_opp_owner_region_stamped_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_area_id') }}                                                       AS dim_crm_opp_owner_area_stamped_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_name_id') }}                                                  AS dim_crm_opp_owner_role_name_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_1_id') }}                                               AS dim_crm_opp_owner_role_level_1_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_2_id') }}                                               AS dim_crm_opp_owner_role_level_2_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_3_id') }}                                               AS dim_crm_opp_owner_role_level_3_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_4_id') }}                                               AS dim_crm_opp_owner_role_level_4_id,
      {{ get_keyed_nulls('prep_crm_user_hierarchy.dim_crm_user_role_level_5_id') }}                                               AS dim_crm_opp_owner_role_level_5_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_hierarchy_sk') }}                                                                AS dim_crm_user_hierarchy_live_sk,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_business_unit_id') }}                                                            AS dim_crm_user_business_unit_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_sales_segment_id') }}                                                            AS dim_crm_user_sales_segment_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_geo_id') }}                                                                      AS dim_crm_user_geo_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_region_id') }}                                                                   AS dim_crm_user_region_id,
      {{ get_keyed_nulls('sales_rep.dim_crm_user_area_id') }}                                                                     AS dim_crm_user_area_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_hierarchy_sk') }}                                                        AS dim_crm_user_hierarchy_account_user_sk,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_business_unit_id') }}                                                    AS dim_crm_account_user_business_unit_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_sales_segment_id') }}                                                    AS dim_crm_account_user_sales_segment_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_geo_id') }}                                                              AS dim_crm_account_user_geo_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_region_id') }}                                                           AS dim_crm_account_user_region_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_area_id') }}                                                             AS dim_crm_account_user_area_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_name_id') }}                                                        AS dim_crm_account_user_role_name_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_1_id') }}                                                     AS dim_crm_account_user_role_level_1_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_2_id') }}                                                     AS dim_crm_account_user_role_level_2_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_3_id') }}                                                     AS dim_crm_account_user_role_level_3_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_4_id') }}                                                     AS dim_crm_account_user_role_level_4_id,
      {{ get_keyed_nulls('sales_rep_account.dim_crm_user_role_level_5_id') }}                                                     AS dim_crm_account_user_role_level_5_id,
      sfdc_opportunity.dim_crm_current_account_set_hierarchy_sk,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_sales_segment_id
        ELSE dim_crm_opp_owner_sales_segment_stamped_id
      END                                                                                                                         AS dim_crm_current_account_set_sales_segment_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_geo_id
        ELSE  dim_crm_opp_owner_geo_stamped_id
      END                                                                                                                         AS dim_crm_current_account_set_geo_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_region_id
        ELSE dim_crm_opp_owner_region_stamped_id
      END                                                                                                                         AS dim_crm_current_account_set_region_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_area_id
        ELSE dim_crm_opp_owner_area_stamped_id
      END                                                                                                                         AS dim_crm_current_account_set_area_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_business_unit_id
        ELSE dim_crm_opp_owner_business_unit_stamped_id
      END                                                                                                                         AS dim_crm_current_account_set_business_unit_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_role_name_id
        ELSE dim_crm_opp_owner_role_name_id
      END                                                                                                                         AS dim_crm_current_account_set_role_name_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_role_level_1_id
        ELSE dim_crm_opp_owner_role_level_1_id
      END                                                                                                                         AS dim_crm_current_account_set_role_level_1_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_role_level_2_id
        ELSE dim_crm_opp_owner_role_level_2_id
      END                                                                                                                         AS dim_crm_current_account_set_role_level_2_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_role_level_3_id
        ELSE dim_crm_opp_owner_role_level_3_id
      END                                                                                                                         AS dim_crm_current_account_set_role_level_3_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_role_level_4_id
        ELSE dim_crm_opp_owner_role_level_4_id
      END                                                                                                                         AS dim_crm_current_account_set_role_level_4_id,
      CASE
        WHEN close_fiscal_year < prep_date.current_fiscal_year
          THEN dim_crm_account_user_role_level_5_id
        ELSE dim_crm_opp_owner_role_level_5_id
      END                                                                                                                         AS dim_crm_current_account_set_role_level_5_id,
      sfdc_opportunity.ssp_id,
      sfdc_opportunity.ga_client_id,

      -- flags
      sfdc_opportunity.is_closed,
      sfdc_opportunity.is_won,
      sfdc_opportunity.valid_deal_count,
      sfdc_opportunity.is_refund,
      sfdc_opportunity.is_downgrade,
      sfdc_opportunity.is_swing_deal,
      sfdc_opportunity.is_edu_oss,
      sfdc_opportunity.is_web_portal_purchase,
      sfdc_opportunity.fpa_master_bookings_flag,
      sfdc_opportunity.is_sao,
      sfdc_opportunity.is_sdr_sao,
      sfdc_opportunity.is_net_arr_closed_deal,
      sfdc_opportunity.is_new_logo_first_order,
      sfdc_opportunity.is_net_arr_pipeline_created,
      sfdc_opportunity.is_win_rate_calc,
      sfdc_opportunity.is_closed_won,
      sfdc_opportunity.is_stage_1_plus,
      sfdc_opportunity.is_stage_3_plus,
      sfdc_opportunity.is_stage_4_plus,
      sfdc_opportunity.is_lost,
      sfdc_opportunity.is_open,
      sfdc_opportunity.is_active,
      sfdc_opportunity.is_credit,
      sfdc_opportunity.is_renewal,
      sfdc_opportunity.is_deleted,
      sfdc_opportunity.is_excluded_from_pipeline_created,
      sfdc_opportunity.is_duplicate,
      sfdc_opportunity.is_contract_reset,
      sfdc_opportunity.is_comp_new_logo_override,
      sfdc_opportunity.is_eligible_open_pipeline,
      sfdc_opportunity.is_eligible_asp_analysis,
      sfdc_opportunity.is_eligible_age_analysis,
      sfdc_opportunity.is_eligible_churn_contraction,
      sfdc_opportunity.is_booked_net_arr,
      sales_rep.is_hybrid_user AS is_hybrid_opp_owner_opp,
      sales_rep_account.is_hybrid_user AS is_hybrid_account_owner_opp,
      sfdc_opportunity.is_abm_tier_sao,
      sfdc_opportunity.is_abm_tier_closed_won,

      sfdc_opportunity.primary_solution_architect,
      sfdc_opportunity.product_details,
      sfdc_opportunity.product_category,
      sfdc_opportunity.products_purchased,
      sfdc_opportunity.growth_type,
      sfdc_opportunity.opportunity_deal_size,
      sfdc_opportunity.closed_buckets,

      -- channel fields
      sfdc_opportunity.lead_source,
      sfdc_opportunity.dr_partner_deal_type,
      sfdc_opportunity.dr_partner_engagement,
      sfdc_opportunity.partner_account,
      sfdc_opportunity.dr_status,
      sfdc_opportunity.dr_deal_id,
      sfdc_opportunity.dr_primary_registration,
      sfdc_opportunity.distributor,
      sfdc_opportunity.influence_partner,
      sfdc_opportunity.fulfillment_partner,
      sfdc_opportunity.platform_partner,
      sfdc_opportunity.partner_track,
      sfdc_opportunity.resale_partner_track,
      sfdc_opportunity.is_public_sector_opp,
      sfdc_opportunity.is_registration_from_portal,
      sfdc_opportunity.calculated_discount,
      sfdc_opportunity.partner_discount,
      sfdc_opportunity.partner_discount_calc,
      sfdc_opportunity.partner_margin_percentage,
      sfdc_opportunity.comp_channel_neutral,
      sfdc_opportunity.aggregate_partner,

      -- additive fields
      sfdc_opportunity.incremental_acv                                                                                      AS iacv,
      sfdc_opportunity.net_incremental_acv                                                                                  AS net_iacv,
      sfdc_opportunity.opportunity_based_iacv_to_net_arr_ratio,
      sfdc_opportunity.segment_order_type_iacv_to_net_arr_ratio,
      sfdc_opportunity.calculated_from_ratio_net_arr,
      sfdc_opportunity.net_arr,
      sfdc_opportunity.net_arr_stage_1,
      sfdc_opportunity.xdr_net_arr_stage_1,
      sfdc_opportunity.xdr_net_arr_stage_3,
      sfdc_opportunity.enterprise_agile_planning_net_arr,
      sfdc_opportunity.duo_net_arr,
      sfdc_opportunity.raw_net_arr,
      sfdc_opportunity.created_and_won_same_quarter_net_arr,
      sfdc_opportunity.new_logo_count,
      sfdc_opportunity.amount,
      sfdc_opportunity.recurring_amount,
      sfdc_opportunity.true_up_amount,
      sfdc_opportunity.proserv_amount,
      sfdc_opportunity.other_non_recurring_amount,
      sfdc_opportunity.arr_basis,
      sfdc_opportunity.arr,
      sfdc_opportunity.count_crm_attribution_touchpoints,
      sfdc_opportunity.weighted_linear_iacv,
      sfdc_opportunity.count_campaigns,
      sfdc_opportunity.probability,
      sfdc_opportunity.days_in_sao,
      sfdc_opportunity.open_1plus_deal_count,
      sfdc_opportunity.open_3plus_deal_count,
      sfdc_opportunity.open_4plus_deal_count,
      sfdc_opportunity.booked_deal_count,
      sfdc_opportunity.churned_contraction_deal_count,
      sfdc_opportunity.open_1plus_net_arr,
      sfdc_opportunity.open_3plus_net_arr,
      sfdc_opportunity.open_4plus_net_arr,
      sfdc_opportunity.booked_net_arr,
      sfdc_opportunity.churned_contraction_net_arr,
      sfdc_opportunity.calculated_deal_count,
      sfdc_opportunity.booked_churned_contraction_deal_count,
      sfdc_opportunity.booked_churned_contraction_net_arr,
      sfdc_opportunity.renewal_amount,
      sfdc_opportunity.total_contract_value,
      sfdc_opportunity.days_in_stage,
      sfdc_opportunity.calculated_age_in_days,
      sfdc_opportunity.days_since_last_activity,
      sfdc_opportunity.won_arr_basis_for_clari,
      sfdc_opportunity.arr_basis_for_clari,
      sfdc_opportunity.forecasted_churn_for_clari,
      sfdc_opportunity.override_arr_basis_clari,
      sfdc_opportunity.vsa_start_date_net_arr,
      sfdc_opportunity.cycle_time_in_days,
      sfdc_opportunity.created_arr,
      sfdc_opportunity.closed_won_opps,
      sfdc_opportunity.closed_opps,
      sfdc_opportunity.created_deals,
      sfdc_opportunity.positive_booked_deal_count,
      sfdc_opportunity.positive_booked_net_arr,
      sfdc_opportunity.positive_open_deal_count,
      sfdc_opportunity.positive_open_net_arr,
      sfdc_opportunity.closed_deals,
      sfdc_opportunity.closed_net_arr,
      sfdc_opportunity.number_of_sa_activity_tasks,

      -- PTC related fields
      sfdc_opportunity.ptc_predicted_arr,
      sfdc_opportunity.ptc_predicted_renewal_risk_category

    FROM sfdc_opportunity
    LEFT JOIN crm_account_dimensions
      ON sfdc_opportunity.dim_crm_account_id = crm_account_dimensions.dim_crm_account_id
    LEFT JOIN prep_crm_account
      ON sfdc_opportunity.dim_crm_account_id = prep_crm_account.dim_crm_account_id
    LEFT JOIN sales_qualified_source
      ON sfdc_opportunity.sales_qualified_source = sales_qualified_source.sales_qualified_source_name
    LEFT JOIN order_type
      ON sfdc_opportunity.order_type = order_type.order_type_name
    LEFT JOIN order_type AS order_type_current
      ON sfdc_opportunity.order_type_current = order_type_current.order_type_name
    LEFT JOIN deal_path
      ON sfdc_opportunity.deal_path = deal_path.deal_path_name
    LEFT JOIN sales_segment
      ON sfdc_opportunity.sales_segment = sales_segment.sales_segment_name
    LEFT JOIN prep_crm_user_hierarchy
      ON sfdc_opportunity.dim_crm_opp_owner_stamped_hierarchy_sk = prep_crm_user_hierarchy.dim_crm_user_hierarchy_sk
    LEFT JOIN prep_crm_user_hierarchy AS account_hierarchy
      ON prep_crm_account.dim_crm_parent_account_hierarchy_sk = account_hierarchy.dim_crm_user_hierarchy_sk
    LEFT JOIN dr_partner_engagement
      ON sfdc_opportunity.dr_partner_engagement = dr_partner_engagement.dr_partner_engagement_name
    LEFT JOIN alliance_type
      ON sfdc_opportunity.alliance_type = alliance_type.alliance_type_name
    LEFT JOIN alliance_type AS alliance_type_current
      ON sfdc_opportunity.alliance_type_current = alliance_type_current.alliance_type_name
    LEFT JOIN channel_type
      ON sfdc_opportunity.channel_type = channel_type.channel_type_name
    LEFT JOIN sales_rep
      ON sfdc_opportunity.dim_crm_user_id = sales_rep.dim_crm_user_id
    LEFT JOIN sales_rep AS sales_rep_account
      ON prep_crm_account.dim_crm_user_id = sales_rep_account.dim_crm_user_id
    LEFT JOIN prep_date
      ON prep_date.date_id = sfdc_opportunity.close_date_id

)

{{ dbt_audit(
    cte_ref="final_opportunities",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2020-11-30",
    updated_date="2024-12-30"
) }}
