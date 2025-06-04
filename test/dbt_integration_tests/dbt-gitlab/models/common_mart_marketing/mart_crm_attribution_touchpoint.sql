{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_crm_touchpoint','dim_crm_touchpoint'),
    ('fct_crm_attribution_touchpoint','fct_crm_attribution_touchpoint'),
    ('dim_campaign','dim_campaign'),
    ('fct_campaign','fct_campaign'),
    ('dim_crm_person','dim_crm_person'),
    ('fct_crm_person', 'fct_crm_person'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user','dim_crm_user'),
    ('fct_crm_opportunity','fct_crm_opportunity'),
    ('dim_crm_opportunity', 'dim_crm_opportunity'),
    ('dim_date', 'dim_date')
]) }}

, joined AS (

    SELECT
      -- touchpoint info
      dim_crm_touchpoint.dim_crm_touchpoint_id,
      {{ dbt_utils.generate_surrogate_key(['fct_crm_attribution_touchpoint.dim_crm_person_id','dim_campaign.dim_campaign_id','dim_crm_touchpoint.bizible_touchpoint_date_time']) }} AS touchpoint_person_campaign_date_id,
      dim_crm_touchpoint.bizible_touchpoint_date,
      dim_crm_touchpoint.bizible_touchpoint_date_time,
      dim_crm_touchpoint.bizible_touchpoint_month,
      dim_crm_touchpoint.bizible_touchpoint_position,
      dim_crm_touchpoint.bizible_touchpoint_source,
      dim_crm_touchpoint.bizible_touchpoint_source_type,
      dim_crm_touchpoint.bizible_touchpoint_type,
      dim_crm_touchpoint.touchpoint_offer_type,
      dim_crm_touchpoint.touchpoint_offer_type_grouped,
      dim_crm_touchpoint.bizible_ad_campaign_name,
      dim_crm_touchpoint.bizible_ad_content,
      dim_crm_touchpoint.bizible_ad_group_name,
      dim_crm_touchpoint.bizible_form_url,
      dim_crm_touchpoint.bizible_form_url_raw,
      dim_crm_touchpoint.bizible_landing_page,
      dim_crm_touchpoint.bizible_landing_page_raw,
      dim_crm_touchpoint.bizible_marketing_channel,
      dim_crm_touchpoint.bizible_marketing_channel_path,
      dim_crm_touchpoint.marketing_review_channel_grouping,
      dim_crm_touchpoint.bizible_medium,
      dim_crm_touchpoint.bizible_referrer_page,
      dim_crm_touchpoint.bizible_referrer_page_raw,
      dim_crm_touchpoint.bizible_form_page_utm_content,
      dim_crm_touchpoint.bizible_form_page_utm_budget,
      dim_crm_touchpoint.bizible_form_page_utm_allptnr,
      dim_crm_touchpoint.bizible_form_page_utm_partnerid,
      dim_crm_touchpoint.bizible_landing_page_utm_content,
      dim_crm_touchpoint.bizible_landing_page_utm_budget,
      dim_crm_touchpoint.bizible_landing_page_utm_allptnr,
      dim_crm_touchpoint.bizible_landing_page_utm_partnerid,
      dim_crm_touchpoint.utm_campaign,
      dim_crm_touchpoint.utm_source,
      dim_crm_touchpoint.utm_medium,
      dim_crm_touchpoint.utm_content,
      dim_crm_touchpoint.utm_budget,
      dim_crm_touchpoint.utm_allptnr,
      dim_crm_touchpoint.utm_partnerid,
      dim_crm_touchpoint.utm_campaign_date,
      dim_crm_touchpoint.utm_campaign_region,
      dim_crm_touchpoint.utm_campaign_budget,
      dim_crm_touchpoint.utm_campaign_type,
      dim_crm_touchpoint.utm_campaign_gtm,
      dim_crm_touchpoint.utm_campaign_language,
      dim_crm_touchpoint.utm_campaign_name,
      dim_crm_touchpoint.utm_campaign_agency,
      dim_crm_touchpoint.utm_content_offer,
      dim_crm_touchpoint.utm_content_asset_type,
      dim_crm_touchpoint.utm_content_industry,
      dim_crm_touchpoint.bizible_salesforce_campaign,
      dim_crm_touchpoint.bizible_integrated_campaign_grouping,
      dim_crm_touchpoint.touchpoint_segment,
      dim_crm_touchpoint.gtm_motion,
      dim_crm_touchpoint.integrated_campaign_grouping,
      dim_crm_touchpoint.pipe_name,
      dim_crm_touchpoint.is_dg_influenced,
      dim_crm_touchpoint.is_dg_sourced,
      dim_crm_touchpoint.devrel_campaign_type,
      dim_crm_touchpoint.devrel_campaign_description,
      dim_crm_touchpoint.devrel_campaign_influence_type,
      fct_crm_attribution_touchpoint.opps_per_touchpoint,
      fct_crm_attribution_touchpoint.bizible_count_lead_creation_touch,
      fct_crm_attribution_touchpoint.bizible_count_first_touch,
      fct_crm_attribution_touchpoint.bizible_attribution_percent_full_path,
      fct_crm_attribution_touchpoint.bizible_count_custom_model,
      fct_crm_attribution_touchpoint.bizible_count_u_shaped,
      fct_crm_attribution_touchpoint.bizible_count_w_shaped,
	    fct_crm_attribution_touchpoint.bizible_weight_full_path,
      fct_crm_attribution_touchpoint.bizible_weight_custom_model,
      fct_crm_attribution_touchpoint.bizible_weight_first_touch,
      fct_crm_attribution_touchpoint.bizible_weight_lead_conversion,
      fct_crm_attribution_touchpoint.bizible_weight_u_shaped,
      fct_crm_attribution_touchpoint.bizible_weight_w_shaped,
      fct_crm_attribution_touchpoint.gitlab_model_weight,
      fct_crm_attribution_touchpoint.time_decay_model_weight,
      fct_crm_attribution_touchpoint.data_driven_model_weight,
      (fct_crm_opportunity.net_arr * (fct_crm_attribution_touchpoint.bizible_weight_first_touch / 100)) AS first_net_arr,
      (fct_crm_opportunity.net_arr * (fct_crm_attribution_touchpoint.bizible_weight_w_shaped / 100)) AS w_net_arr,
      (fct_crm_opportunity.net_arr * (fct_crm_attribution_touchpoint.bizible_weight_u_shaped / 100)) AS u_net_arr,
      (fct_crm_opportunity.net_arr * (fct_crm_attribution_touchpoint.bizible_weight_full_path / 100)) AS full_net_arr,
      (fct_crm_opportunity.net_arr * (fct_crm_attribution_touchpoint.bizible_weight_custom_model / 100)) AS custom_net_arr,
      (fct_crm_opportunity.net_arr / NULLIF(fct_crm_attribution_touchpoint.campaigns_per_opp,0)) AS net_arr_per_campaign,
      fct_crm_attribution_touchpoint.bizible_revenue_full_path,
      fct_crm_attribution_touchpoint.bizible_revenue_custom_model,
      fct_crm_attribution_touchpoint.bizible_revenue_first_touch,
      fct_crm_attribution_touchpoint.bizible_revenue_lead_conversion,
      fct_crm_attribution_touchpoint.bizible_revenue_u_shaped,
      fct_crm_attribution_touchpoint.bizible_revenue_w_shaped,
      dim_crm_touchpoint.bizible_created_date, 
      CASE
            WHEN dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_0_pending_acceptance_date
              THEN 'Pre Opp Creation'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_0_pending_acceptance_date AND (dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_1_discovery_date OR fct_crm_opportunity.stage_1_discovery_date IS NULL)
              THEN 'Stage 0'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_1_discovery_date AND (dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_2_scoping_date OR fct_crm_opportunity.stage_2_scoping_date IS NULL)
              THEN 'Stage 1'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_2_scoping_date AND (dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_3_technical_evaluation_date OR fct_crm_opportunity.stage_3_technical_evaluation_date IS NULL)
              THEN 'Stage 2'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_3_technical_evaluation_date AND (dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_4_proposal_date OR fct_crm_opportunity.stage_4_proposal_date IS NULL)
              THEN 'Stage 3'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_4_proposal_date AND (dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_5_negotiating_date OR fct_crm_opportunity.stage_5_negotiating_date IS NULL)
              THEN 'Stage 4'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_5_negotiating_date AND (dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_6_awaiting_signature_date OR fct_crm_opportunity.stage_6_awaiting_signature_date IS NULL)
              THEN 'Stage 5'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_6_awaiting_signature_date AND ((dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_6_closed_won_date OR dim_crm_touchpoint.bizible_touchpoint_date < fct_crm_opportunity.stage_6_closed_lost_date) OR (fct_crm_opportunity.stage_6_closed_won_date IS NULL AND fct_crm_opportunity.stage_6_closed_lost_date IS NULL))
              THEN 'Stage 6 - Awaiting Signature'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_6_closed_won_date AND fct_crm_opportunity.is_closed_won = TRUE
              THEN 'Stage 6 - Closed Won'
            WHEN dim_crm_touchpoint.bizible_touchpoint_date >= fct_crm_opportunity.stage_6_closed_lost_date
              THEN 'Stage 6 - Closed Lost'
            ELSE stage_name||'-Mapping Missing'
          END AS touchpoint_sales_stage,
      dim_crm_touchpoint.keystone_content_name,
      dim_crm_touchpoint.keystone_gitlab_epic,
      dim_crm_touchpoint.keystone_gtm,
      dim_crm_touchpoint.keystone_url_slug,
      dim_crm_touchpoint.keystone_type,

      -- person info
      fct_crm_attribution_touchpoint.dim_crm_person_id,
      dim_crm_person.sfdc_record_id,
      dim_crm_person.sfdc_record_type,
      dim_crm_person.marketo_lead_id,
      dim_crm_person.email_hash,
      dim_crm_person.email_domain,
      dim_crm_person.owner_id,
      dim_crm_person.person_score,
      dim_crm_person.title                                                  AS crm_person_title,
      dim_crm_person.country                                                AS crm_person_country,
      dim_crm_person.state                                                  AS crm_person_state,
      dim_crm_person.status                                                 AS crm_person_status,
      dim_crm_person.lead_source,
      dim_crm_person.lead_source_type,
      dim_crm_person.source_buckets                                         AS crm_person_source_buckets,
      dim_crm_person.net_new_source_categories,
      dim_crm_person.crm_partner_id,
      fct_crm_person.created_date                                           AS crm_person_created_date,
      fct_crm_person.inquiry_date,
      fct_crm_person.mql_date_first,
      fct_crm_person.mql_date_latest,
      fct_crm_person.legacy_mql_date_first,
      fct_crm_person.legacy_mql_date_latest,
      fct_crm_person.accepted_date,
      fct_crm_person.qualifying_date,
      fct_crm_person.qualified_date,
      fct_crm_person.converted_date,
      fct_crm_person.is_mql,
      fct_crm_person.is_inquiry,
      fct_crm_person.mql_count,
      fct_crm_person.last_utm_content,
      fct_crm_person.last_utm_campaign,
      dim_crm_person.account_demographics_sales_segment,
      dim_crm_person.account_demographics_geo,
      dim_crm_person.account_demographics_region,
      dim_crm_person.account_demographics_area,
      dim_crm_person.is_partner_recalled,

      -- campaign info
      dim_campaign.dim_campaign_id,
      dim_campaign.campaign_name,
      dim_campaign.is_active                                                AS campaign_is_active,
      dim_campaign.status                                                   AS campagin_status,
      dim_campaign.type,
      dim_campaign.description,
      dim_campaign.budget_holder,
      dim_campaign.bizible_touchpoint_enabled_setting,
      dim_campaign.strategic_marketing_contribution,
      dim_campaign.large_bucket,
      dim_campaign.reporting_type,
      dim_campaign.allocadia_id,
      dim_campaign.is_a_channel_partner_involved,
      dim_campaign.is_an_alliance_partner_involved,
      dim_campaign.is_this_an_in_person_event,
      dim_campaign.will_there_be_mdf_funding,
      dim_campaign.alliance_partner_name,
      dim_campaign.channel_partner_name,
      dim_campaign.sales_play,
      dim_campaign.total_planned_mqls,
      fct_campaign.dim_parent_campaign_id,
      fct_campaign.campaign_owner_id,
      fct_campaign.created_by_id                                            AS campaign_created_by_id,
      fct_campaign.start_date                                               AS campaign_start_date,
      fct_campaign.end_date                                                 AS campaign_end_date,
      fct_campaign.created_date                                             AS campaign_created_date,
      fct_campaign.last_modified_date                                       AS campaign_last_modified_date,
      fct_campaign.last_activity_date                                       AS campaign_last_activity_date,
      fct_campaign.region                                                   AS campaign_region,
      fct_campaign.sub_region                                               AS campaign_sub_region,
      fct_campaign.budgeted_cost,
      fct_campaign.expected_response,
      fct_campaign.expected_revenue,
      fct_campaign.actual_cost,
      fct_campaign.amount_all_opportunities,
      fct_campaign.amount_won_opportunities,
      fct_campaign.count_contacts,
      fct_campaign.count_converted_leads,
      fct_campaign.count_leads,
      fct_campaign.count_opportunities,
      fct_campaign.count_responses,
      fct_campaign.count_won_opportunities,
      fct_campaign.count_sent,

      --planned values
      fct_campaign.planned_inquiry,
      fct_campaign.planned_mql,
      fct_campaign.planned_pipeline,
      fct_campaign.planned_sao,
      fct_campaign.planned_won,
      fct_campaign.planned_roi,
      fct_campaign.total_planned_mql,

      -- campaign owner info
      campaign_owner.user_name                             AS campaign_rep_name,
      campaign_owner.title                                 AS campaign_rep_title,
      campaign_owner.team                                  AS campaign_rep_team,
      campaign_owner.is_active                             AS campaign_rep_is_active,
      campaign_owner.user_role_name                        AS campaign_rep_role_name,
      campaign_owner.crm_user_sales_segment                AS campaign_crm_user_segment_name_live,
      campaign_owner.crm_user_geo                          AS campaign_crm_user_geo_name_live,
      campaign_owner.crm_user_region                       AS campaign_crm_user_region_name_live,
      campaign_owner.crm_user_area                         AS campaign_crm_user_area_name_live,

      -- sales rep info
      dim_crm_user.user_name                                AS rep_name,
      dim_crm_user.title                                    AS rep_title,
      dim_crm_user.team,
      dim_crm_user.is_active                                AS rep_is_active,
      dim_crm_user.user_role_name,
      dim_crm_user.crm_user_sales_segment                   AS touchpoint_crm_user_segment_name_live,
      dim_crm_user.crm_user_geo                             AS touchpoint_crm_user_geo_name_live,
      dim_crm_user.crm_user_region                          AS touchpoint_crm_user_region_name_live,
      dim_crm_user.crm_user_area                            AS touchpoint_crm_user_area_name_live,
      dim_crm_user.sdr_sales_segment,
      dim_crm_user.sdr_region,

      -- account info
      dim_crm_account.dim_crm_account_id,
      dim_crm_account.crm_account_name,
      dim_crm_account.crm_account_billing_country,
      dim_crm_account.crm_account_industry,
      dim_crm_account.crm_account_gtm_strategy,
      dim_crm_account.crm_account_focus_account,
      dim_crm_account.health_number,
      dim_crm_account.health_score_color,
      dim_crm_account.dim_parent_crm_account_id,
      dim_crm_account.parent_crm_account_name,
      dim_crm_account.parent_crm_account_sales_segment,
      dim_crm_account.parent_crm_account_industry,
      dim_crm_account.parent_crm_account_territory                          AS parent_crm_account_territory,
      dim_crm_account.parent_crm_account_region                             AS parent_crm_account_region,
      dim_crm_account.parent_crm_account_area                               AS parent_crm_account_area,
      dim_crm_account.crm_account_owner_user_segment,
      dim_crm_account.record_type_id,
      dim_crm_account.gitlab_com_user,
      dim_crm_account.crm_account_type,
      dim_crm_account.technical_account_manager,
      dim_crm_account.merged_to_account_id,
      dim_crm_account.is_reseller,
      dim_crm_account.is_focus_partner,

      -- opportunity info
      fct_crm_attribution_touchpoint.dim_crm_opportunity_id,
      fct_crm_opportunity.sales_accepted_date,
      fct_crm_opportunity.close_date                                       AS opportunity_close_date,
      fct_crm_opportunity.created_date                                     AS opportunity_created_date,
      fct_crm_opportunity.arr_created_date                                 AS pipeline_created_date,
      dim_crm_opportunity.is_won,
      fct_crm_opportunity.is_net_arr_pipeline_created,
      fct_crm_opportunity.is_net_arr_closed_deal,
      fct_crm_opportunity.is_closed,
      fct_crm_opportunity.days_in_sao,
      fct_crm_opportunity.iacv,
      fct_crm_opportunity.net_arr,
      fct_crm_opportunity.amount,
      dim_crm_opportunity.is_edu_oss,
      dim_crm_opportunity.stage_name,
      dim_crm_opportunity.reason_for_loss,
      fct_crm_opportunity.is_sao,
      fct_crm_attribution_touchpoint.is_mgp_opportunity,
      fct_crm_attribution_touchpoint.is_mgp_channel_based,
      dim_crm_opportunity.deal_path AS deal_path_name,
      dim_crm_opportunity.order_type,
      dim_crm_opportunity.sales_qualified_source AS sales_qualified_source_name,
      dim_crm_opportunity.subscription_type,
      fct_crm_opportunity.closed_buckets,
      dim_crm_opportunity.source_buckets                                   AS opportunity_source_buckets,
      dim_crm_opportunity.crm_sales_dev_rep_id,
      dim_crm_opportunity.crm_business_dev_rep_id,
      dim_crm_opportunity.sdr_or_bdr,
      dim_crm_opportunity.opportunity_development_representative,
      dim_crm_opportunity.is_web_portal_purchase,
      fct_crm_opportunity.count_crm_attribution_touchpoints                AS crm_attribution_touchpoints_per_opp,
      fct_crm_opportunity.weighted_linear_iacv,
      fct_crm_opportunity.count_campaigns                                  AS count_campaigns_per_opp,
      (fct_crm_opportunity.iacv / NULLIF(fct_crm_opportunity.count_campaigns,0))     AS iacv_per_campaign,

      -- bizible influenced
       CASE
        WHEN dim_campaign.budget_holder = 'fmm'
              OR campaign_rep_role_name = 'Field Marketing Manager'
              OR LOWER(dim_crm_touchpoint.utm_content) LIKE '%field%'
              OR LOWER(dim_campaign.type) = 'field event'
              OR LOWER(dim_crm_person.lead_source) = 'field event'
          THEN 1
        ELSE 0
      END AS is_fmm_influenced,
      CASE
        WHEN dim_crm_touchpoint.bizible_touchpoint_position LIKE '%FT%' 
          AND is_fmm_influenced = 1 
          THEN 1
        ELSE 0
      END AS is_fmm_sourced,
    --budget holder
    {{integrated_budget_holder(
      'dim_campaign.budget_holder',
      'dim_crm_touchpoint.utm_budget',
      'dim_crm_touchpoint.bizible_ad_campaign_name',
      'dim_crm_touchpoint.utm_medium',
      'campaign_owner.user_role_name'
      ) 
    }}
    FROM fct_crm_attribution_touchpoint
    LEFT JOIN dim_crm_touchpoint
      ON fct_crm_attribution_touchpoint.dim_crm_touchpoint_id = dim_crm_touchpoint.dim_crm_touchpoint_id
    LEFT JOIN dim_campaign
      ON fct_crm_attribution_touchpoint.dim_campaign_id = dim_campaign.dim_campaign_id
    LEFT JOIN fct_campaign
      ON fct_crm_attribution_touchpoint.dim_campaign_id = fct_campaign.dim_campaign_id
    LEFT JOIN dim_crm_person
      ON fct_crm_attribution_touchpoint.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN fct_crm_person
      ON fct_crm_attribution_touchpoint.dim_crm_person_id = fct_crm_person.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON fct_crm_attribution_touchpoint.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_user
      ON fct_crm_attribution_touchpoint.dim_crm_user_id = dim_crm_user.dim_crm_user_id
    LEFT JOIN dim_crm_user AS campaign_owner
      ON fct_campaign.campaign_owner_id = campaign_owner.dim_crm_user_id
    LEFT JOIN fct_crm_opportunity
      ON fct_crm_attribution_touchpoint.dim_crm_opportunity_id = fct_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN dim_crm_opportunity
      ON fct_crm_attribution_touchpoint.dim_crm_opportunity_id = dim_crm_opportunity.dim_crm_opportunity_id

), linear_base AS ( --the number of touches a given opp has in total
    --linear attribution Net_Arr of an opp / all touches (count_touches) for each opp - weighted by the number of touches in the given bucket (campaign,channel,etc)

    SELECT
      dim_crm_opportunity_id, 
      net_arr,
      COUNT(dim_crm_touchpoint_id) AS touchpoints_per_opportunity,
      net_arr/NULLIF(touchpoints_per_opportunity,0) AS weighted_linear_net_arr
    FROM  joined
    {{dbt_utils.group_by(n=2)}}

), final AS (

    SELECT
      joined.*,
      linear_base.touchpoints_per_opportunity,
      (joined.opps_per_touchpoint / NULLIF(linear_base.touchpoints_per_opportunity,0)) AS l_weight,
      (joined.net_arr * l_weight) AS linear_net_arr
    FROM joined
    LEFT JOIN linear_base 
      ON joined.dim_crm_opportunity_id = linear_base.dim_crm_opportunity_id
    WHERE joined.dim_crm_touchpoint_id IS NOT NULL

)

SELECT *
FROM final
