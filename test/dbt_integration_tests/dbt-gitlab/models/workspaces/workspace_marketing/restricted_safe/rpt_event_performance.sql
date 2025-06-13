{{ config(
    materialized='table'
) }}

{{ simple_cte([

    ('fct_campaign','fct_campaign'),
    ('dim_campaign','dim_campaign'),
    ('dim_crm_user','dim_crm_user'),
    ('dim_date', 'dim_date'),
    ('mart_crm_person','mart_crm_person'),
    ('mart_crm_opportunity','mart_crm_opportunity'),
    ('sfdc_campaign_member','sfdc_campaign_member'),
    ('mart_crm_opportunity_daily_snapshot','mart_crm_opportunity_daily_snapshot'),
    ('mart_crm_account','mart_crm_account'),
    ('sfdc_bizible_attribution_touchpoint_snapshots_source','sfdc_bizible_attribution_touchpoint_snapshots_source')
  ]) 
}}

, campaigns AS (
  SELECT
    dim_campaign.dim_campaign_id,
    fct_campaign.dim_parent_campaign_id,
    dim_campaign.campaign_name,
    dim_campaign.status                                                                         AS campaign_status,
    dim_campaign.description                                                                    AS campaign_description,
    dim_campaign.budget_holder                                                                  AS budget_holder,
    dim_campaign.allocadia_id,
    dim_campaign.is_a_channel_partner_involved,
    dim_campaign.is_an_alliance_partner_involved,
    dim_campaign.is_this_an_in_person_event,
    dim_campaign.alliance_partner_name,
    dim_campaign.channel_partner_name,
    dim_campaign.total_planned_mqls,
    dim_campaign.will_there_be_mdf_funding,
    dim_campaign.campaign_partner_crm_id,
    dim_campaign.type                                                                           AS campaign_type,
    fct_campaign.campaign_owner_id,
    campaign_owner.user_name                                                                    AS campaign_owner_name,
    campaign_owner.manager_name                                                                 AS campaign_owner_manager_name,
    fct_campaign.created_by_id                                                                  AS campaign_created_by_id,
    fct_campaign.start_date                                                                     AS campaign_start_date,
    COALESCE(TRY_TO_DATE(LEFT(dim_campaign.campaign_name, 8), 'YYYYMMDD'), campaign_start_date) AS true_event_date,
    fct_campaign.end_date                                                                       AS campaign_end_date,
    fct_campaign.created_date                                                                   AS campaign_created_date,
    fct_campaign.region                                                                         AS campaign_region,
    fct_campaign.sub_region                                                                     AS campaign_sub_region,
    fct_campaign.budgeted_cost                                                                  AS campaign_budgeted_cost,
    fct_campaign.actual_cost                                                                    AS campaign_actual_cost,
    fct_campaign.expected_response                                                              AS campaign_expected_response,
    fct_campaign.expected_revenue                                                               AS campaign_expected_revenue,
    fct_campaign.amount_all_opportunities,
    fct_campaign.amount_won_opportunities,
    fct_campaign.count_contacts,
    fct_campaign.count_converted_leads,
    fct_campaign.count_leads,
    fct_campaign.count_opportunities,
    fct_campaign.count_responses,
    fct_campaign.count_won_opportunities,
    dim_campaign.series_campaign_id,
    dim_campaign.series_campaign_name,
    CASE 
    WHEN true_event_date > CURRENT_DATE 
    THEN TRUE END AS future_event_flag,

    -- dates
    campaign_start.fiscal_quarter_name_fy                                                       AS campaign_fiscal_quarter_name_fy,
    campaign_start.fiscal_year                                                                  AS campaign_fiscal_year

  FROM
    fct_campaign
  LEFT JOIN dim_campaign
    ON fct_campaign.dim_campaign_id = dim_campaign.dim_campaign_id
  LEFT JOIN dim_crm_user AS campaign_owner
    ON fct_campaign.campaign_owner_id = campaign_owner.dim_crm_user_id
  INNER JOIN dim_date AS campaign_start
    ON fct_campaign.start_date_id = campaign_start.date_id

  WHERE true_event_date >= '2023-02-01'
  AND campaign_type in ('Owned Event', 'Workshop', 'Executive Roundtables', 'Webcast', 'Sponsored Webcast', 'Conference', 'Speaking Session', 'Virtual Sponsorship', 'Self-Service Virtual Event', 'Vendor Arranged Meetings')
  AND campaign_status in ('In Progress', 'Planned', 'Completed')

),

campaign_members AS (
  SELECT

    -- id's
    mart_crm_person.dim_crm_person_id,
    sfdc_campaign_member.campaign_id                           AS dim_campaign_id,
    sfdc_campaign_member.lead_or_contact_id,
    mart_crm_person.marketo_lead_id,

    -- member
    sfdc_campaign_member.campaign_member_has_responded,
    sfdc_campaign_member.campaign_member_response_date,
    sfdc_campaign_member.is_mql_after_campaign,
    sfdc_campaign_member.campaign_member_status,
    sfdc_campaign_member.campaign_member_created_date,
    sfdc_campaign_member.utm_campaign,
    sfdc_campaign_member.utm_medium,
    sfdc_campaign_member.utm_source,
    sfdc_campaign_member.last_form_submission_page,
    campaigns.true_event_date,
    campaigns.campaign_name,

    -- person
    mart_crm_person.dim_crm_user_id                               AS person_owner_id,
    COALESCE(mart_crm_person.dim_crm_account_id, 'No Account ID') AS person_account_id,
    mart_crm_person.sfdc_record_id                                AS person_sfdc_record_id,
    mart_crm_person.mql_date_latest,
    mart_crm_person.created_date                                  AS person_created_date,
    mart_crm_person.inquiry_date,
    mart_crm_person.accepted_date                                 AS person_accepted_date,
    mart_crm_person.email_domain_type,
    mart_crm_person.status                                        AS person_status,
    mart_crm_person.lead_source,
    mart_crm_person.crm_partner_id                                AS person_crm_person_id,
    mart_crm_person.is_partner_recalled                           AS person_is_partner_recalled,
    mart_crm_person.prospect_share_status                         AS person_prospect_share_status,
    mart_crm_person.partner_prospect_status                       AS person_partner_prospect_status,
    mart_crm_person.partner_prospect_owner_name                   AS person_partner_prospect_owner_name,
    mart_crm_person.partner_prospect_id                           AS person_partner_prospect_id,
    mart_crm_person.is_mql,
    mart_crm_person.is_inquiry,
    mart_crm_person.is_first_order_person                         AS person_is_first_order,
    mart_crm_person.account_demographics_sales_segment            AS person_account_demographics_sales_segment,
    mart_crm_person.account_demographics_sales_segment_grouped    AS person_account_demographics_sales_segment_grouped,
    mart_crm_person.account_demographics_geo                      AS person_account_demographics_geo,
    mart_crm_person.account_demographics_region                   AS person_account_demographics_region,
    mart_crm_person.account_demographics_area                     AS person_account_demographics_area,

    -- partner from the campaign
    campaign_partner_account.crm_account_name                     AS campaign_partner_crm_account_name,
    campaign_partner_account.parent_crm_account_name              AS campaign_partner_parent_crm_account_name,
    campaign_partner_account.crm_account_owner                    AS campaign_partner_crm_account_owner

  FROM sfdc_campaign_member
  INNER JOIN campaigns 
    ON sfdc_campaign_member.campaign_id = campaigns.dim_campaign_id
  LEFT JOIN mart_crm_person
    ON sfdc_campaign_member.lead_or_contact_id = mart_crm_person.sfdc_record_id
  -- partner from campaigns
  LEFT JOIN mart_crm_account AS campaign_partner_account
    ON campaigns.campaign_partner_crm_id = campaign_partner_account.dim_crm_account_id
  WHERE mart_crm_person.email_domain != 'gitlab.com'

),

account_open_pipeline_live AS (
  SELECT
    mart_crm_opportunity.dim_crm_account_id,
    SUM(COALESCE(net_arr, 0)) AS open_pipeline_live
  FROM mart_crm_opportunity
  WHERE 
    is_net_arr_pipeline_created = TRUE AND 
    is_eligible_open_pipeline = 1 AND 
    is_open = 1
  {{dbt_utils.group_by(n=1)}}

),

account_summary AS (
  SELECT 
    dim_campaign_id,
    person_account_id                                                                         AS dim_crm_account_id,
    true_event_date,
    campaign_name,
    open_pipeline_live,
    COUNT(DISTINCT
          CASE
          WHEN campaign_member_status IN ('Attended On-demand', 'No Show', 'Follow-up Requested', 'Meeting Requested', 'Registered', 'Attended', 'Subscribed to Updates', 'Cancelled', 'Meeting No Show', 'Visited Booth', 'Meeting Attended', 'Follow Up Requested') 
          THEN dim_crm_person_id
          END)                                                         AS registered_leads,
    COUNT(
      DISTINCT 
      CASE 
      WHEN campaign_member_has_responded = TRUE 
      THEN dim_crm_person_id 
      END) AS attended_leads
  FROM
    campaign_members
  LEFT JOIN
    account_open_pipeline_live
    ON campaign_members.person_account_id = account_open_pipeline_live.dim_crm_account_id
  {{dbt_utils.group_by(n=5)}}
),




--SNAPSHOT MODELS

snapshot_dates AS (
  SELECT 
    dim_campaign_id,
    DATEADD(DAY, -2, current_date) AS date_day,
    'Current Date'  AS event_snapshot_type
  FROM 
  campaigns
  UNION
  SELECT 
    dim_campaign_id,
    true_event_date AS date_day,
    'Event Date'    AS event_snapshot_type
  FROM 
  campaigns
  UNION
  SELECT 
    dim_campaign_id,
    DATEADD(DAY, 14, true_event_date) AS date_day,
    '14 Days Post Event'              AS event_snapshot_type
  FROM 
  campaigns
  UNION
  SELECT 
    dim_campaign_id,
    DATEADD(DAY, 30, true_event_date) AS date_day,
    '30 Days Post Event'              AS event_snapshot_type
  FROM 
  campaigns
  UNION
  SELECT 
    dim_campaign_id,
    DATEADD(DAY, 90, true_event_date) AS date_day,
    '90 Days Post Event'              AS event_snapshot_type
  FROM 
  campaigns
),

snapshot_opportunity_dates AS (
  SELECT DISTINCT 
  date_day 
  FROM 
  snapshot_dates
),


opportunity_snapshot_base AS (
  SELECT 
    snapshot.dim_crm_opportunity_id,
    snapshot.dim_crm_account_id,
    account.crm_account_name                  AS account_name,
    snapshot.dim_parent_crm_account_id,
    account.parent_crm_account_name,
    live.opportunity_category,
    live.subscription_type,
    live.order_type,
    live.sales_qualified_source_name,
    snapshot.opportunity_name,
    snapshot.product_category,
    snapshot.product_details,
    snapshot.stage_name as snapshot_stage_name,
    live.stage_name as live_stage_name,
    CASE 
      WHEN snapshot.stage_name != live.stage_name 
      THEN  snapshot.stage_name || ' -> ' || live.stage_name 
      ELSE 'No Progression'
    END AS opportunity_stage_progression,
    --Account Info
    account.owner_role                        AS account_owner_role,
    account.parent_crm_account_territory,
    live.parent_crm_account_sales_segment,
    live.parent_crm_account_geo,
    live.parent_crm_account_region,
    live.parent_crm_account_area,
    live.parent_crm_account_geo_pubsec_segment,
    live.report_geo,
    live.report_region,
    live.report_area,
    live.report_segment,
    live.pipe_council_grouping,
    live.report_geo_pubsec_segment,
    live.report_role_level_1,
    live.report_role_level_2,
    live.report_role_level_3,
    live.opportunity_owner_role,
    --Dates 
    snapshot.created_date,
    snapshot.sales_accepted_date,
    snapshot.pipeline_created_date,
    snapshot.pipeline_created_fiscal_quarter_name,
    snapshot.pipeline_created_fiscal_year,
    snapshot.net_arr_created_date,
    snapshot.close_date,
    snapshot.close_fiscal_quarter_name,
    snapshot.snapshot_date                    AS opportunity_snapshot_date,
    dim_date.day_of_fiscal_quarter_normalised AS pipeline_created_day_of_fiscal_quarter_normalised,
    dim_date.day_of_fiscal_year_normalised    AS pipeline_created_day_of_fiscal_year_normalised,
    --Flags
    live.is_sao,
    live.is_won,
    live.is_web_portal_purchase,
    live.is_edu_oss,
    live.is_net_arr_pipeline_created,
    live.is_open,
    live.is_lost,
    live.is_closed,
    live.is_renewal,
    live.is_refund,
    live.is_credit                            AS is_credit_flag,
    snapshot.is_eligible_open_pipeline        AS snapshot_is_eligible_open_pipeline,
    snapshot.is_net_arr_pipeline_created      AS snapshot_is_net_arr_pipeline_created,
    snapshot.is_booked_net_arr                AS snapshot_is_booked_net_arr,
    --    is_eligible_sao_flag,
    live.is_eligible_open_pipeline            AS is_eligible_open_pipeline_flag,
    live.is_booked_net_arr                    AS is_booked_net_arr_flag,
    live.is_eligible_age_analysis             AS is_eligible_age_analysis_flag,

    --Metrics
    snapshot.net_arr                          AS opp_net_arr

  FROM
    mart_crm_opportunity_daily_snapshot AS snapshot
  INNER JOIN snapshot_opportunity_dates
    ON snapshot.snapshot_date = snapshot_opportunity_dates.date_day
  LEFT JOIN mart_crm_opportunity AS live
    ON snapshot.dim_crm_opportunity_id = live.dim_crm_opportunity_id
  LEFT JOIN mart_crm_account AS account
    ON snapshot.dim_crm_account_id = account.dim_crm_account_id
  LEFT JOIN dim_date
    ON snapshot.pipeline_created_date = dim_date.date_day
  WHERE snapshot.dim_crm_account_id != '0014M00001kGcORQA0'  -- test account
),

opportunity_campaign_snapshot_prep AS (
  SELECT
    account_summary.dim_crm_account_id,
    account_summary.dim_campaign_id,
    account_summary.campaign_name,
    opportunity_snapshot_base.dim_crm_opportunity_id,
    opportunity_snapshot_base.snapshot_is_eligible_open_pipeline,
    opportunity_snapshot_base.pipeline_created_date,
    opportunity_snapshot_base.is_net_arr_pipeline_created,
    opportunity_snapshot_base.snapshot_is_net_arr_pipeline_created,
    account_summary.true_event_date,
    snapshot_dates.event_snapshot_type,
    snapshot_dates.date_day AS snapshot_date,
    COALESCE(attended_leads > 0 AND account_summary.dim_crm_account_id IS NOT NULL, FALSE)  AS account_has_attended_flag,          
    --METRICS 
    account_summary.registered_leads,
    account_summary.attended_leads,
    account_summary.open_pipeline_live,
    opportunity_snapshot_base.opp_net_arr,
    CASE 
      WHEN opportunity_snapshot_base.pipeline_created_date > true_event_date AND 
      opportunity_snapshot_base.is_net_arr_pipeline_created 
      THEN opp_net_arr 
    END
    AS sourced_pipeline_post_event,
    CASE 
      WHEN opportunity_snapshot_base.pipeline_created_date > true_event_date AND 
      opportunity_snapshot_base.is_net_arr_pipeline_created 
      THEN opportunity_snapshot_base.dim_crm_opportunity_id 
    END
    AS sourced_opps_post_event,

    CASE 
      WHEN opportunity_snapshot_base.snapshot_is_eligible_open_pipeline = 1 AND 
      opportunity_snapshot_base.is_net_arr_pipeline_created 
      THEN opp_net_arr 
    END
    AS open_pipeline, 
    CASE 
      WHEN opportunity_snapshot_base.snapshot_is_eligible_open_pipeline = 1 AND 
      opportunity_snapshot_base.is_net_arr_pipeline_created 
      THEN opportunity_snapshot_base.dim_crm_opportunity_id 
    END
    AS open_pipeline_opps,
    CASE 
      WHEN opportunity_snapshot_base.snapshot_is_net_arr_pipeline_created = 1 
      THEN opp_net_arr 
    END AS snapshot_pipeline_created,
    CASE 
      WHEN opportunity_snapshot_base.snapshot_is_net_arr_pipeline_created = 1 
      THEN opportunity_snapshot_base.dim_crm_opportunity_id 
    END AS snapshot_pipeline_created_opps
  FROM
    account_summary
  LEFT JOIN
    snapshot_dates
    ON account_summary.dim_campaign_id = snapshot_dates.dim_campaign_id
  LEFT JOIN
    opportunity_snapshot_base
    ON account_summary.dim_crm_account_id = opportunity_snapshot_base.dim_crm_account_id
      AND snapshot_dates.date_day = opportunity_snapshot_base.opportunity_snapshot_date
 
),

eligible_opps AS (
    SELECT DISTINCT
    opportunity_campaign_snapshot_prep.dim_crm_opportunity_id,
    opportunity_campaign_snapshot_prep.dim_campaign_id,
    opportunity_campaign_snapshot_prep.dim_crm_account_id,
    CASE 
      WHEN event_snapshot_type = 'Event Date' AND 
      snapshot_is_eligible_open_pipeline = TRUE AND 
      is_net_arr_pipeline_created = TRUE 
      THEN TRUE 
      ELSE FALSE 
    END AS open_pipeline_at_event_date_flag,
    CASE 
      WHEN event_snapshot_type = 'Current Date' AND 
      opportunity_campaign_snapshot_prep.pipeline_created_date <= opportunity_campaign_snapshot_prep.true_event_date AND 
      snapshot_is_eligible_open_pipeline = TRUE AND 
      is_net_arr_pipeline_created = TRUE 
      THEN TRUE 
      ELSE FALSE 
    END AS open_pipeline_current_date_flag,

    CASE 
      WHEN opportunity_campaign_snapshot_prep.pipeline_created_date >= opportunity_campaign_snapshot_prep.true_event_date AND 
      opportunity_campaign_snapshot_prep.is_net_arr_pipeline_created
      THEN TRUE 
      ELSE FALSE  
      END AS sourced_pipeline_post_event_flag,      
    FROM opportunity_campaign_snapshot_prep
),

opportunity_campaign_snapshot_base AS (
    SELECT DISTINCT
    opportunity_campaign_snapshot_prep.*,
    eligible_opps.open_pipeline_at_event_date_flag,
    eligible_opps.open_pipeline_current_date_flag,
    eligible_opps.sourced_pipeline_post_event_flag
    FROM 
    opportunity_campaign_snapshot_prep
    LEFT JOIN 
    eligible_opps
    ON 
    opportunity_campaign_snapshot_prep.dim_crm_opportunity_id = eligible_opps.dim_crm_opportunity_id
    AND 
    opportunity_campaign_snapshot_prep.dim_crm_account_id = eligible_opps.dim_crm_account_id
    AND 
    opportunity_campaign_snapshot_prep.dim_campaign_id = eligible_opps.dim_campaign_id
    WHERE 
    eligible_opps.open_pipeline_at_event_date_flag = TRUE 
    OR eligible_opps.sourced_pipeline_post_event_flag = TRUE 
    OR eligible_opps.open_pipeline_current_date_flag = TRUE
),


attribution_touchpoint_snapshot_base AS (
  SELECT 
    snapshot_dates.date_day                                                                AS touchpoint_snapshot_date,
    snapshot_dates.dim_campaign_id,
    snapshot_dates.event_snapshot_type,
    sfdc_bizible_attribution_touchpoint_snapshots_source.touchpoint_id                     AS dim_crm_touchpoint_id,
    sfdc_bizible_attribution_touchpoint_snapshots_source.opportunity_id                    AS dim_crm_opportunity_id,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_touchpoint_date,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_weight_custom_model / 100 AS bizible_count_custom_model,
    sfdc_bizible_attribution_touchpoint_snapshots_source.bizible_weight_custom_model
  FROM
    sfdc_bizible_attribution_touchpoint_snapshots_source
  INNER JOIN snapshot_dates ON
    ( (dbt_valid_from <= date_day AND dbt_valid_to > date_day) OR 
      (dbt_valid_from <= date_day AND dbt_valid_to IS NULL)
    ) 
    AND sfdc_bizible_attribution_touchpoint_snapshots_source.campaign_id = snapshot_dates.dim_campaign_id
),
-- TOUCHPOINT GRAIN COMBINED MODEL WITH OPP INFORMATION

combined_models AS (
  SELECT 
  --IDs
    opportunity_snapshot_base.dim_crm_opportunity_id,
    opportunity_snapshot_base.dim_crm_account_id,
    opportunity_snapshot_base.dim_parent_crm_account_id,
    attribution_touchpoint_snapshot_base.dim_crm_touchpoint_id,
    attribution_touchpoint_snapshot_base.dim_campaign_id,
    --Dates
    opportunity_snapshot_base.created_date,
    opportunity_snapshot_base.sales_accepted_date,
    opportunity_snapshot_base.pipeline_created_date,
    opportunity_snapshot_base.pipeline_created_fiscal_quarter_name,
    opportunity_snapshot_base.pipeline_created_fiscal_year,
    opportunity_snapshot_base.pipeline_created_day_of_fiscal_quarter_normalised,
    opportunity_snapshot_base.pipeline_created_day_of_fiscal_year_normalised,
    opportunity_snapshot_base.net_arr_created_date,
    opportunity_snapshot_base.close_date,
    opportunity_snapshot_base.close_fiscal_quarter_name,
    attribution_touchpoint_snapshot_base.bizible_touchpoint_date,
    attribution_touchpoint_snapshot_base.touchpoint_snapshot_date,
    opportunity_snapshot_base.opportunity_snapshot_date,

    --Account Info
    opportunity_snapshot_base.account_owner_role,
    opportunity_snapshot_base.parent_crm_account_territory,
    opportunity_snapshot_base.parent_crm_account_sales_segment,
    opportunity_snapshot_base.parent_crm_account_geo,
    opportunity_snapshot_base.parent_crm_account_region,
    opportunity_snapshot_base.parent_crm_account_area,
    opportunity_snapshot_base.parent_crm_account_geo_pubsec_segment,
    opportunity_snapshot_base.account_name,
    opportunity_snapshot_base.parent_crm_account_name,
    opportunity_snapshot_base.report_geo,
    opportunity_snapshot_base.report_region,
    opportunity_snapshot_base.report_area,
    opportunity_snapshot_base.report_segment,
    opportunity_snapshot_base.pipe_council_grouping,
    opportunity_snapshot_base.report_geo_pubsec_segment,
    opportunity_snapshot_base.report_role_level_1,
    opportunity_snapshot_base.report_role_level_2,
    opportunity_snapshot_base.report_role_level_3,

    --Touchpoint Dimensions
    attribution_touchpoint_snapshot_base.event_snapshot_type,
    --Campaign Dimensions 
    campaigns.campaign_name,
    campaigns.campaign_status,
    campaigns.campaign_description,
    campaigns.budget_holder,
    campaigns.allocadia_id,
    campaigns.is_a_channel_partner_involved,
    campaigns.is_an_alliance_partner_involved,
    campaigns.is_this_an_in_person_event,
    campaigns.alliance_partner_name,
    campaigns.channel_partner_name,
    campaigns.total_planned_mqls,
    campaigns.will_there_be_mdf_funding,
    campaigns.campaign_partner_crm_id,
    campaigns.campaign_type,
    campaigns.campaign_owner_id,
    campaigns.campaign_owner_name,
    campaigns.campaign_owner_manager_name,
    campaigns.campaign_created_by_id,
    campaigns.campaign_start_date,
    campaigns.true_event_date,

    --Opportunity Dimensions
    opportunity_snapshot_base.opportunity_category,
    opportunity_snapshot_base.subscription_type,
    opportunity_snapshot_base.order_type,
    opportunity_snapshot_base.sales_qualified_source_name,
    opportunity_snapshot_base.snapshot_stage_name,
    opportunity_snapshot_base.live_stage_name,

    --Flags
    opportunity_snapshot_base.is_sao,
    opportunity_snapshot_base.is_won,
    opportunity_snapshot_base.is_web_portal_purchase,
    opportunity_snapshot_base.is_edu_oss,
    opportunity_snapshot_base.is_open,
    opportunity_snapshot_base.is_lost,
    opportunity_snapshot_base.is_closed,
    opportunity_snapshot_base.is_renewal,
    opportunity_snapshot_base.is_refund,
    opportunity_snapshot_base.is_credit_flag,
    opportunity_snapshot_base.is_net_arr_pipeline_created,
    opportunity_snapshot_base.is_booked_net_arr_flag,
    opportunity_snapshot_base.is_eligible_age_analysis_flag,
    opportunity_snapshot_base.snapshot_is_eligible_open_pipeline,
    opportunity_snapshot_base.snapshot_is_net_arr_pipeline_created,
    opportunity_snapshot_base.snapshot_is_booked_net_arr,

    --Metrics
    opportunity_snapshot_base.opp_net_arr,
    attribution_touchpoint_snapshot_base.bizible_weight_custom_model,
    attribution_touchpoint_snapshot_base.bizible_weight_custom_model / 100 * opportunity_snapshot_base.opp_net_arr AS influenced_net_arr

  FROM attribution_touchpoint_snapshot_base
  LEFT JOIN opportunity_snapshot_base
    ON attribution_touchpoint_snapshot_base.dim_crm_opportunity_id = opportunity_snapshot_base.dim_crm_opportunity_id
      AND attribution_touchpoint_snapshot_base.touchpoint_snapshot_date = opportunity_snapshot_base.opportunity_snapshot_date
  LEFT JOIN campaigns
    ON attribution_touchpoint_snapshot_base.dim_campaign_id = campaigns.dim_campaign_id


),

aggregated_opportunity_influenced_performance AS (
  SELECT
    combined_models.dim_campaign_id,
    combined_models.dim_crm_account_id,
    combined_models.dim_crm_opportunity_id,
    combined_models.true_event_date,
    combined_models.event_snapshot_type,
    combined_models.opportunity_snapshot_date,
    SUM(CASE WHEN is_net_arr_pipeline_created = 1 THEN influenced_net_arr END) AS influenced_pipeline
  FROM
    combined_models
  {{dbt_utils.group_by(n=6)}}

),

final AS (


  SELECT 
  --IDs
    account_summary.dim_crm_account_id,
    mart_crm_account.dim_parent_crm_account_id,
    account_summary.dim_campaign_id,
    campaigns.dim_parent_campaign_id,
    opportunity_campaign_snapshot_base.dim_crm_opportunity_id,
  --DATES
    account_summary.true_event_date,
    snapshot_dates.event_snapshot_type,
    snapshot_dates.date_day                 AS snapshot_date,
    dim_date.fiscal_quarter_name_fy         AS event_fiscal_quarter_fy,
    dim_date.first_day_of_month             AS event_date_month,
    dim_date.day_of_month                   AS event_day_of_month,
    dim_date.month_name                     AS event_month_name,
    dim_date.month_of_fiscal_year           AS event_month_of_fiscal_year,
    dim_date.fiscal_month_name_fy           AS event_fiscal_month_name_fy,
    dim_date.day_of_fiscal_quarter          AS event_day_of_fiscal_quarter,
    dim_date.day_of_fiscal_year             AS event_day_of_fiscal_year,
    dim_date.fiscal_quarters_ago            AS event_fiscal_quarters_ago,
    dim_date.current_day_of_month,
    dim_date.current_day_of_fiscal_quarter,
    dim_date.current_day_of_fiscal_year,
    dim_date.current_fiscal_quarter_name_fy,
    opportunity_campaign_snapshot_base.pipeline_created_date,
  --ACCOUNT FIELDS 
    opportunity_campaign_snapshot_base.account_has_attended_flag,
    mart_crm_account.abm_tier,
    mart_crm_account.crm_account_owner_id,
    mart_crm_account.crm_account_owner,
    mart_crm_account.owner_role,
    mart_crm_account.crm_account_name,
    mart_crm_account.crm_account_focus_account,
    mart_crm_account.parent_crm_account_geo,
    mart_crm_account.parent_crm_account_region,
    mart_crm_account.parent_crm_account_sales_segment,
    mart_crm_account.parent_crm_account_area,
    mart_crm_account.crm_account_owner_user_segment,
    mart_crm_account.six_sense_account_profile_fit,
    mart_crm_account.six_sense_account_reach_score,
    mart_crm_account.six_sense_account_profile_score,
    mart_crm_account.six_sense_account_buying_stage,
    mart_crm_account.six_sense_account_numerical_reach_score,
    mart_crm_account.six_sense_account_update_date,
    mart_crm_account.six_sense_account_6_qa_start_date,
    mart_crm_account.six_sense_account_6_qa_end_date,
    mart_crm_account.six_sense_account_6_qa_age_days,
    mart_crm_account.six_sense_account_intent_score,
    mart_crm_account.six_sense_segments,
    mart_crm_account.pte_score_group,
    mart_crm_account.is_sdr_target_account,
    mart_crm_account.is_first_order_available,
    mart_crm_account.crm_account_type,
    mart_crm_account.crm_account_industry,
    mart_crm_account.crm_account_sub_industry,
  --CAMPAIGN FIELDS
    campaigns.campaign_name,
    campaigns.campaign_status,
    campaigns.campaign_description,
    campaigns.budget_holder,
    campaigns.allocadia_id,
    campaigns.is_a_channel_partner_involved,
    campaigns.is_an_alliance_partner_involved,
    campaigns.is_this_an_in_person_event,
    campaigns.alliance_partner_name,
    campaigns.channel_partner_name,
    campaigns.total_planned_mqls,
    campaigns.will_there_be_mdf_funding,
    campaigns.campaign_partner_crm_id,
    campaigns.campaign_type,
    campaigns.campaign_owner_id,
    campaigns.campaign_owner_name,
    campaigns.campaign_owner_manager_name,
    campaigns.campaign_created_by_id,
    campaigns.campaign_start_date,
    campaigns.campaign_end_date,
    campaigns.campaign_created_date,
    campaigns.campaign_region,
    campaigns.campaign_sub_region,
    campaigns.campaign_budgeted_cost,
    campaigns.campaign_actual_cost,
    campaigns.future_event_flag,
    campaigns.series_campaign_id,
    campaigns.series_campaign_name,
  --Opportunity dimensions
    opportunity_snapshot_base.snapshot_stage_name,
    opportunity_snapshot_base.live_stage_name,
    opportunity_snapshot_base.opportunity_stage_progression,
    opportunity_snapshot_base.opportunity_category,
    opportunity_snapshot_base.subscription_type,
    opportunity_snapshot_base.order_type,
    opportunity_snapshot_base.sales_qualified_source_name,
    opportunity_snapshot_base.opportunity_name,
    opportunity_snapshot_base.product_category,
    opportunity_snapshot_base.product_details,
    opportunity_snapshot_base.parent_crm_account_geo_pubsec_segment,
    opportunity_snapshot_base.report_geo,
    opportunity_snapshot_base.report_region,
    opportunity_snapshot_base.report_area,
    opportunity_snapshot_base.report_segment,
    opportunity_snapshot_base.pipe_council_grouping,
    opportunity_snapshot_base.report_geo_pubsec_segment,
    opportunity_snapshot_base.report_role_level_1,
    opportunity_snapshot_base.report_role_level_2,
    opportunity_snapshot_base.report_role_level_3,
    --Opportunity Flags
    opportunity_snapshot_base.is_sao,
    opportunity_snapshot_base.is_won,
    opportunity_snapshot_base.is_web_portal_purchase,
    opportunity_snapshot_base.is_edu_oss,
    opportunity_snapshot_base.is_open,
    opportunity_snapshot_base.is_lost,
    opportunity_snapshot_base.is_closed,
    opportunity_snapshot_base.is_renewal,
    opportunity_snapshot_base.is_refund,
    opportunity_snapshot_base.is_credit_flag,
    opportunity_snapshot_base.is_net_arr_pipeline_created,
    opportunity_snapshot_base.is_booked_net_arr_flag,
    opportunity_snapshot_base.is_eligible_age_analysis_flag,
    opportunity_snapshot_base.snapshot_is_eligible_open_pipeline,
    opportunity_snapshot_base.snapshot_is_net_arr_pipeline_created,
    opportunity_snapshot_base.snapshot_is_booked_net_arr,
    opportunity_campaign_snapshot_base.open_pipeline_at_event_date_flag,
    opportunity_campaign_snapshot_base.open_pipeline_current_date_flag,
    opportunity_campaign_snapshot_base.sourced_pipeline_post_event_flag,
  --ACCOUNT LEVEL METRICS
    account_summary.open_pipeline_live,
    account_summary.registered_leads,
    account_summary.attended_leads,
  --Pipeline/Opp Metrics
    opportunity_campaign_snapshot_base.sourced_pipeline_post_event,
    opportunity_campaign_snapshot_base.sourced_opps_post_event,
    opportunity_campaign_snapshot_base.open_pipeline,
    opportunity_campaign_snapshot_base.open_pipeline_opps,
    opportunity_campaign_snapshot_base.snapshot_pipeline_created,
    opportunity_campaign_snapshot_base.snapshot_pipeline_created_opps,
    opportunity_snapshot_base.opp_net_arr,
    aggregated_opportunity_influenced_performance.influenced_pipeline

  --
    FROM account_summary

    LEFT JOIN snapshot_dates 
    ON account_summary.dim_campaign_id = snapshot_dates.dim_campaign_id 

    LEFT JOIN 
    opportunity_campaign_snapshot_base
    ON account_summary.dim_campaign_id = opportunity_campaign_snapshot_base.dim_campaign_id
    AND account_summary.dim_crm_account_id = opportunity_campaign_snapshot_base.dim_crm_account_id
    AND snapshot_dates.date_day = opportunity_campaign_snapshot_base.snapshot_date
    AND snapshot_dates.event_snapshot_type = opportunity_campaign_snapshot_base.event_snapshot_type 


    LEFT JOIN 
    campaigns
    ON account_summary.dim_campaign_id = campaigns.dim_campaign_id 

    LEFT JOIN opportunity_snapshot_base 
    ON opportunity_campaign_snapshot_base.dim_crm_opportunity_id = opportunity_snapshot_base.dim_crm_opportunity_id
    AND opportunity_campaign_snapshot_base.snapshot_date = opportunity_snapshot_base.opportunity_snapshot_date

    LEFT JOIN aggregated_opportunity_influenced_performance 
    ON opportunity_campaign_snapshot_base.dim_crm_opportunity_id = aggregated_opportunity_influenced_performance.dim_crm_opportunity_id
    AND opportunity_campaign_snapshot_base.dim_campaign_id = aggregated_opportunity_influenced_performance.dim_campaign_id
    AND opportunity_campaign_snapshot_base.true_event_date = aggregated_opportunity_influenced_performance.true_event_date 
    AND opportunity_campaign_snapshot_base.snapshot_date = aggregated_opportunity_influenced_performance.opportunity_snapshot_date  

    LEFT JOIN mart_crm_account
    ON opportunity_campaign_snapshot_base.dim_crm_account_id = mart_crm_account.dim_crm_account_id

    LEFT JOIN dim_date
    ON account_summary.true_event_date = dim_date.date_day
)


SELECT *
FROM final