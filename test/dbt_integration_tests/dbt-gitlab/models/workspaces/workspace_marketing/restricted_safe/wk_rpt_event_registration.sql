{{ config(
    materialized='table'
) }}

{{ simple_cte([
    ('fct_campaign','fct_campaign'),
    ('dim_campaign','dim_campaign'),
    ('dim_crm_user','dim_crm_user'),
    ('dim_date', 'dim_date'),
    ('mart_crm_person','mart_crm_person'),
    ('sfdc_campaign_member','sfdc_campaign_member'),
    ('mart_crm_account','mart_crm_account'),
    ('sfdc_campaign_member', 'sfdc_campaign_member'),
    ('wk_marketo_activity_fill_out_form', 'wk_marketo_activity_fill_out_form'),
    ('wk_marketo_activity_add_to_sfdc_campaign', 'wk_marketo_activity_add_to_sfdc_campaign')
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
    ON true_event_date = campaign_start.date_day

  WHERE true_event_date >= '2023-02-01'
  AND campaign_type in ('Owned Event', 'Workshop', 'Executive Roundtables', 'Webcast', 'Sponsored Webcast', 'Conference', 'Speaking Session', 'Virtual Sponsorship', 'Self-Service Virtual Event', 'Vendor Arranged Meetings')
  AND campaign_status in ('In Progress', 'Planned', 'Completed')
), campaign_members_with_campaigns AS (
  SELECT

    -- id's
    mart_crm_person.dim_crm_person_id,
    sfdc_campaign_member.campaign_id                           AS dim_campaign_id,
    sfdc_campaign_member.campaign_member_id,
    campaigns.dim_parent_campaign_id,
    mart_crm_person.marketo_lead_id,
    campaigns.allocadia_id,

    -- campaigns
    campaigns.campaign_name,
    campaigns.campaign_status,
    campaigns.campaign_description,
    campaigns.budget_holder,

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
    campaigns.campaign_end_date,
    campaigns.campaign_created_date,
    
    campaigns.campaign_region,
    campaigns.campaign_sub_region,
    
    campaigns.campaign_budgeted_cost,
    campaigns.campaign_actual_cost,
    campaigns.campaign_expected_response,
    campaigns.campaign_expected_revenue,
    campaigns.amount_all_opportunities,
    campaigns.amount_won_opportunities,
    campaigns.count_contacts,
    campaigns.count_converted_leads,
    campaigns.count_leads,
    campaigns.count_opportunities,
    campaigns.count_responses,
    campaigns.count_won_opportunities,
    campaigns.campaign_fiscal_quarter_name_fy,
    campaigns.campaign_fiscal_year,


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

    -- person
    mart_crm_person.dim_crm_user_id                            AS person_owner_id,
    mart_crm_person.dim_crm_account_id                         AS person_account_id,
    mart_crm_person.sfdc_record_id                             AS person_sfdc_record_id,
    mart_crm_person.mql_date_latest,
    mart_crm_person.created_date                               AS person_created_date,
    mart_crm_person.inquiry_date,
    mart_crm_person.accepted_date                              AS person_accepted_date,
    mart_crm_person.email_domain_type,
    mart_crm_person.status                                     AS person_status,
    mart_crm_person.lead_source,
    mart_crm_person.crm_partner_id                             AS person_crm_person_id,
    mart_crm_person.is_partner_recalled                        AS person_is_partner_recalled,
    mart_crm_person.prospect_share_status                      AS person_prospect_share_status,
    mart_crm_person.partner_prospect_status                    AS person_partner_prospect_status,
    mart_crm_person.partner_prospect_owner_name                AS person_partner_prospect_owner_name,
    mart_crm_person.partner_prospect_id                        AS person_partner_prospect_id,
    mart_crm_person.is_mql,
    mart_crm_person.is_inquiry,
    mart_crm_person.is_first_order_person                      AS person_is_first_order,
    mart_crm_person.account_demographics_sales_segment         AS person_account_demographics_sales_segment,
    mart_crm_person.account_demographics_sales_segment_grouped AS person_account_demographics_sales_segment_grouped,
    mart_crm_person.account_demographics_geo                   AS person_account_demographics_geo,
    mart_crm_person.account_demographics_region                AS person_account_demographics_region,
    mart_crm_person.account_demographics_area                  AS person_account_demographics_area,

    -- partner from the campaign
    campaign_partner_account.crm_account_name                  AS campaign_partner_crm_account_name,
    campaign_partner_account.parent_crm_account_name           AS campaign_partner_parent_crm_account_name,
    campaign_partner_account.crm_account_owner                 AS campaign_partner_crm_account_owner

  FROM sfdc_campaign_member
  INNER JOIN campaigns ON sfdc_campaign_member.campaign_id = campaigns.dim_campaign_id
  LEFT JOIN mart_crm_person
    ON sfdc_campaign_member.lead_or_contact_id = mart_crm_person.sfdc_record_id
  -- partner from campaigns
  LEFT JOIN mart_crm_account AS campaign_partner_account
    ON campaigns.campaign_partner_crm_id = campaign_partner_account.dim_crm_account_id

), marketo_query_params as (
    /*

     */
    SELECT
        MARKETO_ACTIVITY_FILL_OUT_FORM_ID,
        max(case when split_part(value, '=', 1) = 'utm_medium' then split_part(value, '=', 2) end)    as utm_medium,
        max(case when split_part(value, '=', 1) = 'utm_campaign' then split_part(value, '=', 2) end)  as utm_campaign,
        max(case when split_part(value, '=', 1) = 'utm_source' then split_part(value, '=', 2) end)    as utm_source,
        max(case when split_part(value, '=', 1) = 'utm_content' then split_part(value, '=', 2) end)   as utm_content,
        max(case when split_part(value, '=', 1) = 'utm_partnerid' then split_part(value, '=', 2) end) as utm_partnerid
    FROM wk_marketo_activity_fill_out_form,
    TABLE(SPLIT_TO_TABLE(query_parameters, '&')) 
    where query_parameters is not null
    group by 1

), marketo_form_fills as (

    SELECT
        wk_marketo_activity_fill_out_form.lead_id AS marketo_lead_id,
        wk_marketo_activity_fill_out_form.activity_date AS form_submit_date,
        wk_marketo_activity_add_to_sfdc_campaign.activity_date AS campaign_sync_date,
        wk_marketo_activity_fill_out_form.campaign_id AS marketo_form_campaign_id,
        wk_marketo_activity_fill_out_form.primary_attribute_value AS marketo_form_name,
        wk_marketo_activity_fill_out_form.webpage_id,
        wk_marketo_activity_fill_out_form.referrer_url,
        wk_marketo_activity_add_to_sfdc_campaign.primary_attribute_value AS sfdc_campaign_name,
        marketo_query_params.utm_medium,
        marketo_query_params.utm_campaign,
        marketo_query_params.utm_source,
        marketo_query_params.utm_content,
        marketo_query_params.utm_partnerid
    FROM wk_marketo_activity_fill_out_form
    LEFT JOIN wk_marketo_activity_add_to_sfdc_campaign
        ON wk_marketo_activity_fill_out_form.lead_id = wk_marketo_activity_add_to_sfdc_campaign.lead_id
            AND wk_marketo_activity_fill_out_form.activity_date::DATE = wk_marketo_activity_add_to_sfdc_campaign.activity_date::DATE
    LEFT JOIN marketo_query_params
        on wk_marketo_activity_fill_out_form.marketo_activity_fill_out_form_id = marketo_query_params.marketo_activity_fill_out_form_id


), final as (
    /*
        One row per campaign, per campaign memeber, per opp from Bizible
        All campaigns must be shown
        All campaign members must be shown
        Touchpoints are left join
        Key ends up being - (campaign_member_id, touchpoint ID, Opp ID)
     */
    select
    
    -- id's
    campaign_members_with_campaigns.dim_campaign_id,
    campaign_members_with_campaigns.campaign_member_id,
    campaign_members_with_campaigns.dim_crm_person_id,
    -- attribution_touchpoints.dim_crm_touchpoint_id      as attribution_dim_crm_touchpoint_id,
    -- attribution_touchpoints.dim_crm_opportunity_id,
    campaign_members_with_campaigns.marketo_lead_id,

    -- Campaign
    campaign_members_with_campaigns.dim_parent_campaign_id,
    campaign_members_with_campaigns.campaign_name,
    campaign_members_with_campaigns.campaign_status,
    campaign_members_with_campaigns.campaign_description,
    campaign_members_with_campaigns.budget_holder,
    campaign_members_with_campaigns.allocadia_id,
    campaign_members_with_campaigns.campaign_type,

    -- Campaign dates
    campaign_members_with_campaigns.campaign_start_date,
    campaign_members_with_campaigns.campaign_end_date,
    campaign_members_with_campaigns.campaign_created_date,
    campaign_members_with_campaigns.campaign_fiscal_quarter_name_fy,
    campaign_members_with_campaigns.campaign_fiscal_year,
    true_event_date,
    
    -- Partner from the campaign
    campaign_members_with_campaigns.campaign_partner_crm_account_name,
    campaign_members_with_campaigns.campaign_partner_parent_crm_account_name,
    campaign_members_with_campaigns.campaign_partner_crm_account_owner,
    campaign_members_with_campaigns.is_a_channel_partner_involved,
    campaign_members_with_campaigns.is_an_alliance_partner_involved,
    campaign_members_with_campaigns.is_this_an_in_person_event,
    campaign_members_with_campaigns.alliance_partner_name,
    campaign_members_with_campaigns.channel_partner_name,
    campaign_members_with_campaigns.will_there_be_mdf_funding,
    campaign_members_with_campaigns.campaign_partner_crm_id,
    
    campaign_members_with_campaigns.campaign_owner_id,
    campaign_members_with_campaigns.campaign_owner_name,
    campaign_members_with_campaigns.campaign_owner_manager_name,
    campaign_members_with_campaigns.campaign_created_by_id,

    campaign_members_with_campaigns.campaign_region,
    campaign_members_with_campaigns.campaign_sub_region,

    -- Campaign Metrics
    campaign_members_with_campaigns.total_planned_mqls,
    campaign_members_with_campaigns.campaign_budgeted_cost,
    campaign_members_with_campaigns.campaign_actual_cost,
    campaign_members_with_campaigns.campaign_expected_response,
    campaign_members_with_campaigns.campaign_expected_revenue,
    campaign_members_with_campaigns.count_responses,
    
    -- Member Info
    campaign_members_with_campaigns.campaign_member_has_responded,
    campaign_members_with_campaigns.campaign_member_response_date,
    campaign_members_with_campaigns.campaign_member_status,
    campaign_members_with_campaigns.campaign_member_created_date,
    campaign_members_with_campaigns.utm_campaign AS campaign_member_utm_campaign,
    campaign_members_with_campaigns.utm_medium AS campaign_member_utm_medium,
    campaign_members_with_campaigns.utm_source AS campaign_member_utm_source,
    campaign_members_with_campaigns.last_form_submission_page AS campaign_member_last_form_submission_page,

    -- Marketo Info
    marketo_form_fills.form_submit_date,
    marketo_form_fills.referrer_url,
    marketo_form_fills.utm_medium,
    marketo_form_fills.utm_campaign,
    marketo_form_fills.utm_source,
    marketo_form_fills.utm_content,
    marketo_form_fills.utm_partnerid,

    -- person
    campaign_members_with_campaigns.person_owner_id,
    campaign_members_with_campaigns.person_account_id,
    campaign_members_with_campaigns.person_sfdc_record_id,
    campaign_members_with_campaigns.mql_date_latest,
    campaign_members_with_campaigns.person_created_date,
    campaign_members_with_campaigns.inquiry_date,
    campaign_members_with_campaigns.person_accepted_date,
    campaign_members_with_campaigns.email_domain_type,
    campaign_members_with_campaigns.person_status,
    campaign_members_with_campaigns.lead_source,
    campaign_members_with_campaigns.person_is_partner_recalled,
    campaign_members_with_campaigns.person_prospect_share_status,
    campaign_members_with_campaigns.person_partner_prospect_status,
    campaign_members_with_campaigns.person_partner_prospect_owner_name,
    campaign_members_with_campaigns.person_partner_prospect_id,
    campaign_members_with_campaigns.is_mql,
    campaign_members_with_campaigns.is_inquiry,
    campaign_members_with_campaigns.person_is_first_order,
    campaign_members_with_campaigns.person_account_demographics_sales_segment,
    campaign_members_with_campaigns.person_account_demographics_sales_segment_grouped,
    campaign_members_with_campaigns.person_account_demographics_geo,
    campaign_members_with_campaigns.person_account_demographics_region,
    campaign_members_with_campaigns.person_account_demographics_area

    from
    campaign_members_with_campaigns 
        left join marketo_form_fills
            on campaign_members_with_campaigns.campaign_name = marketo_form_fills.sfdc_campaign_name
                AND campaign_members_with_campaigns.marketo_lead_id = marketo_form_fills.marketo_lead_id

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@degan",
    updated_by="@rkohnke",
    created_date="2024-05-09",
    updated_date="2024-07-02",
) }}