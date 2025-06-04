{{ config(
    tags=["mnpi_exception"]
) }}

{{ simple_cte([
    ('dim_crm_touchpoint','dim_crm_touchpoint'),
    ('fct_crm_touchpoint','fct_crm_touchpoint'),
    ('dim_campaign','dim_campaign'),
    ('fct_campaign','fct_campaign'),
    ('dim_crm_person','dim_crm_person'),
    ('fct_crm_person', 'fct_crm_person'),
    ('dim_crm_account','dim_crm_account'),
    ('dim_crm_user','dim_crm_user')
]) }}

, joined AS (

    SELECT
      -- touchpoint info
      dim_crm_touchpoint.dim_crm_touchpoint_id,
      {{ dbt_utils.generate_surrogate_key(['fct_crm_touchpoint.dim_crm_person_id','dim_campaign.dim_campaign_id','dim_crm_touchpoint.bizible_touchpoint_date_time']) }} AS touchpoint_person_campaign_date_id,
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
      fct_crm_touchpoint.bizible_count_first_touch,
      fct_crm_touchpoint.bizible_count_lead_creation_touch,
      fct_crm_touchpoint.bizible_count_u_shaped,
      dim_crm_touchpoint.bizible_created_date,
      dim_crm_touchpoint.devrel_campaign_type,
      dim_crm_touchpoint.devrel_campaign_description,
      dim_crm_touchpoint.devrel_campaign_influence_type,
      dim_crm_touchpoint.keystone_content_name,
      dim_crm_touchpoint.keystone_gitlab_epic,
      dim_crm_touchpoint.keystone_gtm,
      dim_crm_touchpoint.keystone_url_slug,
      dim_crm_touchpoint.keystone_type,

      -- person info
      fct_crm_touchpoint.dim_crm_person_id,
      dim_crm_person.sfdc_record_id,
      dim_crm_person.sfdc_record_type,
      dim_crm_person.marketo_lead_id,
      dim_crm_person.email_hash,
      dim_crm_person.email_domain,
      dim_crm_person.owner_id,
      dim_crm_person.person_score,
      dim_crm_person.title                                                 AS crm_person_title,
      dim_crm_person.country                                               AS crm_person_country,
      dim_crm_person.state                                                 AS crm_person_state,
      dim_crm_person.status                                                AS crm_person_status,
      dim_crm_person.lead_source,
      dim_crm_person.lead_source_type,
      dim_crm_person.source_buckets,
      dim_crm_person.net_new_source_categories,
      dim_crm_person.crm_partner_id,
      fct_crm_person.created_date                                          AS crm_person_created_date,
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
      fct_crm_person.true_inquiry_date,
      dim_crm_person.account_demographics_sales_segment,
      dim_crm_person.account_demographics_geo,
      dim_crm_person.account_demographics_region,
      dim_crm_person.account_demographics_area,
      dim_crm_person.is_partner_recalled,

      -- campaign info
      dim_campaign.dim_campaign_id,
      dim_campaign.campaign_name,
      dim_campaign.is_active                                               AS campagin_is_active,
      dim_campaign.status                                                  AS campaign_status,
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
      fct_campaign.created_by_id                                           AS campaign_created_by_id,
      fct_campaign.start_date                                              AS camapaign_start_date,
      fct_campaign.end_date                                                AS campaign_end_date,
      fct_campaign.created_date                                            AS campaign_created_date,
      fct_campaign.last_modified_date                                      AS campaign_last_modified_date,
      fct_campaign.last_activity_date                                      AS campaign_last_activity_date,
      fct_campaign.region                                                  AS campaign_region,
      fct_campaign.sub_region                                              AS campaign_sub_region,
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

      -- sales rep info
      dim_crm_user.user_name                               AS rep_name,
      dim_crm_user.title                                   AS rep_title,
      dim_crm_user.team,
      dim_crm_user.is_active                               AS rep_is_active,
      dim_crm_user.user_role_name,
      dim_crm_user.crm_user_sales_segment                  AS touchpoint_crm_user_segment_name_live,
      dim_crm_user.crm_user_geo                            AS touchpoint_crm_user_geo_name_live,
      dim_crm_user.crm_user_region                         AS touchpoint_crm_user_region_name_live,
      dim_crm_user.crm_user_area                           AS touchpoint_crm_user_area_name_live,
      dim_crm_user.sdr_sales_segment,
      dim_crm_user.sdr_region,

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
      dim_crm_account.parent_crm_account_territory,
      dim_crm_account.parent_crm_account_region,
      dim_crm_account.parent_crm_account_area,
      dim_crm_account.crm_account_owner_user_segment,
      dim_crm_account.record_type_id,
      dim_crm_account.gitlab_com_user,
      dim_crm_account.crm_account_type,
      dim_crm_account.technical_account_manager,
      dim_crm_account.merged_to_account_id,
      dim_crm_account.is_reseller,
      dim_crm_account.is_focus_partner,

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
    }},

    -- counts
     CASE
        WHEN dim_crm_touchpoint.bizible_touchpoint_position LIKE '%LC%' 
          AND dim_crm_touchpoint.bizible_touchpoint_position NOT LIKE '%PostLC%' 
          THEN 1
        ELSE 0
      END AS count_inquiry,
      CASE
        WHEN fct_crm_person.true_inquiry_date >= dim_crm_touchpoint.bizible_touchpoint_date 
          THEN 1
        ELSE 0
      END AS count_true_inquiry,
      CASE
        WHEN fct_crm_person.mql_date_first >= dim_crm_touchpoint.bizible_touchpoint_date 
          THEN 1
        ELSE 0
      END AS count_mql,
      CASE
        WHEN fct_crm_person.mql_date_first >= dim_crm_touchpoint.bizible_touchpoint_date 
          THEN fct_crm_touchpoint.bizible_count_lead_creation_touch
        ELSE 0
      END AS count_net_new_mql,
      CASE
        WHEN fct_crm_person.accepted_date >= dim_crm_touchpoint.bizible_touchpoint_date 
          THEN 1
        ELSE '0'
      END AS count_accepted,
      CASE
        WHEN fct_crm_person.accepted_date >= dim_crm_touchpoint.bizible_touchpoint_date 
          THEN fct_crm_touchpoint.bizible_count_lead_creation_touch
        ELSE 0
      END AS count_net_new_accepted,

      CASE 
        WHEN count_mql=1 THEN dim_crm_person.sfdc_record_id
        ELSE NULL
      END AS mql_crm_person_id

    FROM fct_crm_touchpoint
    LEFT JOIN dim_crm_touchpoint
      ON fct_crm_touchpoint.dim_crm_touchpoint_id = dim_crm_touchpoint.dim_crm_touchpoint_id
    LEFT JOIN dim_campaign
      ON fct_crm_touchpoint.dim_campaign_id = dim_campaign.dim_campaign_id
    LEFT JOIN fct_campaign
      ON fct_crm_touchpoint.dim_campaign_id = fct_campaign.dim_campaign_id
    LEFT JOIN dim_crm_person
      ON fct_crm_touchpoint.dim_crm_person_id = dim_crm_person.dim_crm_person_id
    LEFT JOIN fct_crm_person
      ON fct_crm_touchpoint.dim_crm_person_id = fct_crm_person.dim_crm_person_id
    LEFT JOIN dim_crm_account
      ON fct_crm_touchpoint.dim_crm_account_id = dim_crm_account.dim_crm_account_id
    LEFT JOIN dim_crm_user AS campaign_owner
      ON fct_campaign.campaign_owner_id = campaign_owner.dim_crm_user_id
    LEFT JOIN dim_crm_user
      ON fct_crm_touchpoint.dim_crm_user_id = dim_crm_user.dim_crm_user_id

), count_of_pre_mql_tps AS (

    SELECT DISTINCT
      joined.email_hash,
      COUNT(DISTINCT joined.dim_crm_touchpoint_id) AS pre_mql_touches
    FROM joined
    WHERE joined.mql_date_first IS NOT NULL
      AND joined.bizible_touchpoint_date <= joined.mql_date_first
    GROUP BY 1

), pre_mql_tps_by_person AS (

    SELECT
      count_of_pre_mql_tps.email_hash,
      count_of_pre_mql_tps.pre_mql_touches,
      1/count_of_pre_mql_tps.pre_mql_touches AS pre_mql_weight
    FROM count_of_pre_mql_tps
    GROUP BY 1,2

), pre_mql_tps AS (

    SELECT
      joined.dim_crm_touchpoint_id,
      pre_mql_tps_by_person.pre_mql_weight
    FROM pre_mql_tps_by_person
    LEFT JOIN joined 
      ON pre_mql_tps_by_person.email_hash=joined.email_hash
    WHERE joined.mql_date_first IS NOT NULL
      AND joined.bizible_touchpoint_date <= joined.mql_date_first

), post_mql_tps AS (

    SELECT
      joined.dim_crm_touchpoint_id,
      0 AS pre_mql_weight
    FROM joined
    WHERE joined.bizible_touchpoint_date > joined.mql_date_first
      OR joined.mql_date_first IS null

), mql_weighted_tps AS (

    SELECT *
    FROM pre_mql_tps

    UNION ALL

    SELECT *
    FROM post_mql_tps

),final AS (

  SELECT 
    joined.*,
    mql_weighted_tps.pre_mql_weight
  FROM joined
  LEFT JOIN mql_weighted_tps 
    ON joined.dim_crm_touchpoint_id=mql_weighted_tps.dim_crm_touchpoint_id
  WHERE joined.dim_crm_touchpoint_id IS NOT NULL

)

SELECT *
FROM final
