{{ config(
    tags=["mnpi_exception"] 
) }}

WITH campaign_details AS (

    SELECT *
    FROM {{ ref('prep_campaign') }}

), bizible_touchpoints AS (

    SELECT *
    FROM {{ ref('prep_crm_touchpoint') }}

), prep_bizible_touchpoint_keystone AS (

    SELECT *
    FROM {{ ref('prep_bizible_touchpoint_keystone') }}

), bizible_attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('prep_crm_attribution_touchpoint') }}

), bizible_touchpoints_with_campaign AS (

    SELECT
      bizible_touchpoints.*,
      campaign_details.dim_campaign_id,
      campaign_details.dim_parent_campaign_id
    FROM bizible_touchpoints
    LEFT JOIN campaign_details
      ON bizible_touchpoints.campaign_id = campaign_details.dim_campaign_id

), bizible_attribution_touchpoints_with_campaign AS (

    SELECT
      bizible_attribution_touchpoints.*,
      campaign_details.dim_campaign_id,
      campaign_details.dim_parent_campaign_id
    FROM bizible_attribution_touchpoints
    LEFT JOIN campaign_details
      ON bizible_attribution_touchpoints.campaign_id = campaign_details.dim_campaign_id

), bizible_campaign_grouping AS (

    SELECT *
    FROM {{ ref('map_bizible_campaign_grouping') }}

), devrel_influence_campaigns AS (

    SELECT *
    FROM {{ ref('sheetload_devrel_influenced_campaigns_source') }}

), combined_touchpoints AS (

    SELECT
      --ids
      touchpoint_id                 AS dim_crm_touchpoint_id,
      -- touchpoint info
      bizible_touchpoint_date::DATE AS bizible_touchpoint_date,
      bizible_touchpoint_date AS bizible_touchpoint_date_time,
      DATE_TRUNC('month', bizible_touchpoint_date) AS bizible_touchpoint_month,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_source_type,
      bizible_touchpoint_type,
      touchpoint_offer_type,
      touchpoint_offer_type_grouped,
      bizible_ad_campaign_name,
      bizible_ad_content,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_landing_page,
      bizible_landing_page_raw,

    --UTMs not captured by the Bizible - Landing Page
      PARSE_URL(null)['parameters']['utm_campaign']::VARCHAR  AS bizible_landing_page_utm_campaign,
      PARSE_URL(null)['parameters']['utm_medium']::VARCHAR    AS bizible_landing_page_utm_medium,
      PARSE_URL(null)['parameters']['utm_source']::VARCHAR    AS bizible_landing_page_utm_source,
      PARSE_URL(null)['parameters']['utm_content']::VARCHAR   AS bizible_landing_page_utm_content,
      PARSE_URL(null)['parameters']['utm_budget']::VARCHAR    AS bizible_landing_page_utm_budget,
      PARSE_URL(null)['parameters']['utm_allptnr']::VARCHAR   AS bizible_landing_page_utm_allptnr,
      PARSE_URL(null)['parameters']['utm_partnerid']::VARCHAR AS bizible_landing_page_utm_partnerid,
    --UTMs not captured by the Bizible - Form Page
      PARSE_URL(null)['parameters']['utm_campaign']::VARCHAR     AS bizible_form_page_utm_campaign,
      PARSE_URL(null)['parameters']['utm_medium']::VARCHAR       AS bizible_form_page_utm_medium,
      PARSE_URL(null)['parameters']['utm_source']::VARCHAR       AS bizible_form_page_utm_source,
      PARSE_URL(null)['parameters']['utm_content']::VARCHAR       AS bizible_form_page_utm_content,
      PARSE_URL(null)['parameters']['utm_budget']::VARCHAR        AS bizible_form_page_utm_budget,
      PARSE_URL(null)['parameters']['utm_allptnr']::VARCHAR       AS bizible_form_page_utm_allptnr,
      PARSE_URL(null)['parameters']['utm_partnerid']::VARCHAR     AS bizible_form_page_utm_partnerid,

    --Final UTM Parameters
      COALESCE(bizible_landing_page_utm_campaign, bizible_form_page_utm_campaign)   AS utm_campaign,
      COALESCE(bizible_landing_page_utm_medium, bizible_form_page_utm_medium)       AS utm_medium,
      COALESCE(bizible_landing_page_utm_source, bizible_form_page_utm_source)       AS utm_source,
      COALESCE(bizible_landing_page_utm_budget, bizible_form_page_utm_budget)       AS utm_budget,
      COALESCE(bizible_landing_page_utm_content, bizible_form_page_utm_content)     AS utm_content,
      COALESCE(bizible_landing_page_utm_allptnr, bizible_form_page_utm_allptnr)     AS utm_allptnr,
      COALESCE(bizible_landing_page_utm_partnerid, bizible_form_page_utm_partnerid) AS utm_partnerid,
      
      -- new utm parsing
    {{ utm_campaign_parsing('utm_campaign') }}
    {{ utm_content_parsing('utm_content') }}
      bizible_marketing_channel,
      bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_salesforce_campaign,
      '0'                           AS is_attribution_touchpoint,
      dim_campaign_id,
      dim_parent_campaign_id,
      bizible_created_date

    FROM bizible_touchpoints_with_campaign

    UNION ALL

    SELECT
      --ids
      touchpoint_id                 AS dim_crm_touchpoint_id,
      -- touchpoint info
      bizible_touchpoint_date::DATE AS bizible_touchpoint_date,
      bizible_touchpoint_date AS bizible_touchpoint_date_time,
      DATE_TRUNC('month', bizible_touchpoint_date) AS bizible_touchpoint_month,
      bizible_touchpoint_position,
      bizible_touchpoint_source,
      bizible_touchpoint_source_type,
      bizible_touchpoint_type,
      touchpoint_offer_type,
      touchpoint_offer_type_grouped,
      bizible_ad_campaign_name,
      bizible_ad_content,
      bizible_ad_group_name,
      bizible_form_url,
      bizible_form_url_raw,
      bizible_landing_page,
      bizible_landing_page_raw,

    --UTMs not captured by the Bizible - Landing Page
      PARSE_URL(null)['parameters']['utm_campaign']::VARCHAR  AS bizible_landing_page_utm_campaign,
      PARSE_URL(null)['parameters']['utm_medium']::VARCHAR    AS bizible_landing_page_utm_medium,
      PARSE_URL(null)['parameters']['utm_source']::VARCHAR    AS bizible_landing_page_utm_source,
      PARSE_URL(null)['parameters']['utm_content']::VARCHAR   AS bizible_landing_page_utm_content,
      PARSE_URL(null)['parameters']['utm_budget']::VARCHAR    AS bizible_landing_page_utm_budget,
      PARSE_URL(null)['parameters']['utm_allptnr']::VARCHAR   AS bizible_landing_page_utm_allptnr,
      PARSE_URL(null)['parameters']['utm_partnerid']::VARCHAR AS bizible_landing_page_utm_partnerid,
    --UTMs not captured by the Bizible - Form Page
      PARSE_URL(null)['parameters']['utm_campaign']::VARCHAR     AS bizible_form_page_utm_campaign,
      PARSE_URL(null)['parameters']['utm_medium']::VARCHAR       AS bizible_form_page_utm_medium,
      PARSE_URL(null)['parameters']['utm_source']::VARCHAR       AS bizible_form_page_utm_source,
      PARSE_URL(null)['parameters']['utm_content']::VARCHAR       AS bizible_form_page_utm_content,
      PARSE_URL(null)['parameters']['utm_budget']::VARCHAR        AS bizible_form_page_utm_budget,
      PARSE_URL(null)['parameters']['utm_allptnr']::VARCHAR       AS bizible_form_page_utm_allptnr,
      PARSE_URL(null)['parameters']['utm_partnerid']::VARCHAR     AS bizible_form_page_utm_partnerid,

    --Final UTM Parameters
      COALESCE(bizible_landing_page_utm_campaign, bizible_form_page_utm_campaign)   AS utm_campaign,
      COALESCE(bizible_landing_page_utm_medium, bizible_form_page_utm_medium)       AS utm_medium,
      COALESCE(bizible_landing_page_utm_source, bizible_form_page_utm_source)       AS utm_source,
      COALESCE(bizible_landing_page_utm_budget, bizible_form_page_utm_budget)       AS utm_budget,
      COALESCE(bizible_landing_page_utm_content, bizible_form_page_utm_content)     AS utm_content,
      COALESCE(bizible_landing_page_utm_allptnr, bizible_form_page_utm_allptnr)     AS utm_allptnr,
      COALESCE(bizible_landing_page_utm_partnerid, bizible_form_page_utm_partnerid) AS utm_partnerid,

    -- new utm parsing
    {{ utm_campaign_parsing('utm_campaign') }}
    {{ utm_content_parsing('utm_content') }}

      bizible_marketing_channel,
      CASE
        WHEN dim_parent_campaign_id = '7014M000001dn8MQAQ' THEN 'Paid Social.LinkedIn Lead Gen'
        WHEN bizible_ad_campaign_name = '20201013_ActualTechMedia_DeepMonitoringCI' THEN 'Sponsorship'
        ELSE bizible_marketing_channel_path
      END AS bizible_marketing_channel_path,
      bizible_medium,
      bizible_referrer_page,
      bizible_referrer_page_raw,
      bizible_salesforce_campaign,
      '1'                           AS is_attribution_touchpoint,
      dim_campaign_id,
      dim_parent_campaign_id,
      bizible_created_date

    FROM bizible_attribution_touchpoints_with_campaign

), final AS (

    SELECT
      combined_touchpoints.dim_crm_touchpoint_id,
      combined_touchpoints.bizible_touchpoint_date,
      combined_touchpoints.bizible_touchpoint_date_time,
      combined_touchpoints.bizible_touchpoint_month,
      combined_touchpoints.bizible_touchpoint_position,
      combined_touchpoints.bizible_touchpoint_source,
      combined_touchpoints.bizible_touchpoint_source_type,
      combined_touchpoints.bizible_touchpoint_type,
      combined_touchpoints.touchpoint_offer_type,
      combined_touchpoints.touchpoint_offer_type_grouped,
      combined_touchpoints.bizible_ad_campaign_name,
      combined_touchpoints.bizible_ad_content,
      combined_touchpoints.bizible_ad_group_name,
      combined_touchpoints.bizible_form_url,
      combined_touchpoints.bizible_form_url_raw,
      combined_touchpoints.bizible_landing_page,
      combined_touchpoints.bizible_landing_page_raw,
      combined_touchpoints.bizible_form_page_utm_content,
      combined_touchpoints.bizible_form_page_utm_budget,
      combined_touchpoints.bizible_form_page_utm_allptnr,
      combined_touchpoints.bizible_form_page_utm_partnerid,
      combined_touchpoints.bizible_landing_page_utm_content,
      combined_touchpoints.bizible_landing_page_utm_budget,
      combined_touchpoints.bizible_landing_page_utm_allptnr,
      combined_touchpoints.bizible_landing_page_utm_partnerid,
      combined_touchpoints.utm_campaign,
      combined_touchpoints.utm_medium,
      combined_touchpoints.utm_source,
      combined_touchpoints.utm_content,
      combined_touchpoints.utm_budget,
      combined_touchpoints.utm_allptnr,
      combined_touchpoints.utm_partnerid,
      combined_touchpoints.utm_campaign_date,
      combined_touchpoints.utm_campaign_region,
      combined_touchpoints.utm_campaign_budget,
      combined_touchpoints.utm_campaign_type,
      combined_touchpoints.utm_campaign_gtm,
      combined_touchpoints.utm_campaign_language,
      combined_touchpoints.utm_campaign_name,
      combined_touchpoints.utm_campaign_agency,
      combined_touchpoints.utm_content_offer,
      combined_touchpoints.utm_content_asset_type,
      combined_touchpoints.utm_content_industry,
      combined_touchpoints.bizible_marketing_channel,
      combined_touchpoints.bizible_marketing_channel_path,
      CASE
        WHEN combined_touchpoints.bizible_marketing_channel IN ('Paid Search', 'Content', 'Paid Social') 
          THEN 'Paid Demand Gen'
        WHEN combined_touchpoints.bizible_marketing_channel IN ('Event') 
          THEN 'Events'
        WHEN combined_touchpoints.bizible_marketing_channel IN ('Email') 
          THEN 'Email Nurture'
        WHEN combined_touchpoints.bizible_marketing_channel IN ('Other','Direct', 'Organic Search', 'Digital', 'Web Referral', 'Social') 
          THEN 'Inbound Demand'
        WHEN combined_touchpoints.bizible_marketing_channel IN ('SDR Generated') 
          THEN 'Outbound Demand'
        WHEN combined_touchpoints.bizible_marketing_channel IN ('Referral') 
          THEN 'Partner'
        ELSE CONCAT('not-grouped - ',combined_touchpoints.bizible_marketing_channel)
      END AS marketing_review_channel_grouping,
      combined_touchpoints.bizible_medium,
      combined_touchpoints.bizible_referrer_page,
      combined_touchpoints.bizible_referrer_page_raw,
      combined_touchpoints.bizible_salesforce_campaign,
      combined_touchpoints.is_attribution_touchpoint,
      bizible_campaign_grouping.integrated_campaign_grouping,
      bizible_campaign_grouping.bizible_integrated_campaign_grouping,
      bizible_campaign_grouping.gtm_motion,
      bizible_campaign_grouping.touchpoint_segment,
      CASE
        WHEN combined_touchpoints.dim_crm_touchpoint_id ILIKE 'a6061000000CeS0%' -- Specific touchpoint overrides
          THEN 'Field Event'
        WHEN combined_touchpoints.bizible_marketing_channel_path = 'CPC.AdWords'
          THEN 'Google AdWords'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Email.Other', 'Email.Newsletter','Email.Outreach')
          THEN 'Email'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Field Event','Partners.Google','Brand.Corporate Event','Conference','Speaking Session')
                  OR (combined_touchpoints.bizible_medium = 'Field Event (old)' AND combined_touchpoints.bizible_marketing_channel_path = 'Other')
          THEN 'Field Event'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Paid Social.Facebook','Paid Social.LinkedIn','Paid Social.Twitter','Paid Social.YouTube')
          THEN 'Paid Social'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Social.Facebook','Social.LinkedIn','Social.Twitter','Social.YouTube')
          THEN 'Social'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Marketing Site.Web Referral','Web Referral')
          THEN 'Web Referral'
        WHEN combined_touchpoints.bizible_marketing_channel_path in ('Marketing Site.Web Direct', 'Web Direct')
              -- Added to Web Direct
              OR combined_touchpoints.dim_campaign_id in (
                                '701610000008ciRAAQ', -- Trial - GitLab.com
                                '70161000000VwZbAAK', -- Trial - Self-Managed
                                '70161000000VwZgAAK', -- Trial - SaaS
                                '70161000000CnSLAA0', -- 20181218_DevOpsVirtual
                                '701610000008cDYAAY'  -- 2018_MovingToGitLab
                                )
          THEN 'Web Direct'
        WHEN combined_touchpoints.bizible_marketing_channel_path LIKE 'Organic Search.%'
              OR combined_touchpoints.bizible_marketing_channel_path = 'Marketing Site.Organic'
          THEN 'Organic Search'
        WHEN combined_touchpoints.bizible_marketing_channel_path IN ('Sponsorship')
          THEN 'Paid Sponsorship'
        ELSE 'Unknown'
      END AS pipe_name,
      CASE
        WHEN touchpoint_segment = 'Demand Gen' THEN 1
        ELSE 0
      END AS is_dg_influenced,
      CASE
        WHEN combined_touchpoints.bizible_touchpoint_position LIKE '%FT%' 
          AND is_dg_influenced = 1
          THEN 1
        ELSE 0
      END AS is_dg_sourced,
      combined_touchpoints.bizible_created_date,
      CASE 
        WHEN devrel_influence_campaigns.campaign_name IS NOT NULL 
          THEN TRUE 
          ELSE FALSE 
      END AS is_devrel_influenced_campaign,
      devrel_influence_campaigns.campaign_type      AS devrel_campaign_type,
      devrel_influence_campaigns.description        AS devrel_campaign_description,
      devrel_influence_campaigns.influence_type     AS devrel_campaign_influence_type,
      prep_bizible_touchpoint_keystone.content_name AS keystone_content_name,
      prep_bizible_touchpoint_keystone.gitlab_epic  AS keystone_gitlab_epic,
      prep_bizible_touchpoint_keystone.gtm          AS keystone_gtm,
      prep_bizible_touchpoint_keystone.url_slug     AS keystone_url_slug,
      prep_bizible_touchpoint_keystone.type         AS keystone_type
    FROM combined_touchpoints
    LEFT JOIN bizible_campaign_grouping
      ON combined_touchpoints.dim_crm_touchpoint_id = bizible_campaign_grouping.dim_crm_touchpoint_id
    LEFT JOIN devrel_influence_campaigns
      ON combined_touchpoints.bizible_ad_campaign_name = devrel_influence_campaigns.campaign_name
    LEFT JOIN prep_bizible_touchpoint_keystone
      ON combined_touchpoints.dim_crm_touchpoint_id=prep_bizible_touchpoint_keystone.dim_crm_touchpoint_id
    WHERE combined_touchpoints.dim_crm_touchpoint_id IS NOT NULL
)

{{ dbt_audit(
    cte_ref="final",
    created_by="@mcooperDD",
    updated_by="@rkohnke",
    created_date="2021-01-21",
    updated_date="2024-12-19" 
) }}
