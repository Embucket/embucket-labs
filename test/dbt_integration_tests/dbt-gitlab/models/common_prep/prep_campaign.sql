WITH sfdc_campaign_info AS (

    SELECT *
    FROM {{ ref('sfdc_campaign_source') }}
    WHERE NOT is_deleted

), intermediate AS (

    SELECT
      -- campaign ids
      campaign_id                                   AS dim_campaign_id,
      campaign_parent_id                            AS dim_parent_campaign_id,

      -- campaign details
      campaign_name,
      is_active,
      status,
      type,
      description,
      budget_holder,
      bizible_touchpoint_enabled_setting,
      strategic_marketing_contribution,
      region,
      CASE
        WHEN sub_region = 'Central' AND region = 'EMEA'
            THEN 'Central Europe'
        WHEN sub_region = 'Northern' AND region = 'EMEA'
            THEN 'Northern Europe'
        WHEN sub_region = 'Southern' AND region = 'EMEA'
            THEN 'Southern Europe'
        ELSE sub_region
      END AS sub_region,
      large_bucket,
      reporting_type,
      allocadia_id,
      is_a_channel_partner_involved,
      is_an_alliance_partner_involved,
      is_this_an_in_person_event,
      alliance_partner_name,
      channel_partner_name,
      sales_play,
      gtm_motion,
      total_planned_mqls,
      will_there_be_mdf_funding,
      mdf_request_id,
      campaign_partner_crm_id,

      -- user ids
      campaign_owner_id,
      created_by_id,
      last_modified_by_id,

      -- dates
      start_date,
      end_date,
      created_date,
      last_modified_date,
      last_activity_date,

      --planned values
      planned_inquiry,
      planned_mql,
      planned_pipeline,
      planned_sao,
      planned_won,
      planned_roi,
      total_planned_mql,

      -- additive fields
      budgeted_cost,
      expected_response,
      expected_revenue,
      actual_cost,
      amount_all_opportunities,
      amount_won_opportunities,
      count_contacts,
      count_converted_leads,
      count_leads,
      count_opportunities,
      count_responses,
      count_won_opportunities,
      count_sent,
      registration_goal,
      attendance_goal,

    FROM sfdc_campaign_info

), campaign_parent AS (

    SELECT
        intermediate.dim_campaign_id,
        intermediate.campaign_name,
        parent_campaign.dim_campaign_id AS dim_parent_campaign_id,
        parent_campaign.campaign_name AS parent_campaign_name        
    FROM intermediate
    LEFT JOIN intermediate AS parent_campaign
        ON intermediate.dim_parent_campaign_id=parent_campaign.dim_campaign_id

), series_final AS (

    SELECT
        campaign_parent.dim_campaign_id,
        campaign_parent.campaign_name,
        campaign_parent.dim_parent_campaign_id,
        campaign_parent.parent_campaign_name,
        CASE
            WHEN series.dim_parent_campaign_id IS NULL AND LOWER(series.campaign_name) LIKE '%series%'
                THEN series.dim_campaign_id
            WHEN  LOWER(series.parent_campaign_name) LIKE '%series%'
                THEN series.dim_parent_campaign_id   
            ELSE NULL     
        END AS series_campaign_id,
        CASE
            WHEN series.parent_campaign_name IS NULL AND LOWER(series.campaign_name) LIKE '%series%'
                THEN series.campaign_name
            WHEN LOWER(series.parent_campaign_name) LIKE '%series%'
                THEN series.parent_campaign_name  
            ELSE NULL       
        END AS series_campaign_name
    FROM campaign_parent
    LEFT JOIN campaign_parent AS series
        ON campaign_parent.dim_parent_campaign_id = series.dim_campaign_id

), final AS (

    SELECT
        intermediate.*,
        series_campaign_id,
        series_campaign_name
    FROM intermediate
    LEFT JOIN series_final
        ON intermediate.dim_campaign_id=series_final.dim_campaign_id

)

SELECT *
FROM final
