WITH sfdc_campaigns AS (

    SELECT *
    FROM {{ ref('prep_campaign') }}

), final_campaigns AS (

    SELECT

      -- campaign ids
      dim_campaign_id,
      dim_parent_campaign_id,

      -- user ids
      campaign_owner_id,
      created_by_id,
      last_modified_by_id,

      -- dates
      start_date,
      {{ get_date_id('start_date') }}              AS start_date_id,
      end_date,
      {{ get_date_id('end_date') }}                AS end_date_id,
      created_date,
      {{ get_date_id('created_date') }}            AS created_date_id,
      last_modified_date,
      {{ get_date_id('last_modified_date') }}      AS last_modified_date_id,
      last_activity_date,
      {{ get_date_id('last_activity_date') }}      AS last_activity_date_id,

      region,
      sub_region,

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
      count_sent

    FROM sfdc_campaigns

)

SELECT *
FROM final_campaigns
