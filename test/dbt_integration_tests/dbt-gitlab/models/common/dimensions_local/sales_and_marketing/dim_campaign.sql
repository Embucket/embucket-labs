WITH sfdc_campaign_info AS (

    SELECT *
    FROM {{ ref('prep_campaign') }}

), final AS (

    SELECT
      dim_campaign_id,
      campaign_name,
      is_active,
      status,
      type,
      description,
      budget_holder,
      bizible_touchpoint_enabled_setting,
      strategic_marketing_contribution,
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
      registration_goal,
      attendance_goal,
      will_there_be_mdf_funding,
      mdf_request_id,
      campaign_partner_crm_id,
      series_campaign_id,
      series_campaign_name
    FROM sfdc_campaign_info

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@paul_armstrong",
    updated_by="@rkohnke",
    created_date="2020-1-13",
    updated_date="2024-10-24"
) }}
