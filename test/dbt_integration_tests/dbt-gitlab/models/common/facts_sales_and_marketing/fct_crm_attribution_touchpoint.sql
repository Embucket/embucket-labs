{{ config(
    tags=["mnpi_exception"] 
) }}

WITH bizible_attribution_touchpoints AS (

    SELECT *
    FROM {{ ref('prep_crm_attribution_touchpoint') }}

), crm_person AS (

    SELECT *
    FROM {{ ref('prep_crm_person') }}

), opportunity_dimensions AS (

    SELECT *
    FROM {{ ref('map_crm_opportunity') }}

), prep_crm_opportunity AS (

    SELECT *
    FROM {{ ref('prep_crm_opportunity') }}

), sheetload_gitlab_data_driven_attribution_weights_source AS (

  SELECT *
  FROM {{ ref('sheetload_gitlab_data_driven_attribution_weights_source') }}

), data_driven_weight_base AS (

  SELECT
        bizible_attribution_touchpoints.touchpoint_id AS dim_crm_touchpoint_id,
        bizible_attribution_touchpoints.opportunity_id AS dim_crm_opportunity_id,
        bizible_attribution_touchpoints.bizible_marketing_channel,
        bizible_attribution_touchpoints.touchpoint_offer_type,
        bizible_attribution_touchpoints.bizible_touchpoint_date AS bizible_touchpoint_datetime,
        prep_crm_opportunity.order_type,
        prep_crm_opportunity.arr_created_date,
        DATEDIFF(DAY,bizible_touchpoint_datetime,prep_crm_opportunity.arr_created_date) AS touchpoint_to_arr_days,
        IFNULL(sheetload_gitlab_data_driven_attribution_weights_source.weight,0) AS data_driven_model_weight
    FROM bizible_attribution_touchpoints
    LEFT JOIN prep_crm_opportunity
        ON bizible_attribution_touchpoints.opportunity_id=prep_crm_opportunity.dim_crm_opportunity_id
    LEFT JOIN sheetload_gitlab_data_driven_attribution_weights_source
        ON bizible_attribution_touchpoints.touchpoint_offer_type = sheetload_gitlab_data_driven_attribution_weights_source.offer_type
    WHERE prep_crm_opportunity.is_live = 1 
      AND touchpoint_to_arr_days BETWEEN 0 AND 365

), data_driven_weight_final AS (

  SELECT
      data_driven_weight_base.*,
      SUM(data_driven_model_weight) OVER (PARTITION BY dim_crm_opportunity_id) AS total_opp_data_driven_weight,
      DIV0(data_driven_model_weight,total_opp_data_driven_weight) AS normalised_data_driven_weight,
      MIN(bizible_touchpoint_datetime) OVER (PARTITION BY dim_crm_opportunity_id) AS first_touchpoint_datetime,
      MAX(bizible_touchpoint_datetime) OVER (PARTITION BY dim_crm_opportunity_id) AS last_touchpoint_datetime,
      DATEDIFF(DAY,first_touchpoint_datetime,bizible_touchpoint_datetime) AS touchpoint_days_since_first,
      DATEDIFF(DAY,bizible_touchpoint_datetime, last_touchpoint_datetime) AS touchpoint_days_from_last,
      COUNT(DISTINCT bizible_marketing_channel) OVER (PARTITION BY dim_crm_opportunity_id) AS opportunity_channel_count
  FROM data_driven_weight_base

), data_driven_weight_calculation_base AS (

  SELECT 
    dim_crm_touchpoint_id,
    dim_crm_opportunity_id,
    bizible_marketing_channel,
    touchpoint_offer_type,
    bizible_touchpoint_datetime,
    arr_created_date,
    first_touchpoint_datetime,
    last_touchpoint_datetime,
    touchpoint_days_since_first,
    touchpoint_days_from_last,
    total_opp_data_driven_weight,
    normalised_data_driven_weight,
    (1- LEAST(1, touchpoint_days_from_last/180)) AS last_touch_weight,
    (1- LEAST(1, touchpoint_days_since_first/180)) AS first_touch_weight,
    SUM(last_touch_weight) OVER (PARTITION BY dim_crm_opportunity_id) AS total_opp_last_touch_weight,
    DIV0NULL(last_touch_weight,total_opp_last_touch_weight) AS normalized_last_touch_weight,
    SUM(first_touch_weight) OVER (PARTITION BY dim_crm_opportunity_id) AS total_opp_first_touch_weight, 
    DIV0NULL(first_touch_weight,total_opp_first_touch_weight) AS normalized_first_touch_weight,
    0.67*normalized_last_touch_weight + 0.33*normalized_first_touch_weight AS time_decay_model_weight,
    DIV0NULL(0.4*time_decay_model_weight + 0.6*normalised_data_driven_weight,1) AS gitlab_model_weight
  FROM data_driven_weight_final

), data_driven_weight_calculation_final AS (

  SELECT 
    dim_crm_touchpoint_id,
    dim_crm_opportunity_id,
    bizible_marketing_channel,
    touchpoint_offer_type,
    SUM(gitlab_model_weight) OVER (PARTITION BY dim_crm_opportunity_id) AS total_opp_gitlab_model_weight,
    DIV0NULL(gitlab_model_weight,total_opp_gitlab_model_weight) AS gitlab_model_weight,
    time_decay_model_weight,
    normalised_data_driven_weight AS data_driven_model_weight
  FROM data_driven_weight_calculation_base

), mgp_base AS (

    SELECT DISTINCT
      bizible_attribution_touchpoints.opportunity_id AS dim_crm_opportunity_id,
      prep_crm_opportunity.order_type,
      prep_crm_opportunity.sales_qualified_source,
      prep_crm_opportunity.is_renewal,
      CASE
          WHEN prep_crm_opportunity.order_type IN ('1. First Order','1. New - First Order') AND COUNT(DISTINCT bizible_attribution_touchpoints.bizible_marketing_channel)>=2
            THEN TRUE
          WHEN prep_crm_opportunity.order_type IN ('2. New - Connected','3. Growth') AND COUNT(DISTINCT bizible_attribution_touchpoints.bizible_marketing_channel)>=3
            THEN TRUE
          ELSE FALSE
      END AS is_mgp_channel_based
      FROM bizible_attribution_touchpoints
      LEFT JOIN prep_crm_opportunity
        ON bizible_attribution_touchpoints.opportunity_id=prep_crm_opportunity.dim_crm_opportunity_id
      WHERE bizible_touchpoint_date >= (prep_crm_opportunity.arr_created_date - INTERVAL '365 Days') 
        AND prep_crm_opportunity.is_live = 1 
      {{dbt_utils.group_by(n=4)}}

), campaigns_per_opp AS (

    SELECT
      opportunity_id,
      COUNT(DISTINCT campaign_id) AS campaigns_per_opp
      FROM bizible_attribution_touchpoints
    GROUP BY 1

), opps_per_touchpoint AS (

    SELECT 
      touchpoint_id,
      COUNT(DISTINCT opportunity_id) AS opps_per_touchpoint
    FROM bizible_attribution_touchpoints
    GROUP BY 1

), final_attribution_touchpoint AS (

    SELECT
      bizible_attribution_touchpoints.touchpoint_id AS dim_crm_touchpoint_id,

      -- shared dimension keys
      bizible_attribution_touchpoints.campaign_id AS dim_campaign_id,
      bizible_attribution_touchpoints.opportunity_id AS dim_crm_opportunity_id,
      bizible_attribution_touchpoints.bizible_account AS dim_crm_account_id,
      crm_person.dim_crm_person_id,
      opportunity_dimensions.dim_crm_user_id,
      opportunity_dimensions.dim_order_type_id,
      opportunity_dimensions.dim_sales_qualified_source_id,
      opportunity_dimensions.dim_deal_path_id,
      opportunity_dimensions.dim_parent_crm_account_id,
      opportunity_dimensions.dim_parent_sales_segment_id,
      opportunity_dimensions.dim_parent_sales_territory_id,
      opportunity_dimensions.dim_parent_industry_id,
      opportunity_dimensions.dim_account_sales_segment_id,
      opportunity_dimensions.dim_account_sales_territory_id,
      opportunity_dimensions.dim_account_industry_id,
      opportunity_dimensions.dim_account_location_country_id,
      opportunity_dimensions.dim_account_location_region_id,

      -- counts
      opps_per_touchpoint.opps_per_touchpoint,
      campaigns_per_opp.campaigns_per_opp,
      bizible_attribution_touchpoints.bizible_count_first_touch,
      bizible_attribution_touchpoints.bizible_count_lead_creation_touch,
      bizible_attribution_touchpoints.bizible_attribution_percent_full_path,
      bizible_attribution_touchpoints.bizible_count_u_shaped,
      bizible_attribution_touchpoints.bizible_count_w_shaped,
      bizible_attribution_touchpoints.bizible_count_custom_model,
	
	-- touchpoint weight info
      bizible_attribution_touchpoints.bizible_weight_full_path,
      bizible_attribution_touchpoints.bizible_weight_custom_model,
      bizible_attribution_touchpoints.bizible_weight_first_touch,
      bizible_attribution_touchpoints.bizible_weight_lead_conversion,
      bizible_attribution_touchpoints.bizible_weight_u_shaped,
      bizible_attribution_touchpoints.bizible_weight_w_shaped,
      IFNULL(data_driven_weight_calculation_final.gitlab_model_weight,0)      AS gitlab_model_weight,
      IFNULL(data_driven_weight_calculation_final.time_decay_model_weight,0)  AS time_decay_model_weight,
      IFNULL(data_driven_weight_calculation_final.data_driven_model_weight,0) AS data_driven_model_weight,
     
	  -- touchpoint revenue info
      bizible_attribution_touchpoints.bizible_revenue_full_path,
      bizible_attribution_touchpoints.bizible_revenue_custom_model,
      bizible_attribution_touchpoints.bizible_revenue_first_touch,
      bizible_attribution_touchpoints.bizible_revenue_lead_conversion,
      bizible_attribution_touchpoints.bizible_revenue_u_shaped,
      bizible_attribution_touchpoints.bizible_revenue_w_shaped,
      bizible_attribution_touchpoints.bizible_created_date,
      mgp_base.is_renewal,
      mgp_base.sales_qualified_source,
      mgp_base.is_mgp_channel_based
    FROM bizible_attribution_touchpoints
    LEFT JOIN crm_person
      ON bizible_attribution_touchpoints.bizible_contact = crm_person.sfdc_record_id
    LEFT JOIN opportunity_dimensions
      ON bizible_attribution_touchpoints.opportunity_id = opportunity_dimensions.dim_crm_opportunity_id
    LEFT JOIN  campaigns_per_opp 
      ON bizible_attribution_touchpoints.opportunity_id =  campaigns_per_opp.opportunity_id
    LEFT JOIN opps_per_touchpoint
      ON bizible_attribution_touchpoints.touchpoint_id = opps_per_touchpoint.touchpoint_id
    LEFT JOIN mgp_base
      ON bizible_attribution_touchpoints.opportunity_id=mgp_base.dim_crm_opportunity_id
    LEFT JOIN data_driven_weight_calculation_final
      ON bizible_attribution_touchpoints.touchpoint_id = data_driven_weight_calculation_final.dim_crm_touchpoint_id

), mgp_weight_sum AS (

  SELECT
    dim_crm_opportunity_id,
    ROUND(
      SUM(gitlab_model_weight),0
    ) AS mgp_weight_sum
  FROM final_attribution_touchpoint
  GROUP BY 1

), mgp_opportunity_final AS (

  SELECT
    final_attribution_touchpoint.dim_crm_opportunity_id,
    CASE
      WHEN (final_attribution_touchpoint.is_mgp_channel_based = TRUE
        OR final_attribution_touchpoint.sales_qualified_source = 'SDR Generated')
        AND final_attribution_touchpoint.is_renewal = 0
        AND mgp_weight_sum.mgp_weight_sum = 1
        THEN TRUE
        ELSE FALSE
    END AS is_mgp_opportunity
  FROM final_attribution_touchpoint
  LEFT JOIN mgp_weight_sum
    ON final_attribution_touchpoint.dim_crm_opportunity_id=mgp_weight_sum.dim_crm_opportunity_id

), final AS (

  SELECT DISTINCT
    final_attribution_touchpoint.*,
    mgp_opportunity_final.is_mgp_opportunity
  FROM final_attribution_touchpoint
  LEFT JOIN mgp_opportunity_final
    ON final_attribution_touchpoint.dim_crm_opportunity_id=mgp_opportunity_final.dim_crm_opportunity_id

)

SELECT *
FROM final
