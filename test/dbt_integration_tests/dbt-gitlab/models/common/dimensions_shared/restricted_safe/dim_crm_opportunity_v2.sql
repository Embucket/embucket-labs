{{ config(
    tags=["six_hourly"]
) }}


WITH prep_crm_opportunity AS (

    SELECT *
    FROM {{ref('prep_crm_opportunity')}}
    WHERE is_live = TRUE

), distinct_values AS (

  SELECT DISTINCT

    -- Primary Key
    dim_crm_opportunity_id,  

    -- General Information
    opportunity_name,
    forecast_category_name,
    user_segment,
    order_type,
    order_type_current,
    opportunity_category,
    tam_notes,
    payment_schedule,
    opportunity_term,
    growth_type,
    deployment_preference,
    sqs_bucket_engagement,
    record_type_name,
    qsr_notes,
    qsr_status,
    startup_type,
    next_steps,
    competitors,
    solutions_to_be_replaced,
    professional_services_value,
    edu_services_value,
    investment_services_value,
    primary_solution_architect,

    -- Product info, to be distinguished from line items
    product_category,
    product_details,
    products_purchased,
    intended_product_tier,

    -- Risk, loss and downgrades
    probability,
    opportunity_health,
    risk_type,
    risk_reasons,
    manager_confidence,
    reason_for_loss,
    reason_for_loss_details,
    reason_for_loss_staged,
    reason_for_loss_calc,
    downgrade_reason,
    downgrade_details,
    churn_contraction_type,
    churn_contraction_net_arr_bucket,

    -- Renewal fields
    auto_renewal_status,
    renewal_risk_category,
    renewal_swing_arr,
    renewal_manager, 
    renewal_forecast_health,

    -- user hierarchy (TBD if we should instead get this straigh from dim_crm_user_hierarchy)
    parent_crm_account_geo,
    crm_account_owner_sales_segment,
    crm_account_owner_geo,
    crm_account_owner_region,
    crm_account_owner_area,
    crm_account_owner_sales_segment_geo_region_area,

    crm_account_owner_role,
    crm_account_owner_role_level_1,
    crm_account_owner_role_level_2,
    crm_account_owner_role_level_3,
    crm_account_owner_role_level_4,
    crm_account_owner_role_level_5,
    crm_account_owner_title,

    opportunity_owner_user_segment,
    opportunity_owner_role,
    opportunity_owner_manager,
    opportunity_owner_department,
    opportunity_owner_title,
    opportunity_account_owner_role,   
    
    crm_opp_owner_role_level_1,
    crm_opp_owner_role_level_2,
    crm_opp_owner_role_level_3,
    crm_opp_owner_role_level_4,
    crm_opp_owner_role_level_5,
 

    -- Opportunity Owner Stamped fields at date of Sales Accepted Opportunity
    crm_opp_owner_stamped_name,
    crm_account_owner_stamped_name,
    sao_crm_opp_owner_sales_segment_stamped,
    sao_crm_opp_owner_sales_segment_stamped_grouped,
    sao_crm_opp_owner_geo_stamped,
    sao_crm_opp_owner_region_stamped,
    sao_crm_opp_owner_area_stamped,
    sao_crm_opp_owner_segment_region_stamped_grouped,
    sao_crm_opp_owner_sales_segment_geo_region_area_stamped,
    crm_opp_owner_user_role_type_stamped,
    crm_opp_owner_sales_segment_geo_region_area_stamped

    -- VSA fields
    vsa_readout,
    vsa_start_date,
    vsa_url,
    vsa_status,
    vsa_end_date

  FROM prep_crm_opportunity

)
SELECT *
FROM distinct_values
