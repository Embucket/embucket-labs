{{
  config(
    tags=['six_hourly']
  )
}}

WITH source AS (
  SELECT *
  FROM {{ ref('prep_crm_opportunity_calcs') }}

),

live_opp AS (

  /*
    Pull the "live" values for certain fields
    Context: https://gitlab.com/gitlab-data/analytics/-/issues/18976
  */
  SELECT
    dim_crm_opportunity_id,
    dim_crm_account_id,
    dim_crm_user_id,
    sales_qualified_source              AS sales_qualified_source_live,
    sales_qualified_source_grouped      AS sales_qualified_source_grouped_live,
    is_edu_oss                          AS is_edu_oss_live,
    opportunity_category                AS opportunity_category_live,
    deal_path                           AS deal_path_live,
    order_type_grouped                  AS order_type_grouped_live,
    order_type                          AS order_type_live,
    close_date                          AS close_date_live,
    crm_opp_owner_sales_segment_stamped AS crm_opp_owner_sales_segment_stamped_live,
    crm_opp_owner_geo_stamped           AS crm_opp_owner_geo_stamped_live,
    crm_opp_owner_region_stamped        AS crm_opp_owner_region_stamped_live,
    crm_opp_owner_area_stamped          AS crm_opp_owner_area_stamped_live,
    crm_opp_owner_business_unit_stamped AS crm_opp_owner_business_unit_stamped_live,
    CASE
      WHEN DATE_PART('month', close_date) < 2
        THEN DATE_PART('year', close_date)
      ELSE (DATE_PART('year', close_date) + 1)
    END                                 AS close_fiscal_year_live
  FROM {{ ref('prep_crm_opportunity_calcs') }}
  /*
    Since hard deletes are invalidated in the snapshot data,
    deleted opportunities are automatically excluded in this step
    because they do not have a dbt_valid_to value set to NULL.
  */
  WHERE dbt_valid_to IS NULL

),

live_account AS (

  SELECT
    dim_crm_account_id,
    dim_crm_user_id,
    parent_crm_account_geo AS parent_crm_account_geo_live,
    is_jihu_account        AS is_jihu_account_live,
    crm_account_owner_role AS opportunity_account_owner_role_live
  FROM {{ ref('prep_crm_account') }}
  WHERE dim_crm_account_id IS NOT NULL

),

live_user AS (

  SELECT
    dim_crm_user_id,
    dim_crm_user_hierarchy_sk AS dim_crm_user_hierarchy_account_user_sk_live,
    user_role_name            AS opportunity_owner_role_live
  FROM {{ ref('prep_crm_user') }}


),

final AS (

  -- Join live values with snapshot data 
  SELECT
    source.*,

    --live opportunity fields
    live_opp.sales_qualified_source_live,
    live_opp.sales_qualified_source_grouped_live,
    live_opp.is_edu_oss_live,
    live_opp.opportunity_category_live,
    live_opp.deal_path_live,
    live_opp.order_type_grouped_live,
    live_opp.order_type_live,
    live_opp.close_date_live,
    live_opp.crm_opp_owner_sales_segment_stamped_live,
    live_opp.crm_opp_owner_geo_stamped_live,
    live_opp.crm_opp_owner_region_stamped_live,
    live_opp.crm_opp_owner_area_stamped_live,
    live_opp.crm_opp_owner_business_unit_stamped_live,
    live_opp.close_fiscal_year_live,

    --live account fields
    live_account.parent_crm_account_geo_live,
    live_account.is_jihu_account_live,
    live_account.opportunity_account_owner_role_live,

    --live user fields
    account_owner.dim_crm_user_hierarchy_account_user_sk_live,
    opportunity_owner.opportunity_owner_role_live
  FROM source
  LEFT JOIN live_opp
    ON source.dim_crm_opportunity_id = live_opp.dim_crm_opportunity_id
  LEFT JOIN live_account
    ON live_opp.dim_crm_account_id = live_account.dim_crm_account_id
  LEFT JOIN live_user AS opportunity_owner
    ON live_opp.dim_crm_user_id = opportunity_owner.dim_crm_user_id
  LEFT JOIN live_user AS account_owner
    ON live_account.dim_crm_user_id = account_owner.dim_crm_user_id

)

SELECT *
FROM final
