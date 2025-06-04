{{ config(
    tags=["six_hourly"]
) }}

{{ simple_cte([
    ('fct_crm_opportunity', 'fct_crm_opportunity'),
    ('fct_crm_person', 'fct_crm_person'),
    ('prep_date', 'prep_date'),
    ('prep_order_type', 'prep_order_type')
]) }}

, first_order_key AS (

  SELECT dim_order_type_id
  FROM prep_order_type
  WHERE order_type_name = '1. New - First Order'

), net_arr AS (

  SELECT
    close_date_id                               AS actual_date_id,
    close_date::DATE                            AS actual_date,
    'Net ARR'                                   AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}               AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    dim_crm_current_account_set_role_name_id      AS dim_role_name_id,
    dim_crm_current_account_set_role_level_1_id   AS dim_role_level_1_id,
    dim_crm_current_account_set_role_level_2_id   AS dim_role_level_2_id,
    dim_crm_current_account_set_role_level_3_id   AS dim_role_level_3_id,
    dim_crm_current_account_set_role_level_4_id   AS dim_role_level_4_id,
    dim_crm_current_account_set_role_level_5_id   AS dim_role_level_5_id,

    NULL                                        AS email_hash,
    new_logo_count,
    net_arr

  FROM fct_crm_opportunity
  WHERE fpa_master_bookings_flag = TRUE

), closed_deals AS (

  SELECT
    close_date_id                               AS actual_date_id,
    close_date::DATE                            AS actual_date,
    'Deals'                                     AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}               AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    dim_crm_current_account_set_role_name_id      AS dim_role_name_id,
    dim_crm_current_account_set_role_level_1_id   AS dim_role_level_1_id,
    dim_crm_current_account_set_role_level_2_id   AS dim_role_level_2_id,
    dim_crm_current_account_set_role_level_3_id   AS dim_role_level_3_id,
    dim_crm_current_account_set_role_level_4_id   AS dim_role_level_4_id,
    dim_crm_current_account_set_role_level_5_id   AS dim_role_level_5_id,

    NULL AS email_hash,
    new_logo_count,
    net_arr

  FROM fct_crm_opportunity
  WHERE fpa_master_bookings_flag = TRUE

), new_logos AS (

  SELECT
    close_date_id                               AS actual_date_id,
    close_date::DATE                            AS actual_date,
    'New Logos'                                 AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}               AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    dim_crm_current_account_set_role_name_id      AS dim_role_name_id,
    dim_crm_current_account_set_role_level_1_id   AS dim_role_level_1_id,
    dim_crm_current_account_set_role_level_2_id   AS dim_role_level_2_id,
    dim_crm_current_account_set_role_level_3_id   AS dim_role_level_3_id,
    dim_crm_current_account_set_role_level_4_id   AS dim_role_level_4_id,
    dim_crm_current_account_set_role_level_5_id   AS dim_role_level_5_id,

    NULL AS email_hash,
    new_logo_count,
    net_arr

  FROM fct_crm_opportunity
  WHERE is_new_logo_first_order = TRUE

), saos AS (

  SELECT
    sales_accepted_date_id                      AS actual_date_id,
    sales_accepted_date::DATE                   AS actual_date,
    'Stage 1 Opportunities'                     AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}               AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    dim_crm_current_account_set_role_name_id      AS dim_role_name_id,
    dim_crm_current_account_set_role_level_1_id   AS dim_role_level_1_id,
    dim_crm_current_account_set_role_level_2_id   AS dim_role_level_2_id,
    dim_crm_current_account_set_role_level_3_id   AS dim_role_level_3_id,
    dim_crm_current_account_set_role_level_4_id   AS dim_role_level_4_id,
    dim_crm_current_account_set_role_level_5_id   AS dim_role_level_5_id,

    NULL AS email_hash,
    new_logo_count,
    net_arr
  FROM fct_crm_opportunity
  WHERE is_sao = TRUE

), pipeline_created AS (

  SELECT
    arr_created_date_id                        AS actual_date_id,
    arr_created_date::DATE                     AS actual_date,
    'Net ARR Pipeline Created'                 AS sales_funnel_kpi_name,

    dim_crm_opportunity_id,
    dim_crm_account_id,
    {{ get_keyed_nulls('NULL') }}              AS dim_crm_person_id,

    dim_order_type_id,
    dim_sales_qualified_source_id,

    dim_crm_current_account_set_hierarchy_sk      AS dim_hierarchy_sk,
    dim_crm_current_account_set_business_unit_id  AS dim_business_unit_id,
    dim_crm_current_account_set_sales_segment_id  AS dim_sales_segment_id,
    dim_crm_current_account_set_geo_id            AS dim_geo_id,
    dim_crm_current_account_set_region_id         AS dim_region_id,
    dim_crm_current_account_set_area_id           AS dim_area_id,
    dim_crm_current_account_set_role_name_id      AS dim_role_name_id,
    dim_crm_current_account_set_role_level_1_id   AS dim_role_level_1_id,
    dim_crm_current_account_set_role_level_2_id   AS dim_role_level_2_id,
    dim_crm_current_account_set_role_level_3_id   AS dim_role_level_3_id,
    dim_crm_current_account_set_role_level_4_id   AS dim_role_level_4_id,
    dim_crm_current_account_set_role_level_5_id   AS dim_role_level_5_id,

    NULL                                      AS email_hash,
    new_logo_count,
    net_arr
  FROM fct_crm_opportunity
  WHERE is_net_arr_pipeline_created = TRUE
    AND arr_created_date IS NOT NULL

), mqls AS (

  SELECT
    mql_date_first_pt_id                      AS actual_date_id,
    prep_date.date_actual                     AS actual_date,
    'MQL'                                     AS sales_funnel_kpi_name,

    {{ get_keyed_nulls('NULL') }}             AS dim_crm_opportunity_id,
    dim_crm_account_id,
    dim_crm_person_id,

    -- we need to add the order_type key for first order because fct_sales_funnel_target has first order for all MQL targets
    first_order_key.dim_order_type_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_sales_qualified_source_id,

    dim_account_demographics_hierarchy_sk,
    {{ get_keyed_nulls('NULL') }}                AS dim_crm_user_business_unit_id,
    dim_account_demographics_sales_segment_id,
    dim_account_demographics_geo_id,
    dim_account_demographics_region_id,
    dim_account_demographics_area_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_name_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_1_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_2_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_3_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_4_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_5_id,

    email_hash,
    NULL                                      AS new_logo_count,
    NULL                                      AS net_arr
  FROM fct_crm_person
  LEFT JOIN prep_date
    ON fct_crm_person.mql_date_first_pt_id = prep_date.date_id
  LEFT JOIN first_order_key
  WHERE mql_date_first_pt_id IS NOT NULL

), trials AS (

  SELECT
    created_date_pt_id                        AS actual_date_id,
    prep_date.date_actual                     AS actual_date,
    'Trials'                                  AS sales_funnel_kpi_name,

    {{ get_keyed_nulls('NULL') }}             AS dim_crm_opportunity_id,
    dim_crm_account_id,
    dim_crm_person_id,

    -- we need to add the order_type key for first order because fct_sales_funnel_target has first order for all MQL targets
    first_order_key.dim_order_type_id,
    {{ get_keyed_nulls('NULL') }}             AS dim_sales_qualified_source_id,

    dim_account_demographics_hierarchy_sk,
    {{ get_keyed_nulls('NULL') }}                AS dim_crm_user_business_unit_id,
    dim_account_demographics_sales_segment_id,
    dim_account_demographics_geo_id,
    dim_account_demographics_region_id,
    dim_account_demographics_area_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_name_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_1_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_2_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_3_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_4_id,
    {{ get_keyed_nulls('NULL') }}                AS dim_role_level_5_id,

    email_hash,
    NULL                                      AS new_logo_count,
    NULL                                      AS net_arr
  FROM fct_crm_person
  LEFT JOIN prep_date
    ON fct_crm_person.created_date_pt_id = prep_date.date_id
  LEFT JOIN first_order_key
  WHERE is_lead_source_trial = TRUE

), metrics AS (

  SELECT *
  FROM net_arr

  UNION ALL

  SELECT *
  FROM closed_deals

  UNION ALL

  SELECT *
  FROM new_logos

  UNION ALL

  SELECT *
  FROM saos

  UNION ALL

  SELECT *
  FROM pipeline_created

  UNION ALL

  SELECT *
  FROM mqls

  UNION ALL

  SELECT *
  FROM trials

)

SELECT
  {{ dbt_utils.generate_surrogate_key(['metrics.actual_date_id', 'metrics.sales_funnel_kpi_name', 'metrics.dim_crm_opportunity_id', 'metrics.dim_crm_person_id']) }} AS sales_funnel_actual_sk,
  {{ dbt_utils.generate_surrogate_key(['metrics.sales_funnel_kpi_name'])}} AS dim_sales_funnel_kpi_sk,
  metrics.actual_date_id,
  metrics.actual_date,
  metrics.sales_funnel_kpi_name,
  metrics.dim_crm_opportunity_id,
  metrics.dim_crm_account_id,
  metrics.dim_crm_person_id,
  metrics.dim_order_type_id,
  metrics.dim_sales_qualified_source_id,
  {{ get_keyed_nulls('metrics.dim_hierarchy_sk') }} AS dim_hierarchy_sk,
  metrics.dim_business_unit_id,
  metrics.dim_sales_segment_id,
  metrics.dim_geo_id,
  metrics.dim_region_id,
  metrics.dim_area_id,
  metrics.dim_role_name_id,
  metrics.dim_role_level_1_id,
  metrics.dim_role_level_2_id,
  metrics.dim_role_level_3_id,
  metrics.dim_role_level_4_id,
  metrics.dim_role_level_5_id,
  metrics.email_hash,
  metrics.new_logo_count,
  metrics.net_arr
FROM metrics
