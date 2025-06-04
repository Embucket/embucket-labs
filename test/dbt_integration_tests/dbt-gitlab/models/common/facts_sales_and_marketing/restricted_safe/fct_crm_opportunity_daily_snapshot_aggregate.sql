{{ config(
    materialized="incremental",
    unique_key="opportunity_daily_snapshot_aggregate_pk"
) }}

WITH actuals AS (

  SELECT * 
  FROM {{ ref('fct_crm_opportunity_daily_snapshot') }}
  
  {% if is_incremental() %}
    WHERE snapshot_date > (SELECT MAX(snapshot_date) FROM {{this}})
  {% endif %}

),

final AS (

  SELECT

    {{ dbt_utils.generate_surrogate_key([
      'snapshot_id',
      'dim_crm_current_account_set_hierarchy_sk',
      'order_type',
      'order_type_grouped',
      'stage_name',
      'deal_path',
      'subscription_type',
      'sales_qualified_source',
      'sales_qualified_source_grouped',
      'close_fiscal_quarter_date',
      'close_fiscal_quarter_name_for_pipegen_grouping',
      'created_fiscal_quarter_date',
      'arr_created_fiscal_quarter_date',
      'deal_path_live',
      'sales_qualified_source_live',
      'sales_qualified_source_grouped_live',
      'order_type_live',
      'order_type_grouped_live'])
    }}                                                                 AS opportunity_daily_snapshot_aggregate_pk,

    -- keys and attributes
    dim_crm_current_account_set_hierarchy_sk,
    
    --snapshot fields
    stage_name,
    subscription_type,
    deal_path,
    sales_qualified_source,
    sales_qualified_source_grouped,
    order_type,
    order_type_grouped,
    is_stage_1_plus,
    is_stage_3_plus,
    is_stage_4_plus,
    
    --live fields
    deal_path_live,
    sales_qualified_source_live,
    sales_qualified_source_grouped_live,
    order_type_live,
    order_type_grouped_live,

    --dates
    snapshot_id,
    snapshot_date,
    snapshot_month,
    snapshot_fiscal_year,
    snapshot_fiscal_quarter_name,
    snapshot_fiscal_quarter_date,
    snapshot_day_of_fiscal_quarter_normalised,
    snapshot_day_of_fiscal_year_normalised,
    snapshot_last_day_of_fiscal_quarter,

    close_fiscal_quarter_date,
    close_fiscal_quarter_name_for_pipegen_grouping,
    created_fiscal_quarter_date,
    arr_created_fiscal_quarter_date,
    landing_quarter_relative_to_arr_created_date,
    landing_quarter_relative_to_snapshot_date,
    snapshot_to_close_diff,
    arr_created_to_close_diff,

    -- Running sum of metrics
    SUM(created_arr_in_snapshot_quarter)                               AS created_arr_in_snapshot_quarter,
    SUM(closed_won_opps_in_snapshot_quarter)                           AS closed_won_opps_in_snapshot_quarter,
    SUM(closed_opps_in_snapshot_quarter)                               AS closed_opps_in_snapshot_quarter,
    SUM(booked_net_arr_in_snapshot_quarter)                            AS booked_net_arr_in_snapshot_quarter,
    SUM(created_deals_in_snapshot_quarter)                             AS created_deals_in_snapshot_quarter,
    SUM(cycle_time_in_days_in_snapshot_quarter)                        AS cycle_time_in_days_in_snapshot_quarter,
    SUM(booked_deal_count_in_snapshot_quarter)                         AS booked_deal_count_in_snapshot_quarter,
    SUM(open_1plus_net_arr_in_snapshot_quarter)                        AS open_1plus_net_arr_in_snapshot_quarter,
    SUM(open_3plus_net_arr_in_snapshot_quarter)                        AS open_3plus_net_arr_in_snapshot_quarter,
    SUM(open_4plus_net_arr_in_snapshot_quarter)                        AS open_4plus_net_arr_in_snapshot_quarter,
    SUM(open_1plus_deal_count_in_snapshot_quarter)                     AS open_1plus_deal_count_in_snapshot_quarter,
    SUM(open_3plus_deal_count_in_snapshot_quarter)                     AS open_3plus_deal_count_in_snapshot_quarter,
    SUM(open_4plus_deal_count_in_snapshot_quarter)                     AS open_4plus_deal_count_in_snapshot_quarter,
    SUM(positive_booked_deal_count_in_snapshot_quarter)                AS positive_booked_deal_count_in_snapshot_quarter,
    SUM(positive_booked_net_arr_in_snapshot_quarter)                   AS positive_booked_net_arr_in_snapshot_quarter,
    SUM(positive_open_deal_count_in_snapshot_quarter)                  AS positive_open_deal_count_in_snapshot_quarter,
    SUM(positive_open_net_arr_in_snapshot_quarter)                     AS positive_open_net_arr_in_snapshot_quarter,
    SUM(closed_deals_in_snapshot_quarter)                              AS closed_deals_in_snapshot_quarter,
    SUM(closed_net_arr_in_snapshot_quarter)                            AS closed_net_arr_in_snapshot_quarter,

    -- Additive fields
    SUM(net_incremental_acv)                                           AS net_incremental_acv,
    SUM(incremental_acv)                                               AS incremental_acv,
    SUM(created_in_snapshot_quarter_net_arr)                           AS created_in_snapshot_quarter_net_arr,
    SUM(created_in_snapshot_quarter_deal_count)                        AS created_in_snapshot_quarter_deal_count,
    SUM(opportunity_based_iacv_to_net_arr_ratio)                       AS opportunity_based_iacv_to_net_arr_ratio,
    SUM(segment_order_type_iacv_to_net_arr_ratio)                      AS segment_order_type_iacv_to_net_arr_ratio,
    SUM(calculated_from_ratio_net_arr)                                 AS calculated_from_ratio_net_arr,
    SUM(net_arr)                                                       AS net_arr,
    SUM(raw_net_arr)                                                   AS raw_net_arr,
    SUM(created_and_won_same_quarter_net_arr)                          AS created_and_won_same_quarter_net_arr,
    SUM(new_logo_count)                                                AS new_logo_count,
    SUM(amount)                                                        AS amount,
    SUM(recurring_amount)                                              AS recurring_amount,
    SUM(true_up_amount)                                                AS true_up_amount,
    SUM(proserv_amount)                                                AS proserv_amount,
    SUM(other_non_recurring_amount)                                    AS other_non_recurring_amount,
    SUM(arr_basis)                                                     AS arr_basis,
    SUM(arr)                                                           AS arr,
    SUM(count_crm_attribution_touchpoints)                             AS count_crm_attribution_touchpoints,
    SUM(weighted_linear_iacv)                                          AS weighted_linear_iacv,
    SUM(count_campaigns)                                               AS count_campaigns,
    SUM(probability)                                                   AS probability,
    SUM(days_in_sao)                                                   AS days_in_sao,
    SUM(open_1plus_deal_count)                                         AS open_1plus_deal_count,
    SUM(open_3plus_deal_count)                                         AS open_3plus_deal_count,
    SUM(open_4plus_deal_count)                                         AS open_4plus_deal_count,
    SUM(booked_deal_count)                                             AS booked_deal_count,
    SUM(churned_contraction_deal_count)                                AS churned_contraction_deal_count,
    SUM(open_1plus_net_arr)                                            AS open_1plus_net_arr,
    SUM(open_3plus_net_arr)                                            AS open_3plus_net_arr,
    SUM(open_4plus_net_arr)                                            AS open_4plus_net_arr,
    SUM(booked_net_arr)                                                AS booked_net_arr,
    SUM(churned_contraction_net_arr)                                   AS churned_contraction_net_arr,
    SUM(calculated_deal_count)                                         AS calculated_deal_count,
    SUM(cycle_time_in_days)                                            AS cycle_time_in_days,
    SUM(booked_churned_contraction_deal_count)                         AS booked_churned_contraction_deal_count,
    SUM(booked_churned_contraction_net_arr)                            AS booked_churned_contraction_net_arr,
    SUM(renewal_amount)                                                AS renewal_amount,
    SUM(total_contract_value)                                          AS total_contract_value,
    SUM(days_in_stage)                                                 AS days_in_stage,
    SUM(calculated_age_in_days)                                        AS calculated_age_in_days,
    SUM(days_since_last_activity)                                      AS days_since_last_activity,
    SUM(won_arr_basis_for_clari)                                       AS won_arr_basis_for_clari,
    SUM(arr_basis_for_clari)                                           AS arr_basis_for_clari,
    SUM(forecasted_churn_for_clari)                                    AS forecasted_churn_for_clari,
    SUM(override_arr_basis_clari)                                      AS override_arr_basis_clari,
    SUM(vsa_start_date_net_arr)                                        AS vsa_start_date_net_arr,
    SUM(created_arr)                                                   AS created_arr,
    SUM(closed_won_opps)                                               AS closed_won_opps,
    SUM(closed_opps)                                                   AS closed_opps,
    SUM(created_deals)                                                 AS created_deals,
    SUM(positive_booked_deal_count)                                    AS positive_booked_deal_count,
    SUM(positive_booked_net_arr)                                       AS positive_booked_net_arr,
    SUM(positive_open_deal_count)                                      AS positive_open_deal_count,
    SUM(positive_open_net_arr)                                         AS positive_open_net_arr,
    SUM(closed_deals)                                                  AS closed_deals,
    SUM(closed_net_arr)                                                AS closed_net_arr
  FROM actuals
  GROUP BY ALL

)

SELECT *
FROM final