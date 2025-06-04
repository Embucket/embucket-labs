{{ config({
        "materialized": "table",
        "transient": false,
        "alias": "mart_arr_all_weekly"
    })
}}

{{ mart_arr_with_zero_dollar_charges_source('fct_mrr_weekly_with_zero_dollar_charges') }},

weekly_joined AS (
SELECT 
* EXCLUDE (dim_charge_id, fiscal_quarter_name_fy,fiscal_year, unit_of_measure,mrr,arr,quantity),
SUM(mrr)                                                                      AS mrr,
SUM(arr)                                                                      AS arr,
SUM(quantity)                                                                 AS quantity,
ARRAY_AGG(DISTINCT unit_of_measure) WITHIN GROUP (ORDER BY unit_of_measure)   AS unit_of_measure

FROM joined
group by all),

weekly_joined_with_primary_key AS (
  SELECT     {{ dbt_utils.generate_surrogate_key(['weekly_joined.dim_date_id', 
                                          'weekly_joined.subscription_name', 
                                           'weekly_joined.dim_product_detail_id']) }} AS primary_key,
        weekly_joined.*
  FROM weekly_joined

),

final AS (
  SELECT
    weekly_joined_with_primary_key.date_actual AS arr_week,
    {{ mart_arr_with_zero_dollar_charges_fields('weekly_joined_with_primary_key') }},
    weekly_joined_with_primary_key.parent_crm_account_sales_segment_legacy
  FROM weekly_joined_with_primary_key
)

SELECT *
FROM final
