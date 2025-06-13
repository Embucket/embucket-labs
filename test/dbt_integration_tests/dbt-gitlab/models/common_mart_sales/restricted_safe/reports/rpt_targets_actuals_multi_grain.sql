{{ config(
    tags=["mnpi_exception"]
) }}


WITH unioned AS (

  {{ union_tables(
    [
        ref('mart_crm_opportunity_7th_day_weekly_snapshot'),
        ref('mart_crm_opportunity_7th_day_weekly_snapshot_aggregate'),
        ref('rpt_pipeline_coverage'),
        ref('rpt_final_bookings')
    ],
    filters={
        'mart_crm_opportunity_7th_day_weekly_snapshot': 'is_current_snapshot_quarter = true',
        'mart_crm_opportunity_7th_day_weekly_snapshot_aggregate': 'is_current_snapshot_quarter = false'
    }
) }}

),

final AS (

  SELECT 
    unioned.*,
    MAX(CASE 
      WHEN source != 'targets_actuals' AND snapshot_date < CURRENT_DATE() THEN snapshot_date 
      ELSE NULL 
    END) OVER ()                                                        AS max_snapshot_date, -- We want to ensure we have the max_snapshot_date that comes from the actuals in every row but excluding the future dates we have in the targets data 
    FLOOR((DATEDIFF(day, current_first_day_of_fiscal_quarter, max_snapshot_date) / 7)) 
                                                                        AS most_recent_snapshot_week
  FROM unioned 
  WHERE snapshot_fiscal_quarter_date >= DATEADD(QUARTER, -9, CURRENT_DATE())
  -- filter out unneeded opps
  AND (source IN ('final_bookings','targets_actuals')
    OR created_deals_in_snapshot_quarter IS NOT NULL
    OR open_1plus_deal_count_in_snapshot_quarter IS NOT NULL
    OR closed_deals_in_snapshot_quarter IS NOT NULL
    OR created_arr_in_snapshot_quarter IS NOT NULL)

)

SELECT final.*,
  fiscal_quarter_name_fy AS most_recent_snapshot_date_fiscal_quarter_name_fy,
  fiscal_quarter_name_fy AS most_recent_snapshot_date_fiscal_quarter_name,
  first_day_of_fiscal_quarter AS most_recent_snapshot_date_fiscal_quarter_date
FROM final
LEFT JOIN {{ ref('dim_date') }} dim_date
ON final.max_snapshot_date = dim_date.date_actual
