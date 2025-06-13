{{ config(materialized='view') }}

{{ simple_cte([
    ('dim_subscription', 'dim_subscription'),
    ('dim_date', 'dim_date'),
    ('dim_crm_account', 'dim_crm_account'),
    ('mart_crm_opportunity', 'mart_crm_opportunity'),
    ('rpt_product_usage_health_score', 'rpt_product_usage_health_score'),
    ('mart_arr_all', 'mart_arr_with_zero_dollar_charges'),
    ('mart_crm_account', 'mart_crm_account'),
    ('dim_date','dim_date')
]) }},

total_bookings_final AS (
SELECT 
    '1.1 Total Bookings' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
     fiscal_quarter_name_fy AS quarter,
     SUM(booked_net_arr) AS actuals_raw
    
    FROM mart_crm_opportunity
    LEFT JOIN dim_date 
    ON date_actual = close_date
    WHERE date_trunc('month',close_date) <= date_trunc('month',current_date) 
    AND fiscal_quarter_name_fy LIKE '%FY25%'
    AND fpa_master_bookings_flag = true
    GROUP BY 1,2,3
    ORDER BY 3 DESC
),

duo_final as (
SELECT 
    '2.1 Duo' AS yearly_name,
    'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
     fiscal_quarter_name_fy AS quarter,
     SUM(duo_net_arr) AS actuals_raw
    
    FROM mart_crm_opportunity
    LEFT JOIN dim_date 
    ON date_actual = close_date
    WHERE date_trunc('month',close_date) <= date_trunc('month',current_date) 
    AND fiscal_quarter_name_fy LIKE '%FY25%'
    AND fpa_master_bookings_flag = true
    GROUP BY 1,2,3
    ORDER BY 3 DESC
),

adoption_metrics_1 AS (
SELECT
    *
  FROM rpt_product_usage_health_score
  WHERE INSTANCE_TYPE = 'Production'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_month, dim_subscription_id_original, delivery_type
  ORDER BY billable_user_count desc nulls last, ping_created_at desc nulls last) = 1
),

adoption_metrics_2 AS (
SELECT
   '3.1 Adoption Metrics' AS yearly_name,
   'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_ARR_ALL, PROD.COMMON_MART_PRODUCT.RPT_PRODUCT_USAGE_HEALTH_SCORE' AS source_table,
   fiscal_quarter_name_fy,
   arr_month,
   IFF(adoption_metrics_1.ping_created_at is not null, true, false) AS usage_data_sent,
   SUM(arr) AS arr,
   SUM(arr)/SUM(SUM(arr)) OVER (partition by arr_month) AS percent_of_total
  
   FROM mart_arr_all
   LEFT JOIN adoption_metrics_1
   ON arr_month = snapshot_month
   AND mart_arr_all.dim_subscription_id_original = adoption_metrics_1.dim_subscription_id_original
   AND mart_arr_all.product_delivery_type = adoption_metrics_1.delivery_type
   WHERE product_tier_name NOT IN ('Storage','Not Applicable')
   GROUP BY 1,2,3,4,5
),

adoption_metrics_final AS (
SELECT
   yearly_name,
   source_table,
   dim_date.fiscal_quarter_name_fy as quarter,
   percent_of_total AS actuals_raw

   FROM adoption_metrics_2
   LEFT JOIN dim_date
   ON arr_month = date_actual
   WHERE arr_month <= dateadd('month',-1,current_date)
   AND fiscal_month_name_fy LIKE '%FY25%'
   AND usage_data_sent = true
   QUALIFY dense_rank() over (partition by quarter order by arr_month desc) = 1
),

security_adoption_1 AS (
SELECT DISTINCT
   arr_month,
   dim_subscription_id_original,
   product_delivery_type,
   SUM(arr) AS total_subscription_arr 

   FROM mart_arr_all 
   LEFT JOIN mart_crm_account 
   ON mart_arr_all.dim_crm_account_id = mart_crm_account.dim_crm_account_id 
   WHERE arr_month >= '2023-02-01' 
   AND arr_month <= dateadd('month',-1,current_date)
   AND crm_account_type = 'Customer' 
   AND product_tier_name ILIKE '%Ultimate%'
   GROUP BY 1,2,3 
),

security_adoption_2 AS (
SELECT
    *
  FROM rpt_product_usage_health_score
  WHERE INSTANCE_TYPE = 'Production'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_month, dim_subscription_id_original, delivery_type
  ORDER BY billable_user_count desc nulls last, ping_created_at desc nulls last) = 1
),

security_adoption_3 AS (
SELECT DISTINCT
   security_adoption_1.arr_month,
   security_adoption_1.dim_subscription_id_original,
   security_adoption_1.product_delivery_type,
   security_adoption_1.total_subscription_arr,
   security_adoption_2.security_color_ultimate_only 
   
   FROM security_adoption_1 
   LEFT JOIN security_adoption_2 
   ON security_adoption_1.dim_subscription_id_original = security_adoption_2.dim_subscription_id_original 
   AND security_adoption_1.arr_month = security_adoption_2.snapshot_month 
   AND security_adoption_1.product_delivery_type = security_adoption_2.delivery_type 
),

security_adoption_4 as (
SELECT DISTINCT
   arr_month,
   security_color_ultimate_only,
   fiscal_quarter_name_fy as quarter,
   dense_rank() over (partition by fiscal_quarter_name_fy order by arr_month desc) as arr_month_rank,
   SUM(total_subscription_arr) AS ultimate_ARR,
   RATIO_TO_REPORT(ultimate_ARR) OVER (PARTITION BY arr_month) AS percent_of_ultimate_arr 
   
   FROM security_adoption_3 		
   LEFT JOIN dim_date 
   ON arr_month = date_actual 
   WHERE SECURITY_COLOR_ULTIMATE_ONLY IS NOT NULL 
   AND arr_month <= dateadd('month',-1,current_date)
   GROUP BY 1,2,3
   ORDER BY 1 DESC 
),

security_adoption_final as (
SELECT
   '3.3 Ultimate Security & Compliance Adoption' AS yearly_name,
   'PROD.COMMON_MART_PRODUCT.RPT_PRODUCT_USAGE_HEALTH_SCORE, PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_ARR_ALL' AS source_table,
   quarter,
   percent_of_ultimate_arr AS actuals_raw 

   FROM security_adoption_4 
   WHERE security_color_ultimate_only = 'Green'
   AND quarter LIKE '%FY25%'
   AND arr_month_rank = 1
),

churn_contraction_1 AS (
SELECT
   mart_crm_opportunity.close_fiscal_quarter_name,
   mart_crm_opportunity.arr_basis_for_clari,
   mart_crm_opportunity.order_type,
   mart_crm_opportunity.won_arr_basis_for_clari - mart_crm_opportunity.arr_basis_for_clari AS renewal_net_arr 
   
   FROM mart_crm_opportunity
   JOIN dim_crm_account
   ON dim_crm_account.dim_crm_account_id = mart_crm_opportunity.dim_crm_account_id
   WHERE mart_crm_opportunity.subscription_type = 'Renewal' 
   AND mart_crm_opportunity.stage_name IN ('Closed Won','8-Closed Lost')
   AND mart_crm_opportunity.is_jihu_account = FALSE 
   AND mart_crm_opportunity.order_type IN ('3. Growth','4. Contraction','5. Churn - Partial','6. Churn - Final')
   AND mart_crm_opportunity.close_month <= date_trunc('month', current_date) 
),

churn_contraction_2 AS (
SELECT
   CLOSE_FISCAL_QUARTER_NAME,
   SUM(CASE WHEN order_type in ('3. Growth','4. Contraction','5. Churn - Partial','6. Churn - Final') then renewal_net_arr end) AS renewal_net_arr_loss,     
   SUM(arr_basis_for_clari) AS atr 
   
   FROM churn_contraction_1
   GROUP BY 1 
),

churn_contraction_final AS (
SELECT
   '3.4 Churn and Contraction' AS yearly_name,
   'PROD.RESTRICTED_SAFE_COMMON_SALES.MART_CRM_OPPORTUNITY, PROD.COMMON.DIM_SUBSCRIPTION' AS source_table,
   CLOSE_FISCAL_QUARTER_NAME AS quarter,
   renewal_net_arr_loss / atr* - 1 AS actuals_raw
   
   FROM churn_contraction_2
   WHERE quarter like 'FY25%'
),

ultimate_bookings_final AS (
SELECT 
   '4.2 Ultimate Bookings' AS yearly_name,
   'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
   fiscal_quarter_name_fy AS quarter,
   SUM(booked_net_arr) AS actuals_raw
    
   FROM mart_crm_opportunity 
   LEFT JOIN dim_date 
   ON date_actual = close_date
   WHERE date_trunc('month',close_date) <= date_trunc('month',current_date) 
   AND fiscal_quarter_name_fy LIKE '%FY25%'
   AND product_category LIKE '%Ultimate%'
   AND fpa_master_bookings_flag = true
   GROUP BY 1,2,3
   ORDER BY 3 DESC
),

dedicated_bookings_final AS (
SELECT 
   '4.3 Dedicated Bookings' AS yearly_name,
   'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
   fiscal_quarter_name_fy AS quarter,
   SUM(booked_net_arr) AS actuals_raw
    
   FROM mart_crm_opportunity
   LEFT JOIN dim_date 
   ON date_actual = close_date
   WHERE date_trunc('month',close_date) <= date_trunc('month',current_date) 
   AND fiscal_quarter_name_fy LIKE '%FY25%'
   AND product_details ilike '%dedicated%'
   AND fpa_master_bookings_flag = true
   GROUP BY 1,2,3
   ORDER BY 3 DESC
),

plan_final as (
SELECT 
   '4.4 Plan' AS yearly_name,
   'PROD.RESTRICTED_SAFE_COMMON_MART_SALES.MART_CRM_OPPORTUNITY' AS source_table,
   fiscal_quarter_name_fy AS quarter,
   SUM(enterprise_agile_planning_net_arr) AS actuals_raw
    
   FROM mart_crm_opportunity
   LEFT JOIN dim_date 
   ON date_actual = close_date
   WHERE date_trunc('month',close_date) <= date_trunc('month',current_date) 
   AND fiscal_quarter_name_fy LIKE '%FY25%'
   AND fpa_master_bookings_flag = true
   GROUP BY 1,2,3
   ORDER BY 3 DESC
),

final  AS (

SELECT * FROM total_bookings_final 

UNION ALL

SELECT * FROM duo_final

UNION ALL

SELECT * FROM adoption_metrics_final

UNION ALL

SELECT * FROM security_adoption_final 

UNION ALL

SELECT * FROM churn_contraction_final

UNION ALL

SELECT * FROM ultimate_bookings_final

UNION ALL

SELECT * FROM dedicated_bookings_final

UNION ALL

SELECT * FROM plan_final
)

SELECT 
  * 
FROM 
  final
  