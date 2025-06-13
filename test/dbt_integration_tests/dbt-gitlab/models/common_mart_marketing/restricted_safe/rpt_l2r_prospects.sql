SELECT
   * 
FROM
   {{ ref('rpt_lead_to_revenue') }} AS rpt_lead_to_revenue 
WHERE
   crm_account_type IN ('Prospect','Former Customer')
   AND dim_crm_btp_touchpoint_id IS NOT NULL 	
   AND dim_crm_account_id IS NOT NULL 	
   AND bizible_touchpoint_date >= dateadd(day, - 366, current_date) 
   AND 
   (
      pipeline_created_date >= dateadd(day, - 366, current_date) 
      OR dim_crm_opportunity_id IS NULL
   )
   AND 
   (
      LOWER(account_demographics_geo) != 'jihu' 
      AND LOWER(account_demographics_sales_segment) != 'jihu' 
      AND LOWER(opp_account_demographics_geo) != 'jihu' 
   )
   AND 
   (
      opp_order_type = '1. New - First Order' 
      OR opp_order_type is null 
   )