{{ config(materialized='view') }}

WITH final AS (

    SELECT 
      dim_crm_opportunity_id,
      dim_crm_account_id,
      opportunity_name,
      close_date,
      cp_review_notes,
      qsr_notes,
      renewal_forecast_health,
      renewal_manager,
      risk_reasons,
      tam_notes,
      arr,
      forecasted_churn_for_clari,
      net_arr

    FROM {{ref('mart_crm_opportunity')}} 

)

SELECT * 
FROM final

