{{ config(materialized='view') }}

WITH final AS (

    SELECT 
      account_owner,
      account_tier,
      account_tier_notes,
      dim_crm_account_id,
      dim_parent_crm_account_id,
      crm_account_name,
      crm_account_owner_manager,
      crm_account_type,
      gs_health_csm_sentiment,
      gs_last_csm_activity_date,
      is_deleted,
      next_renewal_date,
      health_score_color,
      parent_crm_account_geo,
      parent_crm_account_industry,
      parent_crm_account_name,
      parent_crm_account_region,
      parent_crm_account_sales_segment,
      risk_reason,
      tam_manager,
      technical_account_manager,
      carr_this_account,
      crm_account_employee_count,
      ptc_score_group,
      pte_score_group

    FROM {{ref('mart_crm_account')}}

)

SELECT * 
FROM final
