{{ config(materialized='table') }}

{{ simple_cte([
    ('rpt_l2r_campaign_interactions', 'rpt_l2r_campaign_interactions'),
    ('mart_crm_account', 'mart_crm_account')
]) }}

, campaigns AS (

  SELECT
      {{ dbt_utils.star(
           from = ref('rpt_l2r_campaign_interactions'), 
           except=['CREATED_BY','UPDATED_BY','MODEL_CREATED_DATE','MODEL_UPDATED_DATE','DBT_UPDATED_AT','DBT_CREATED_AT']
           ) 
      }}
  FROM
    {{ ref('rpt_l2r_campaign_interactions') }}

   WHERE touchpoint_type = 'Person Touchpoint'

), final AS (

  SELECT
   campaigns.* 
  FROM campaigns 
  INNER JOIN mart_crm_account 
    ON mart_crm_account.dim_crm_account_id = campaigns.dim_crm_account_id
   WHERE mart_crm_account.is_deleted = FALSE
   AND mart_crm_account.crm_account_type = 'Customer'

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@jonglee1218",
    updated_by="@jonglee1218",
    created_date="2024-02-21",
    updated_date="2024-02-21",
  ) }}