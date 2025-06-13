{{ config(
    tags=["mnpi_exception"]
) }}

WITH schedule_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_schedule_source') }}

), final AS (

    SELECT DISTINCT
      
      -- ids
      revenue_contract_schedule_id      AS dim_revenue_contract_schedule_id, 
      
      -- dates
      exchange_rate_date,
      post_date,

      -- currency
      transactional_currency,
      schedule_type,
      
      -- metadata
      revenue_contract_schedule_created_by,
      revenue_contract_schedule_created_date,
      revenue_contract_schedule_updated_by,
      revenue_contract_schedule_updated_date

    FROM schedule_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}