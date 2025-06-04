{{ config(
    tags=["mnpi_exception"]
) }}

WITH performance_obligation_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_performance_obligation_source') }}

), final AS (

    SELECT DISTINCT
    
      -- ids
      event_id              AS dim_accounting_event_id,

      -- event details
      event_name,
      event_type,
      event_column_1,
      event_column_2,
      event_column_3,
      event_column_4,
      event_column_5

      -- metadata
      event_created_by,
      event_created_date,
      event_updated_by,
      event_updated_date

    FROM performance_obligation_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}