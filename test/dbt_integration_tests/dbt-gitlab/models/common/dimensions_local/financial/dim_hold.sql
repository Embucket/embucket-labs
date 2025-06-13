{{ config(
    tags=["mnpi_exception"]
) }}

WITH hold_source AS (

    SELECT *
    FROM {{ ref('zuora_revenue_revenue_contract_hold_source') }}

), final AS (

    SELECT DISTINCT
      
      -- ids
      hold_id               AS dim_hold_id,

      -- hold details
      hold_type,
      hold_name,
      hold_description,
      hold_level,
      hold_schedule_type

      -- metadata
      hold_created_by,
      hold_created_date,
      hold_update_by,
      hold_update_date

    FROM hold_source

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2021-06-21",
    updated_date="2021-06-21",
    ) 
 }}