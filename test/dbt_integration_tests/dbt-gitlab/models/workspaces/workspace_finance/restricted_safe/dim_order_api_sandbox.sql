{{ config(
    tags=["mnpi"]
) }}

WITH base AS (

    SELECT *
    FROM {{ref('zuora_api_sandbox_order_source')}}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      
      dim_order_id,
      order_description,
      order_created_date,
      order_date,
      order_number,
      order_state,
      order_status,
      is_created_by_migration

    FROM base

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@ken_aguilar",
    updated_by="@ken_aguilar",
    created_date="2022-02-07",
    updated_date="2022-02-07"
) }}