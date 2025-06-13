{{ config(
    tags=["mnpi"]
) }}

WITH base AS (

    SELECT *
    FROM {{ref('zuora_central_sandbox_order_source')}}
    WHERE is_deleted = FALSE

), final AS (

    SELECT
      
      order_id              AS dim_order_id,
      description           AS order_description,
      created_date          AS order_created_date,
      order_date,
      order_number,
      state                 AS order_state,
      status                AS order_status,
      created_by_migration  AS is_created_by_migration

    FROM base

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-03-31",
    updated_date="2022-03-31"
) }}