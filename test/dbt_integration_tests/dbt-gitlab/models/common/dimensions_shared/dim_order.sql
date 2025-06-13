WITH base AS (

    SELECT *
    FROM {{ref('zuora_order_source')}}

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

    UNION ALL

    SELECT
      --Surrogate Keys
      MD5('-1')                             AS dim_order_id,

      --Information
      'Missing order_description'           AS order_description,
      '9999-12-31 00:00:00.000 +0000'       AS order_created_date,
      '9999-12-31 00:00:00.000 +0000'       AS order_date,
      'Missing order_number'                AS order_number,
      'Missing order_state'                 AS order_state,
      'Missing order_status'                AS order_status,
      NULL                                  AS is_created_by_migration

)

{{ dbt_audit(
    cte_ref="final",
    created_by="@michellecooper",
    updated_by="@michellecooper",
    created_date="2022-06-06",
    updated_date="2022-06-06"
) }}