WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'customers') }}

), renamed AS (

    SELECT
      --Primary Key
      customer_id::FLOAT                 AS customer_id,

      --Foreign Keys
      subsidiary_id::FLOAT               AS subsidiary_id,
      currency_id::FLOAT                 AS currency_id,
      parent_id::FLOAT                   AS parent_id,
      department_id::FLOAT               AS department_id,

      --Info
      companyname::VARCHAR               AS customer_name,
      name::VARCHAR                      AS customer_alt_name,
      full_name::VARCHAR                 AS customer_full_name,
      rev_rec_forecast_rule_id::FLOAT    AS rev_rec_forecast_rule_id,

      --deposit_balance_foreign
      openbalance::FLOAT                 AS customer_balance,
      days_overdue::FLOAT                AS days_overdue,
      _fivetran_deleted                AS is_fivetran_deleted

    FROM source

)

SELECT *
FROM renamed
