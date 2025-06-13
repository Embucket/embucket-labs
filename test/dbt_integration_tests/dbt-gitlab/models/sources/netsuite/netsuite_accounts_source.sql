WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'accounts') }}

), renamed AS (

    SELECT
      --Primary Key
      account_id::FLOAT                                   AS account_id,

      --Foreign Keys
      parent_id::FLOAT                                    AS parent_account_id,
      currency_id::FLOAT                                  AS currency_id,
      department_id::FLOAT                                AS department_id,

      --Info
      name::VARCHAR                                       AS account_name,
      full_name::VARCHAR                                  AS account_full_name,
      full_description::VARCHAR                           AS account_full_description,
      accountnumber::VARCHAR                              AS account_number,
      expense_type_id::FLOAT                              AS expense_type_id,
      type_name::VARCHAR                                  AS account_type,
      type_sequence::FLOAT                                AS account_type_sequence,
      openbalance::FLOAT                                  AS current_account_balance,
      cashflow_rate_type::VARCHAR                         AS cashflow_rate_type,
      general_rate_type::VARCHAR                          AS general_rate_type,

      --Meta
      true::BOOLEAN                                 AS is_account_inactive,
      true::BOOLEAN                            AS is_balancesheet_account,
      true::BOOLEAN                 AS is_account_included_in_elimination,
      true::BOOLEAN                       AS is_account_included_in_reval,
      true::BOOLEAN                    AS is_account_including_child_subscriptions,
      true::BOOLEAN                                AS is_leftside_account,
      true::BOOLEAN                                 AS is_summary_account

    FROM source

)

SELECT *
FROM renamed
