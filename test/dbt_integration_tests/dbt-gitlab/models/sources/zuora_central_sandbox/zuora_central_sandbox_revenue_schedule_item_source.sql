WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'revenue_schedule_item') }}

), renamed AS (

    SELECT
      --Primary Keys
      id::VARCHAR                                   AS revenue_schedule_item_id,

      --Foreign Keys
      account_id::VARCHAR                           AS account_id,
      -- parent_account_id::VARCHAR                    AS parent_account_id,
      accounting_period_id::VARCHAR                 AS accounting_period_id,
      amendment_id::VARCHAR                         AS amendment_id,
      subscription_id::VARCHAR                      AS subscription_id,
      product_id::VARCHAR                           AS product_id,
      rate_plan_charge_id::VARCHAR                  AS rate_plan_charge_id,
      rate_plan_id::VARCHAR                         AS rate_plan_id,
      sold_to_contact_id::VARCHAR                   AS sold_to_contact_id,

      --Info
      amount::FLOAT                                 AS revenue_schedule_item_amount,
      bill_to_contact_id::VARCHAR                   AS bill_to_contact_id,
      currency::VARCHAR                             AS currency,
      created_by_id::VARCHAR                        AS created_by_id,
      created_date::TIMESTAMP_TZ                    AS created_date,
      default_payment_method_id::VARCHAR            AS default_payment_method_id,
      _FIVETRAN_DELETED::BOOLEAN                    AS is_deleted,
      updated_by_id::VARCHAR                        AS updated_by_id,
      updated_date::TIMESTAMP_TZ                    AS updated_date

      FROM source

)

SELECT *
FROM renamed
