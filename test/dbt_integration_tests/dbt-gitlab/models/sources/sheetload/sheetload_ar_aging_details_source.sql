WITH source AS (

  SELECT *
  FROM {{ source('sheetload', 'ar_aging_details') }}

), renamed AS (

  SELECT
    invoice_period::DATE                                AS invoice_period,
    account_name::VARCHAR                               AS account_name,
    account_number::VARCHAR                             AS account_number,
    account_entity::VARCHAR                             AS account_entity,
    currency::VARCHAR                                   AS currency,
    home_currency::VARCHAR                              AS home_currency,
    exchange_rate_date::DATE                            AS exchange_rate_date,
    exchange_rate::NUMBER                               AS exchange_rate,
    invoice_number::VARCHAR                             AS invoice_number,
    invoice_date::DATE                                  AS invoice_date,
    regexp_replace(invoice_amount, '[(),]','')::NUMBER  AS invoice_amount,
    due_date::DATE                                      AS due_date,
    days_aging::NUMBER                                  AS days_aging,
    aging_bucket::VARCHAR                               AS aging_bucket,
    regexp_replace(invoice_balance, '[(),]','')::NUMBER AS invoice_balance,
    invoice_balance_home_currency::NUMBER               AS invoice_balance_home_currency,
    invoice_balance_currency_rounding::NUMBER           AS invoice_balance_currency_rounding,
    "current"::NUMBER                                   AS "current",
    "1_to_30_days_past_due"::NUMBER                     AS "1_to_30_days_past_due",
    "31_to_60_days_past_due"::NUMBER                    AS "31_to_60_days_past_due",
    "61_to_90_days_past_due"::NUMBER                    AS "61_to_90_days_past_due",
    "91_to_120_days_past_due"::NUMBER                   AS "91_to_120_days_past_due",
    more_than_120_days_past_due::NUMBER                 AS more_than_120_days_past_due,
    current_home_currency::NUMBER                       AS current_home_currency,
    "1_to_30_days_past_due_home_currency"::NUMBER       AS "1_to_30_days_past_due_home_currency",
    "31_to_60_days_past_due_home_currency"::NUMBER      AS "31_to_60_days_past_due_home_currency",
    "61_to_90_days_past_due_home_currency"::NUMBER      AS "61_to_90_days_past_due_home_currency",
    "91_to_120_days_past_due_home_currency"::NUMBER     AS "91_to_120_days_past_due_home_currency",
     more_than_120_days_past_due_home_currency::NUMBER  AS more_than_120_days_past_due_home_currency
  FROM source

)

SELECT *
FROM renamed
