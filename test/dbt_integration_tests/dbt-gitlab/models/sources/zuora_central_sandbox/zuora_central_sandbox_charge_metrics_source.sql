WITH source AS (

  SELECT *
  FROM {{ source('zuora_query_api_sandbox', 'chargemetrics') }}

),

renamed AS (

  SELECT
    "ID"::TEXT                                                      AS charge_metrics_id,
    "SUBSCRIPTIONOWNERACCOUNTNUMBER"::TEXT                          AS subscription_owner_account_number,
    "INVOICEOWNERACCOUNTNUMBER"::TEXT                               AS invoice_owner_account_number,
    "SUBSCRIPTIONNAME"::TEXT                                        AS subscription_name,
    "CHARGENUMBER"::TEXT                                            AS charge_number,
    "RATEPLANCHARGEID"::TEXT                                        AS rate_plan_charge_id,
    "PRODUCTID"::TEXT                                               AS product_id,
    "PRODUCTRATEPLANID"::TEXT                                       AS product_rate_plan_id,
    "PRODUCTRATEPLANCHARGEID"::TEXT                                 AS product_rate_plan_charge_id,
    "AMENDMENTID"::TEXT                                             AS amendment_id,
    "AMENDMENTTYPE"::TEXT                                           AS amendment_type,
    TRY_TO_DATE("STARTDATE"::TEXT)                                  AS start_date,
    TRY_TO_DATE("ENDDATE"::TEXT)                                    AS end_date,
    "MRRGROSSAMOUNT"::FLOAT                                         AS mrr_gross_amount,
    "MRRNETAMOUNT"::FLOAT                                           AS mrr_net_amount,
    "MRRDISCOUNTAMOUNT"::FLOAT                                      AS mrr_discount_amount,
    "TCVGROSSAMOUNT"::FLOAT                                         AS tcv_gross_amount,
    "TCVNETAMOUNT"::FLOAT                                           AS tcv_net_amount,
    "TCVDISCOUNTAMOUNT"::FLOAT                                      AS tcv_discount_amount,
    "CURRENCY"::TEXT                                                AS currency,
    TO_TIMESTAMP_NTZ("_UPLOADED_AT"::INT)::TIMESTAMP                AS uploaded_at
  FROM source

)

SELECT *
FROM renamed
