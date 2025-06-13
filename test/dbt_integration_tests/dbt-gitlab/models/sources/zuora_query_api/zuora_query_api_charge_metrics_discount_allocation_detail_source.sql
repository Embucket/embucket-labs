WITH source AS (

  SELECT *
  FROM {{ source('zuora_query_api', 'chargemetricsdiscountallocationdetail') }}

),

renamed AS (

  SELECT

    "Id"::TEXT                                                      AS charge_metrics_discount_allocation_detail_id,
    "ChargeMetricsId"::TEXT                                         AS charge_metrics_id,
    "DiscountChargeNumber"::TEXT                                    AS discount_charge_number,
    "DiscountMrr"::FLOAT                                           AS discount_mrr,
    "DiscountTcv"::FLOAT                                           AS discount_tcv,
    TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "CreatedDate"))::TIMESTAMP AS created_date,
    TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "UpdatedDate"))::TIMESTAMP AS updated_date,
    "DELETED"::BOOLEAN                                              AS deleted,
    "SubscriptionOwnerAccountNumber"::TEXT                          AS subscription_owner_account_number,
    "InvoiceOwnerAccountNumber"::TEXT                               AS invoice_owner_account_number,
    "SubscriptionName"::TEXT                                        AS subscription_name,
    "ChargeNumber"::TEXT                                            AS charge_number,
    "RatePlanChargeId"::TEXT                                        AS rate_plan_charge_id,
    "ProductId"::TEXT                                               AS product_id,
    "ProductRatePlanId"::TEXT                                       AS product_rate_plan_id,
    "ProductRatePlanChargeId"::TEXT                                 AS product_rate_plan_charge_id,
    "AmendmentId"::TEXT                                             AS amendment_id,
    "AmendmentType"::TEXT                                           AS amendment_type,
    TRY_TO_DATE("StartDate"::TEXT)                                  AS start_date,
    TRY_TO_DATE("EndDate"::TEXT)                                    AS end_date,
    "Currency"::TEXT                                                AS currency,
    TO_TIMESTAMP_NTZ("_UPLOADED_AT"::INT)::TIMESTAMP                AS uploaded_at
  FROM source

)

SELECT *
FROM renamed
