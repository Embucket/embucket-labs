WITH source AS (

    SELECT *
    FROM {{ source('zuora_query_api', 'rampintervalmetrics') }}

), renamed AS (

    SELECT
      "Id"::TEXT                                                                AS ramp_interval_metric_id,
      "ChargeNumber"::TEXT                                                      AS charge_number,
      "ProductRatePlanChargeId"::TEXT                                           AS product_rate_plan_charge_id,
      "StartDate"::DATE                                                         AS start_date,
      "EndDate"::DATE                                                           AS end_date,
      "Quantity"::NUMBER                                                        AS quantity,
      "GrossTcb"::NUMBER                                                        AS gross_tcb,
      "NetTcb"::NUMBER                                                          AS net_tcb,
      "DiscountTcb"::NUMBER                                                     AS discount_tcb,
      "GrossTcv"::NUMBER                                                        AS gross_tcv,
      "NetTcv"::NUMBER                                                          AS net_tcv,
      "DiscountTcv"::NUMBER                                                     AS discount_tcv,
      "RampIntervalId"::TEXT                                                    AS ramp_interval_id,
      "RatePlanChargeId"::TEXT                                                  AS rate_plan_charge_id,
      "SubscriptionNumber"::TEXT                                                AS subscription_number,
      "CreatedById"::TEXT                                                       AS created_by_id,
      TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "CreatedDate"))::TIMESTAMP           AS created_date,
      "UpdatedById"::TEXT                                                       AS updated_by_id,
      "UpdatedDate"::DATETIME                                                   AS updated_date,
      TO_TIMESTAMP_NTZ(CAST(_uploaded_at AS INT))::TIMESTAMP                    AS uploaded_at
    FROM source

)

SELECT *
FROM renamed
