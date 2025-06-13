WITH source AS (

  SELECT *
  FROM {{ source('zuora_query_api', 'chargecontractualvalue') }}

),

renamed AS (

  SELECT
    "ID"::TEXT                                                    AS charge_contractual_value_id,
    "AMOUNT"::FLOAT                                                AS amount,
    "createdBy"::TEXT                                             AS created_by,
    TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "createdOn"))::TIMESTAMP AS created_on,
    "CURRENCY"::TEXT                                              AS currency,
    "ELP"::FLOAT                                                 AS elp,
    "elpTaxAmount"::FLOAT                                        AS elp_tax_amount,
    "estimatedEvergreenEndDate"::TEXT                             AS estimated_ever_green_end_date,
    "ratePlanChargeId"::TEXT                                      AS rate_plan_charge_id,
    "REASON"::TEXT                                                AS reason,
    "subscriptionId"::TEXT                                        AS subscription_id,
    "taxAmount"::TEXT                                             AS tax_amount,
    "updatedBy"::TEXT                                             AS updated_by,
    TO_TIMESTAMP(CONVERT_TIMEZONE('UTC', "updatedOn"))::TIMESTAMP AS updated_on,
    TO_TIMESTAMP_NTZ("_UPLOADED_AT"::INT)::TIMESTAMP              AS uploaded_at
  FROM source

)

SELECT *
FROM renamed
