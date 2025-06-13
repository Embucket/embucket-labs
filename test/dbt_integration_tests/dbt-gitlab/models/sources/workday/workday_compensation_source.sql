WITH source AS (
  SELECT *
  FROM {{ source('workday','compensation') }}
  WHERE NOT _fivetran_deleted
  
),

renamed AS (

  SELECT
    source.employee_id::NUMBER AS employee_id,
    source._fivetran_synced::TIMESTAMP AS uploaded_at,
    events.value['EFFECTIVE_DATE']::TIMESTAMP AS effective_date,
    events.value['COMPENSATION_TYPE']::VARCHAR AS compensation_type,
    events.value['COMPENSATION_CHANGE_REASON']::VARCHAR AS compensation_change_reason,
    events.value['PAY_RATE']::VARCHAR AS pay_rate,
    events.value['COMPENSATION_VALUE']::FLOAT AS compensation_value,
    events.value['COMPENSATION_CURRENCY']::VARCHAR AS compensation_currency,
    events.value['CONVERSION_RATE_LOCAL_TO_USD']::FLOAT AS conversion_rate_local_to_usd,
    events.value['COMPENSATION_CURRENCY_USD']::VARCHAR AS compensation_currency_usd,
    events.value['COMPENSATION_VALUE_USD']::FLOAT AS compensation_value_usd,
    events.value['PAY_FREQUENCY']::VARCHAR AS pay_frequency,
    events.value['PER_PAY_PERIOD_AMOUNT']::FLOAT AS per_pay_period_amount,
    events.value['DATE_TIME_INITIATED']::TIMESTAMP AS initiated_at
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => source.compensation_history) AS events

)

SELECT *
FROM renamed
