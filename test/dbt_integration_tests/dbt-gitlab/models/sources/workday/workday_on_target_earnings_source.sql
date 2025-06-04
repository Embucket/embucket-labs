WITH source AS (
  SELECT *
  FROM {{ source('workday','on_target_earnings') }}
  WHERE NOT _fivetran_deleted
  
),

renamed AS (

  SELECT
    source.employee_id::NUMBER AS employee_id,
    source._fivetran_synced::TIMESTAMP AS uploaded_at,
    events.value['ANNUAL_AMOUNT_LOCAL']::FLOAT AS annual_amount_local,
    events.value['ANNUAL_AMOUNT_LOCAL_-_Currency_Code']::VARCHAR
    AS annual_amount_local_currency_code,
    events.value['ANNUAL_AMOUNT_USD_VALUE']::FLOAT AS annual_amount_usd_value,
    events.value['EFFECTIVE_DATE']::DATE AS effective_date,
    events.value['OTE_LOCAL']::FLOAT AS ote_local,
    events.value['OTE_LOCAL_-_Currency_Code']::VARCHAR AS ote_local_currency_code,
    events.value['OTE_TYPE']::VARCHAR AS ote_type,
    events.value['OTE_USD']::FLOAT AS ote_usd,
    events.value['DATE_TIME_INITIATED']::TIMESTAMP AS initiated_at,
    events.value['WID']::VARCHAR AS workday_id
  FROM source
  INNER JOIN LATERAL FLATTEN(INPUT => source.compensation_history) AS events

)

SELECT *
FROM renamed
