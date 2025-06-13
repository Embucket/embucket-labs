{{ config({
    "materialized": "table"
    })
}}

WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'custom_on_target_earnings') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), renamed AS (

    SELECT 
      data_by_row.value['id']::NUMBER                       AS target_earnings_update_id,
      data_by_row.value['employeeId']::NUMBER               AS employee_id,
      TRY_TO_DATE(data_by_row.value['customDate']::VARCHAR) AS effective_date,
      data_by_row.value['customAnnualAmountLocal']::VARCHAR AS annual_amount_local,
      data_by_row.value['customAnnualAmountUSD']::VARCHAR   AS annual_amount_usd,
      data_by_row.value['customOTELocal']::VARCHAR          AS ote_local,
      data_by_row.value['customOTEUSD']::VARCHAR            AS ote_usd,
      data_by_row.value['customType']::VARCHAR              AS ote_type,
      data_by_row.value['customVariablePay']::VARCHAR       AS variable_pay,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => true) data_by_row

), 

reformat AS (

    SELECT 
      target_earnings_update_id,
      employee_id,
      effective_date,
      variable_pay,
      NULLIF(SPLIT_PART(annual_amount_local, ' ', 1),'') AS annual_amount_local,
      NULLIF(SPLIT_PART(annual_amount_usd,' ', 1),'')    AS annual_amount_usd_value,
      NULLIF(SPLIT_PART(ote_local,' ', 1),'')            AS ote_local,
      NULLIF(SPLIT_PART(ote_usd,' ', 1),'')              AS ote_usd,
      SPLIT_PART(annual_amount_local, ' ', 2)            AS annual_amount_local_currency_code,
      SPLIT_PART(ote_local, ' ', 2)                      AS ote_local_currency_code,
      ote_type,
      uploaded_at
    FROM renamed
    WHERE target_earnings_update_id != 23721 --incorrect order

)

SELECT *
FROM reformat
