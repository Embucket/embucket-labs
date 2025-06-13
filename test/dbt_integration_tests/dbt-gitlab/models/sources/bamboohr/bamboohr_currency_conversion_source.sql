WITH source AS (

    SELECT *
    FROM {{ source('bamboohr', 'custom_currency_conversion') }}
    ORDER BY uploaded_at DESC
    LIMIT 1

), renamed AS (

    SELECT 
      data_by_row.value['id']::NUMBER                                      AS conversion_id,
      data_by_row.value['employeeId']::NUMBER                              AS employee_id,
      TRY_TO_DATE(data_by_row.value['customConversionEffectiveDate']::VARCHAR) AS effective_date,
      data_by_row.value['customCurrencyConversionFactor']::DECIMAL(10,5)   AS currency_conversion_factor,
      data_by_row.value['customLocalAnnualSalary']::VARCHAR                AS local_annual_salary,
      data_by_row.value['customUSDAnnualSalary']::VARCHAR                  AS usd_annual_salary,
      uploaded_at
    FROM source,
    LATERAL FLATTEN(INPUT => parse_json(jsontext), OUTER => true) data_by_row

), final AS (

    SELECT 
      conversion_id,
      employee_id,
      effective_date,
      currency_conversion_factor,
      local_annual_salary,
      REGEXP_REPLACE(local_annual_salary, '[0-9/-/#/./*]', '')                    AS annual_local_currency_code,
      REGEXP_REPLACE(usd_annual_salary, '[a-z/-/A-z/#/*]', '')::DECIMAL(10,2)     AS annual_amount_usd_value,
      REGEXP_REPLACE(usd_annual_salary, '[0-9/-/#/./*]', '')                      AS annual_local_usd_code ,
      uploaded_at
    FROM renamed

)

SELECT *
FROM final
