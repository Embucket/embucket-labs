WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'accounting_period') }}

), renamed AS (

    SELECT
      --Primary Keys
      id::VARCHAR                       AS accounting_period_id,

      --Info
      enddate::TIMESTAMP_TZ             AS end_date,
      fiscalyear::NUMBER                AS fiscal_year,
      name::VARCHAR                     AS accounting_period_name,
      startdate::TIMESTAMP_TZ           AS accounting_period_start_date,
      status::VARCHAR                   AS accounting_period_status,
      updatedbyid::VARCHAR              AS updated_by_id,
      updateddate::TIMESTAMP_TZ         AS updated_date

    FROM source

)

SELECT *
FROM renamed
