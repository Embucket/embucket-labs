WITH source AS (

    SELECT
      month::DATE                     AS month,
      quarter::VARCHAR                AS quarter,
      year::VARCHAR                   AS year,
      metric_name::VARCHAR            AS metric_name,
      amount::FLOAT                   AS amount,
      created_by::VARCHAR             AS created_by,
      created_date::DATE              AS created_date,
      updated_by::VARCHAR             AS updated_by,
      updated_date::DATE              AS updated_date
    FROM {{ source('sheetload','manual_public_company_metrics') }}

)

SELECT *
FROM source
