WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'partner_discount_summary') }}

), renamed AS (

    SELECT
      fy::VARCHAR                     AS fiscal_year_name_fy,
      opportunity_id::VARCHAR         AS dim_crm_opportunity_id,
      close_date::VARCHAR             AS close_date,
      lessor_of_discounts::FLOAT      AS discount_percent,
      partner_type::VARCHAR           AS partner_type
    FROM source

)

SELECT *
FROM renamed
