{{ config(alias='date_details') }}

WITH date_details AS (

    SELECT
      *,
        -- beggining of the week
      is_first_day_of_fiscal_quarter_week                                   AS is_first_day_of_fiscal_quarter_week_flag,
      fiscal_quarter_number_absolute                                        AS quarter_number

    FROM {{ ref('dim_date') }} 
    ORDER BY 1 DESC

)

SELECT *
FROM date_details