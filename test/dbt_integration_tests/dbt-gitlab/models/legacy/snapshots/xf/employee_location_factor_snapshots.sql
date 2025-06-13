{{ config({
    "schema": "sensitive",
    "database": env_var('SNOWFLAKE_PREP_DATABASE'),
    "materialized": "table"
    })
}}

{% set max_date_in_analysis = "date_trunc('week', dateadd(week, 3, CURRENT_DATE))" %}

WITH employee_locality AS (

  SELECT *
  FROM {{ ref('employee_locality') }}

),

source AS (


  SELECT
    employee_number,
    bamboo_locality,
    location_factor,
    updated_at                                                               AS valid_from,
    LEAD(updated_at) OVER (PARTITION BY employee_number ORDER BY updated_at) AS valid_to
  FROM employee_locality

),

intermediate AS (

  SELECT
    employee_number                                                        AS bamboo_employee_number,
    bamboo_locality                                                        AS locality,
    location_factor,
    LEAD(
      location_factor) OVER
    (PARTITION BY bamboo_employee_number ORDER BY valid_from)              AS next_location_factor,
    valid_from,
    COALESCE(valid_to, {{ max_date_in_analysis }}) AS valid_to
  FROM source

),

deduplicated AS (

  SELECT *
  FROM intermediate
  QUALIFY ROW_NUMBER() OVER (PARTITION BY bamboo_employee_number, locality, location_factor, next_location_factor ORDER BY valid_from) = 1

),

final AS (

  SELECT
    bamboo_employee_number,
    locality,
    location_factor,
    valid_from,
    COALESCE(
      LEAD(DATEADD(DAY, -1, valid_from))
        OVER (PARTITION BY bamboo_employee_number ORDER BY valid_from),
      {{ max_date_in_analysis }}
    ) AS valid_to
  FROM deduplicated
  GROUP BY 1, 2, 3, 4

)

SELECT *
FROM final
