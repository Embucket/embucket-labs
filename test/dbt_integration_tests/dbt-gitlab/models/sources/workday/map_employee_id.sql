WITH source AS (
  SELECT *
  FROM {{ ref('bamboohr_id_employee_number_mapping_source') }}
),

renamed AS (

  SELECT DISTINCT
    employee_id AS bhr_employee_id,
    employee_number AS wk_employee_id
  FROM source
  QUALIFY ROW_NUMBER() OVER(PARTITION BY employee_id ORDER BY uploaded_at DESC) = 1

)

SELECT *
FROM renamed