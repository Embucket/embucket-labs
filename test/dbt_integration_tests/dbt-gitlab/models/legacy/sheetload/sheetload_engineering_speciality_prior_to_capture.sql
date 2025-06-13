WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_engineering_speciality_prior_to_capture_source') }}

)

SELECT 
  employee_id, 
  speciality, 
  start_date                 AS speciality_start_date,
  DATEADD('day',-1,end_date) AS speciality_end_date
FROM source