{{ simple_cte([
    ('bamboohr_locality','blended_employee_mapping_source'),
    ('locality','dim_locality')
]) }}

, final AS (

    SELECT 
      bamboohr_locality.employee_number,
      bamboohr_locality.employee_id,
      bamboohr_locality.uploaded_at::DATE AS updated_at,
      bamboohr_locality.locality          AS bamboo_locality,
      locality.location_factor
    FROM bamboohr_locality
    LEFT JOIN locality
      ON LOWER(bamboohr_locality.locality) = LOWER(locality.locality)
      AND DATE_TRUNC('day',bamboohr_locality.uploaded_at) >= locality.valid_from 
      AND DATE_TRUNC('day',bamboohr_locality.uploaded_at) < locality.valid_to
    WHERE bamboohr_locality.locality IS NOT NULL
    --1st time we started capturing locality
      AND bamboohr_locality.uploaded_at >= '2020-03-24'
    

)

SELECT *
FROM final