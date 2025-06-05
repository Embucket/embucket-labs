
  
    

create or replace transient table EMBUCKET.workday.workday_hcm_worker_source
    

    
    as (WITH source AS (

  SELECT *
  FROM "EMBUCKET".workday_hcm.worker
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    id::VARCHAR                             AS id,
    user_id::VARCHAR                        AS employee_id,
    compensation_grade_id::VARCHAR          AS compensation_grade_id,
    compensation_grade_profile_id::VARCHAR  AS compensation_grade_profile_id
  FROM source

)

SELECT *
FROM final
    )
;


  