
  
    

create or replace transient table EMBUCKET.workday.workday_hcm_job_family_job_profile_source
    

    
    as (WITH source AS (

  SELECT *
  FROM "EMBUCKET".workday_hcm.job_family_job_profile
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    job_profile_id::VARCHAR AS job_profile_id,
    job_family_id::VARCHAR  AS job_family_id
  FROM source

)

SELECT *
FROM final
    )
;


  