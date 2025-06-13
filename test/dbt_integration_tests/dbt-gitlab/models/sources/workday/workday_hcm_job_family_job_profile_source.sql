WITH source AS (

  SELECT *
  FROM {{ source('workday_hcm','job_family_job_profile') }}
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
