WITH source AS (

  SELECT *
  FROM {{ source('workday_hcm','job_family') }}
  WHERE NOT _fivetran_deleted

),

final AS (

  SELECT
    id::VARCHAR                             AS id,
    summary::VARCHAR                        AS summary,
    IFF(inactive::BOOLEAN = 0, TRUE, FALSE) AS is_job_family_active,
    effective_date::DATE                    AS effective_date,
    job_family_code::VARCHAR                AS job_family_code
  FROM source

)

SELECT *
FROM final
