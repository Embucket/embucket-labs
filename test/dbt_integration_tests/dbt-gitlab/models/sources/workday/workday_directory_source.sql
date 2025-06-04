WITH source AS (
  SELECT *
  FROM {{ source('snapshots','workday_employee_directory_snapshots') }}
  
),

renamed AS (

  SELECT
    employee_id::NUMBER AS employee_id,
    work_email::VARCHAR AS work_email,
    full_name::VARCHAR AS full_name,
    job_title::VARCHAR AS job_title,
    supervisor::VARCHAR AS supervisor,
    _fivetran_synced::TIMESTAMP AS uploaded_at,
    dbt_valid_from::TIMESTAMP AS valid_from,
    dbt_valid_to::TIMESTAMP AS valid_to,
    IFF(dbt_valid_to IS NULL,TRUE,FALSE) AS is_current
  FROM source

)

SELECT *
FROM renamed
