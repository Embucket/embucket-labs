WITH source AS (

  SELECT *
  FROM {{ source('xactly', 'xc_plan_assignment') }}

),

renamed AS (

  SELECT

    plan_assignment_id::FLOAT AS plan_assignment_id,
    version::FLOAT AS version,
    assignment_id::FLOAT AS assignment_id,
    assignment_type::FLOAT AS assignment_type,
    assignment_name::VARCHAR AS assignment_name,
    is_active::VARCHAR AS is_active,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name,
    plan_id::FLOAT AS plan_id,
    active_start_date::VARCHAR AS active_start_date,
    active_end_date::VARCHAR AS active_end_date

  FROM source

)

SELECT *
FROM renamed
