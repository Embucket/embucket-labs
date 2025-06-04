WITH source AS (

  SELECT *
  FROM {{ source('xactly', 'xc_period_type') }}

),

renamed AS (

  SELECT

    period_type_id::FLOAT AS period_type_id,
    version::FLOAT AS version,
    name::VARCHAR AS name,
    is_active::VARCHAR AS is_active,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name

  FROM source

)

SELECT *
FROM renamed
