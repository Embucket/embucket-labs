WITH source AS (

  SELECT *
  FROM {{ source('xactly', 'xc_planrules') }}

),

renamed AS (

  SELECT

    planrules_id::FLOAT AS planrules_id,
    version::FLOAT AS version,
    is_active::VARCHAR AS is_active,
    created_date::VARCHAR AS created_date,
    created_by_id::FLOAT AS created_by_id,
    created_by_name::VARCHAR AS created_by_name,
    modified_date::VARCHAR AS modified_date,
    modified_by_id::FLOAT AS modified_by_id,
    modified_by_name::VARCHAR AS modified_by_name,
    plan_id::FLOAT AS plan_id,
    rule_id::FLOAT as rule_id

  FROM source

)

SELECT *
FROM renamed
