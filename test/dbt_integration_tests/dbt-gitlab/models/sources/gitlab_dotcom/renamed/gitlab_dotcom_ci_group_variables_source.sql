WITH source AS (

  SELECT *
  FROM {{ ref('gitlab_dotcom_ci_group_variables_dedupe_source') }}
  
), renamed AS (

  SELECT 
    id::NUMBER              AS ci_group_variable_id,
    group_id::NUMBER        AS ci_group_variable_group_id, 
    created_at::TIMESTAMP   AS created_at, 
    updated_at::TIMESTAMP   AS updated_at, 
    masked                  AS masked, 
    variable_type           AS variable_variable_type 
  FROM source

)


SELECT *
FROM renamed
