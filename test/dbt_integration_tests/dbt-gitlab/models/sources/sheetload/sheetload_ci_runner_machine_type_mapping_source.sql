WITH source AS (

  SELECT * 
  FROM {{ source('sheetload','ci_runner_machine_type_mapping') }}

)

SELECT * 
FROM source
