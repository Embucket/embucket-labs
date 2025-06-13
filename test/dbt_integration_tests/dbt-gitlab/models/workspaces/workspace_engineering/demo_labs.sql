WITH source AS (

  SELECT *
  FROM {{ ref('labs_source') }}

)

SELECT *
FROM source