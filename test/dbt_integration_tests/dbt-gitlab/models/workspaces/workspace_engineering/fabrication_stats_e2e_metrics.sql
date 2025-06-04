WITH source AS (

  SELECT *
  FROM {{ ref('fabrication_stats_source') }}

)

SELECT *
FROM source
