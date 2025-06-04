WITH source AS (

  SELECT *
  FROM {{ ref('all_fabrication_stats_source') }}

)

SELECT *
FROM source
