WITH source AS (

  SELECT *
  FROM {{ ref('test_stats_e2e_source') }}

)

SELECT *
FROM source
