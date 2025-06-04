WITH source AS (

  SELECT *
  FROM {{ ref('all_test_stats_source') }}

)

SELECT *
FROM source
