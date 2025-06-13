WITH source AS (

  SELECT *
  FROM {{ ref('main_test_stats_source') }}

)

SELECT *
FROM source
