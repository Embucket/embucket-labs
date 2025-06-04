WITH source AS (

  SELECT *
  FROM {{ ref('main_test_stats_e2e_source') }}

)

SELECT *
FROM source
