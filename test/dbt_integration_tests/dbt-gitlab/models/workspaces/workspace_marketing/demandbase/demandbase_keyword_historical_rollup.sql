WITH source AS (

    SELECT *
    FROM {{ ref('demandbase_keyword_historical_rollup_source') }}

)

SELECT *
FROM source