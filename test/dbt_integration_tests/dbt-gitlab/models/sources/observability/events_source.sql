WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'dup_prod_insights_events') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed