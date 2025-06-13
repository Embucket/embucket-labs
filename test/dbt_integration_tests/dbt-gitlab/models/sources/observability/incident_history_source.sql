WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'incident_history') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed