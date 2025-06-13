WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'monitors') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed