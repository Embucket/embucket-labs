WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'field_cleanup_suggestions') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed