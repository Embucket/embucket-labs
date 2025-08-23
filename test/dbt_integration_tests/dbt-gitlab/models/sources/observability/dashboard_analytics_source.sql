WITH source AS (

    SELECT *
    FROM {{ source('monte_carlo_prod_insights', 'dup_dashboard_analytics') }}

), renamed AS (

    SELECT
     *
    FROM source

)

SELECT *
FROM renamed