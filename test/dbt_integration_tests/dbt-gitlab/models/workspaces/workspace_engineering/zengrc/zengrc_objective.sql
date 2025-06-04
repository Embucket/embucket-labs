WITH source AS (

    SELECT *
    FROM {{ ref('zengrc_objective_source') }}

)

SELECT *
FROM source