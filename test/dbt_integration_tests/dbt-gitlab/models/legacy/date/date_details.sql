WITH source AS (

    SELECT *
    FROM {{ ref('dim_date')}}

)

SELECT *
FROM source
