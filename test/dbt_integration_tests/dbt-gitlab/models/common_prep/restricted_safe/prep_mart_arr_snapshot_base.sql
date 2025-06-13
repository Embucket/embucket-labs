WITH base AS (

    SELECT *
    FROM {{ ref('mart_arr_snapshot') }}
    
)

SELECT *
FROM base
