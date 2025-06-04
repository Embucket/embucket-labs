WITH base AS (

    SELECT *
    FROM {{ source('snapshots', 'fct_mrr_snapshot') }}
    
)

SELECT *
FROM base
