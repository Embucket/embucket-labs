WITH base AS (

    SELECT *
    FROM {{ source('snapshots', 'dim_subscription_snapshot') }}
    
)

SELECT *
FROM base
