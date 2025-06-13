WITH base AS (

    SELECT *
    FROM {{ source('snapshots', 'mart_available_to_renew_snapshot') }}

)

SELECT *
FROM base
