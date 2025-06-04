WITH base AS (

    SELECT *
    FROM {{ ref('prep_app_release') }}

)

SELECT *
FROM base
