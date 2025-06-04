WITH base AS (

    SELECT *
    FROM {{ ref('prep_app_release_major_minor') }}

)

SELECT *
FROM base
