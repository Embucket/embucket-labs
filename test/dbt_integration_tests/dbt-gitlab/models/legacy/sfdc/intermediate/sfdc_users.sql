with base as (

    SELECT *
    FROM {{ ref('sfdc_users_source') }}

)

SELECT *
FROM base