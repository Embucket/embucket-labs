with base as (

    SELECT * 
    FROM {{ ref('sfdc_user_roles_source') }}

)

SELECT *
FROM base