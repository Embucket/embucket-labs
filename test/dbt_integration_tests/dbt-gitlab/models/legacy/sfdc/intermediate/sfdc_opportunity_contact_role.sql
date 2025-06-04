WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_contact_role_source') }}

)

SELECT *
FROM base