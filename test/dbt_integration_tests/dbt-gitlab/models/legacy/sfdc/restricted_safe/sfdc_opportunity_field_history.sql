WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_field_history_source') }}

)

SELECT *
FROM base