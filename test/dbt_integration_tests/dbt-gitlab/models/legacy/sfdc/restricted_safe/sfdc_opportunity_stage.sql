
WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_opportunity_stage_source') }}

)

SELECT *
FROM base