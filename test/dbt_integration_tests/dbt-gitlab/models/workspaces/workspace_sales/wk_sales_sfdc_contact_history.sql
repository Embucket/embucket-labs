{{ config(alias='sfdc_contact_history') }}

WITH base AS (

    SELECT *
    FROM {{ ref('sfdc_contact_history_source') }}

)

SELECT *
FROM base
