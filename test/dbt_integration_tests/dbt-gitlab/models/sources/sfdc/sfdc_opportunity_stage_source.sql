{{ config(
    tags=["mnpi"]
) }}

WITH source AS ( 

    SELECT *
    FROM {{ source('salesforce', 'opportunity_stage') }}

), renamed AS (

    SELECT
      id                   AS sfdc_id,
      masterlabel          AS primary_label,
      defaultprobability   AS default_probability,
      forecastcategoryname AS forecast_category_name,
      isactive             AS is_active,
      isclosed             AS is_closed,
      iswon                AS is_won
    FROM source

)    

SELECT *
FROM renamed

