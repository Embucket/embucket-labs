{{ config(
    tags=["mnpi"]
) }}

WITH source AS ( 

    SELECT *
    FROM {{ source('salesforce', 'opportunity_history') }}

), renamed AS (
    SELECT
      opportunityid         AS opportunity_id,
      id                    AS opportunity_history_id,

      createddate           AS field_modified_at,
      createdbyid           AS created_by_id,
      createddate           AS created_date,
      closedate             AS close_date,
      forecastcategory      AS forecast_category,
      probability           AS probability,

      isdeleted             AS is_deleted,
      amount                AS amount,

      expectedrevenue       AS expected_revenue,
      stagename             AS stage_name,
      systemmodstamp        AS systemmodstamp
    FROM source

)    

SELECT *
FROM renamed
