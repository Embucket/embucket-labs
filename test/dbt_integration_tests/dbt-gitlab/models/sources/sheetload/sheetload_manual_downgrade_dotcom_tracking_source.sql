WITH source AS (

    SELECT *
    FROM {{ source('sheetload', 'manual_downgrade_dotcom_tracking') }}

), renamed AS (

    SELECT
      namespace_id::NUMBER         AS namespace_id,
      TRY_TO_DATE(downgraded_date) AS downgraded_date
    FROM source  

)

SELECT *
FROM renamed
