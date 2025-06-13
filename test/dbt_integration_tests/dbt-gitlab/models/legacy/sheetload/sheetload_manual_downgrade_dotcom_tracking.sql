WITH source AS (

    SELECT *
    FROM {{ ref('sheetload_manual_downgrade_dotcom_tracking_source') }}

)

SELECT *
FROM source