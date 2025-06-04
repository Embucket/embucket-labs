WITH source AS (

  SELECT *
  FROM {{ ref('sheetload_leading_organization_entry_dates_source') }}

)

SELECT *
FROM source
