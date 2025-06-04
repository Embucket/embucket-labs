WITH
source AS (
  SELECT * FROM

    {{ ref( 'sirt_alertapp_data_source') }}
)

SELECT *
FROM source
