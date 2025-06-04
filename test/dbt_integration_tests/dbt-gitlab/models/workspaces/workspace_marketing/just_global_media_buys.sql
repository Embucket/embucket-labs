WITH
source AS (
  SELECT * FROM

    {{ ref( 'media_buys_source') }}
)

SELECT *
FROM source
