
WITH source AS (
  SELECT *
  FROM {{ source('sheetload', 'devrel_influenced_campaigns') }}
), renamed AS (
  SELECT 
    campaign_name::VARCHAR  as campaign_name,
    campaign_type::VARCHAR  as campaign_type,
    description::VARCHAR    as description,
    influence_type::VARCHAR as influence_type,
    url::VARCHAR  as url,
    dri::VARCHAR  as dri

  FROM source
)
SELECT *
FROM renamed
WHERE campaign_name IS NOT NULL