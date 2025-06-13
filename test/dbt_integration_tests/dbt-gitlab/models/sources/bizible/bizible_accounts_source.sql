WITH source AS (

    SELECT
      id                      AS account_id,
      created_date            AS created_date,
      modified_date           AS modified_date,
      name                    AS name,
      web_site                AS web_site,
      industry                AS industry,
      country                 AS country,
      engagement_rating       AS engagement_rating,
      engagement_score        AS engagement_score,
      domain                  AS domain,
      is_deleted              AS is_deleted,
      custom_properties       AS custom_properties,
      _created_date           AS _created_date,
      _modified_date          AS _modified_date,
      _deleted_date           AS _deleted_date      
    FROM {{ source('bizible', 'biz_accounts') }}
 
)

SELECT *
FROM source