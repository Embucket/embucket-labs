WITH source AS (

    SELECT
      id                    AS url_id,
      scheme                AS scheme,
      host                  AS host,
      port                  AS port,
      path                  AS path,
      row_key               AS row_key,
      _created_date         AS _created_date,
      _modified_date        AS _modified_date,
      _deleted_date         AS _deleted_date
    FROM {{ source('bizible', 'biz_urls') }}
 
)

SELECT *
FROM source