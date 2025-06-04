WITH source AS (

    SELECT
      id                   AS segment_id,
      name                 AS name,
      row_key              AS row_key,
      _created_date        AS _created_date,
      _modified_date       AS _modified_date,
      _deleted_date        AS _deleted_date
    FROM {{ source('bizible', 'biz_segments') }}
 
)

SELECT * 
FROM source