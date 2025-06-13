WITH source AS (

    SELECT
      id                AS email_to_visitor_id,
      email             AS email,
      visitor_id        AS visitor_id,
      modified_date     AS modified_date,
      created_date      AS created_date,
      is_ignore         AS is_ignore,
      is_deleted        AS is_deleted,
      _created_date     AS _created_date,
      _modified_date    AS _modified_date,
      _deleted_date     AS _deleted_date
    FROM {{ source('bizible', 'biz_email_to_visitor_ids') }}
 
)

SELECT *
FROM source

