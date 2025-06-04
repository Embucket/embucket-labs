WITH source AS (

    SELECT
      id                   AS account_to_email_id,
      account_id           AS account_id,
      email                AS email,
      modified_date        AS modified_date,
      created_date         AS created_date,
      is_deleted           AS is_deleted,
      _created_date        AS _created_date,
      _modified_date       AS _modified_date,
      _deleted_date        AS _deleted_date
    FROM {{ source('bizible', 'biz_account_to_emails') }}
    
)

SELECT *
FROM source