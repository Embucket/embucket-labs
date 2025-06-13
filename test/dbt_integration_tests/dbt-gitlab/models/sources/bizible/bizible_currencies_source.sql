WITH source AS (

    SELECT
      id                    AS currency_id,
      is_corporate          AS is_corporate,
      is_enabled            AS is_enabled,
      modified_date         AS modified_date,
      modified_date_crm     AS modified_date_crm,
      created_date          AS created_date,
      created_date_crm      AS created_date_crm,
      iso_code              AS iso_code,
      iso_numeric           AS iso_numeric,
      exponent              AS exponent,
      name                  AS name,
      _created_date         AS _created_date,
      _modified_date        AS _modified_date,
      _deleted_date         AS _deleted_date
    FROM {{ source('bizible', 'biz_currencies') }}
 
)

SELECT *
FROM source

