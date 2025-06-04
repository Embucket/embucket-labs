WITH source AS (

    SELECT
      id              AS conversion_rate_id,
      currency_id     AS currency_id,
      source_iso_code AS source_iso_code,
      start_date      AS start_date,
      end_date        AS end_date,
      conversion_rate AS conversion_rate,
      is_current      AS is_current,
      created_date    AS created_date,
      modified_date   AS modified_date,
      is_deleted      AS is_deleted,
      _created_date   AS _created_date,
      _modified_date  AS _modified_date,
      _deleted_date   AS _deleted_date

    FROM {{ source('bizible', 'biz_conversion_rates') }}
 
    )

SELECT *
FROM source