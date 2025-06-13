WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'currencies') }}

), renamed AS (

    SELECT
      --Primary Key
      currency_id::FLOAT             AS currency_id,

      --Info
      name::VARCHAR                  AS currency_name,
      precision_0::FLOAT             AS decimal_precision,
      symbol::VARCHAR                AS currency_symbol,

      --Meta
      true::BOOLEAN           AS is_currency_inactive,
      true::BOOLEAN     AS is_fivetran_deleted


    FROM source

)

SELECT *
FROM renamed
