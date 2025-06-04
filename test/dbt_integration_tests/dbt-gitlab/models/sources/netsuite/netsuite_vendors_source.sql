WITH source AS (

    SELECT *
    FROM {{ source('netsuite', 'vendors') }}

), renamed AS (

    SELECT
      --Primary Key
      vendor_id::FLOAT                   AS vendor_id,

      --Foreign Key
      represents_subsidiary_id::FLOAT    AS subsidiary_id,
      currency_id::FLOAT                 AS currency_id,

      --Info
      companyname::VARCHAR               AS vendor_name,
      openbalance::FLOAT                 AS vendor_balance,
      comments::VARCHAR                  AS vendor_comments,

      --Meta
      true::BOOLEAN            AS is_1099_eligible,
      true::BOOLEAN                AS is_inactive,
      true::BOOLEAN                 AS is_person

    FROM source
    WHERE LOWER(_fivetran_deleted) = 'false'

)

SELECT *
FROM renamed

--We no longer have first and last names for folks who are paid by contracts.
