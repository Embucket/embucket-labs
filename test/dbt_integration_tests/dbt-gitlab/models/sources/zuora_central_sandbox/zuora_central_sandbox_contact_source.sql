WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'contact') }}

), renamed AS(

    SELECT 
      id                  AS contact_id,
      -- keys
      account_id          AS account_id,


      -- contact info
      first_name          AS first_name,
      last_name           AS last_name,
      nick_name           AS nick_name,
      address_1           AS street_address,
      address_2           AS street_address2,
      county              AS county,
      state               AS state,
      postal_code         AS postal_code,
      city                AS city,
      country             AS country ,
      tax_region          AS tax_region,
      work_email          AS work_email,
      work_phone          AS work_phone,
      other_phone         AS other_phone,
      other_phone_type    AS other_phone_type,
      fax                 AS fax,
      home_phone          AS home_phone,
      mobile_phone        AS mobile_phone,
      personal_email      AS personal_email,
      description         AS description,


      -- metadata
      created_by_id       AS created_by_id,
      created_date        AS created_date,
      updated_by_id       AS updated_by_id,
      updated_date        AS updated_date,
      _FIVETRAN_DELETED   AS is_deleted

    FROM source

)

SELECT *
FROM renamed 
