WITH source AS (

    SELECT *
    FROM {{ source('zuora_api_sandbox', 'contact') }}

), renamed AS(

    SELECT 
      id                  AS contact_id,
      -- keys
      accountid           AS account_id,


      -- contact info
      firstname           AS first_name,
      lastname            AS last_name,
      nickname,
      address1            AS street_address,
      address2            AS street_address2,
      county,
      state,
      postalcode          AS postal_code,
      city,
      country,
      taxregion           AS tax_region,
      workemail           AS work_email,
      workphone           AS work_phone,
      otherphone          AS other_phone,
      otherphonetype      AS other_phone_type,
      fax,
      homephone           AS home_phone,
      mobilephone         AS mobile_phone,
      personalemail       AS personal_email,
      description,


      -- metadata
      createdbyid         AS created_by_id,
      createddate         AS created_date,
      updatedbyid         AS updated_by_id,
      updateddate         AS updated_date,
      deleted             AS is_deleted

    FROM source

)

SELECT *
FROM renamed 
