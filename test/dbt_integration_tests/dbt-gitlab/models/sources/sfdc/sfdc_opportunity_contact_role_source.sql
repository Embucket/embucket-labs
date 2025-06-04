WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'opportunity_contact_role') }}

), renamed AS (

    SELECT

      -- keys

      id               AS opportunity_contact_role_id,
      contactid        AS contact_id,
      opportunityid    AS opportunity_id,
      createdbyid      AS created_by_id,
      lastmodifiedbyid AS last_modified_by_id, 

      -- info
      role             AS contact_role,
      isprimary        AS is_primary_contact,
      createddate      AS created_date,
      lastmodifieddate AS last_modified_date

    FROM source
)

SELECT *
FROM renamed