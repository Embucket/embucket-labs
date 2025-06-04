WITH base AS (

    SELECT * 
    FROM {{ source('salesforce_sandbox', 'user_role') }}

), renamed AS (

    SELECT
      id                    AS user_role_id,
      name                  AS user_role_name,

      --metadata
      lastmodifiedbyid      AS last_modified_id,
      lastmodifieddate      AS last_modified_date,
      systemmodstamp

    FROM base
)

SELECT *
FROM renamed
