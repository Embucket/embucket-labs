WITH source AS (

    SELECT *
    FROM {{ source('snowflake','grants_to_role') }}

), intermediate AS (

    SELECT
      name                                      AS role_name,
      created_on,
      privilege,
      granted_on,
      granted_to                                AS granted_to_type,
      grantee_name                              AS grantee_user_name,
      grant_option,
      granted_by,
      TO_TIMESTAMP_NTZ(_uploaded_at::NUMBER)    AS snapshot_date
    FROM source

)

SELECT *
FROM intermediate
