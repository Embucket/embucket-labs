WITH source AS (

    SELECT *
    FROM {{ source('snowflake','roles') }}

), intermediate AS (

    SELECT
      name                                      AS role_name,      
      created_on,
      is_default,
      is_current,
      is_inherited,
      assigned_to_users,
      granted_to_roles,
      granted_roles,
      owner                                     AS owner_role,
      comment,
      TO_TIMESTAMP_NTZ(_uploaded_at::NUMBER)    AS snapshot_date
    FROM source

)

SELECT *
FROM intermediate
