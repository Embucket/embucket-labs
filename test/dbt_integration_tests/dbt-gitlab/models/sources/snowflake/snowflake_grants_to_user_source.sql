WITH source AS (

    SELECT *
    FROM {{ source('snowflake','grants_to_user') }}

), intermediate AS (

    SELECT
        role                                    AS role_name,
        granted_to                              AS granted_to_type,
        grantee_name,
        TO_TIMESTAMP_NTZ(_uploaded_at::NUMBER)  AS snapshot_date,
        created_on
    FROM source

)

SELECT *
FROM intermediate
