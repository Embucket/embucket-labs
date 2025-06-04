WITH source AS (

    SELECT *
    FROM {{ source('snowflake','users') }}

), intermediate AS (

    SELECT
        name                                    AS user_name,
        created_on,
        login_name,
        display_name,
        first_name,
        last_name,
        email,
        comment,
        disabled                                AS is_disabled,
        default_warehouse,
        default_namespace,
        default_role,
        owner                                   AS owner_role,
        last_success_login,
        expires_at_time,
        locked_until_time,
        TO_TIMESTAMP_NTZ(_uploaded_at::NUMBER)  AS snapshot_date
    FROM source

)

SELECT *
FROM intermediate
