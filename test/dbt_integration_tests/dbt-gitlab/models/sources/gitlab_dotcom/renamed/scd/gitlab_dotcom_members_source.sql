{{ config({
        "materialized": "table"
        })
}}
    
WITH
{{ distinct_source(source=source('gitlab_dotcom', 'members'))}}

, renamed AS (

    SELECT

      id::NUMBER                                    AS member_id,
      access_level::NUMBER                          AS access_level,
      source_id::NUMBER                             AS source_id,
      source_type                                    AS member_source_type,
      user_id::NUMBER                               AS user_id,
      notification_level::NUMBER                    AS notification_level,
      type                                           AS member_type,
      created_at::TIMESTAMP                          AS invite_created_at,
      created_at::TIMESTAMP                          AS created_at,
      created_by_id::NUMBER                         AS created_by_id,
      invite_accepted_at::TIMESTAMP                  AS invite_accepted_at,
      requested_at::TIMESTAMP                        AS requested_at,
      expires_at::TIMESTAMP                          AS expires_at,
      ldap::BOOLEAN                                  AS has_ldap,
      override::BOOLEAN                              AS has_override,
      member_role_id,
      invite_token::VARCHAR AS invite_token,
      valid_from -- Column was added in distinct_source CTE

    FROM distinct_source

)

{{ scd_type_2(
    primary_key_renamed='member_id',
    primary_key_raw='id'
) }}
