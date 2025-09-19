{{ config({
    "alias": "gitlab_dotcom_members_snapshots"
    })
}}

WITH source AS (

	SELECT *
  FROM {{ source('snapshots', 'gitlab_dotcom_members_snapshots') }}

), renamed AS (

  SELECT
  
    dbt_scd_id::VARCHAR                            AS member_snapshot_id,
    id::NUMBER                                    AS member_id,
    access_level::NUMBER                          AS access_level,
    source_id::NUMBER                             AS source_id,
    source_type                                    AS member_source_type,
    user_id::NUMBER                               AS user_id,
    notification_level::NUMBER                    AS notification_level,
    type                                           AS member_type,
    created_at::TIMESTAMP                          AS invite_created_at,
    created_by_id::NUMBER                         AS created_by_id,
    invite_accepted_at::TIMESTAMP                  AS invite_accepted_at,
    requested_at::TIMESTAMP                        AS requested_at,
    expires_at::TIMESTAMP                          AS expires_at,
    ldap::BOOLEAN                                  AS has_ldap,
    override::BOOLEAN                              AS has_override,
    dbt_valid_from::TIMESTAMP                    AS valid_from,
    dbt_valid_to::TIMESTAMP                      AS valid_to

  FROM source

)

SELECT *
FROM renamed
