{{config({
    "materialized": "table",
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
  })
}}

WITH source AS (

    SELECT
      *
    FROM {{ source('salesforce', 'opportunity_team_member') }}

), renamed AS (

      SELECT
        id::VARCHAR                                                     AS opportunity_team_member_id,
        createdbyid::VARCHAR                                            AS created_by_id,
        createddate::TIMESTAMP                                          AS created_date,
        employee_number__c::VARCHAR                                     AS employee_number,
        isdeleted::BOOLEAN                                              AS is_deleted,
        lastmodifiedbyid::VARCHAR                                       AS last_modified_by_id,
        lastmodifieddate::TIMESTAMP                                     AS last_modified_date,
        name::VARCHAR                                                   AS name,
        opportunityaccesslevel::VARCHAR                                 AS opportunity_access_level,
        opportunityid::VARCHAR                                          AS opportunity_id,
        photourl::VARCHAR                                               AS photo_url,
        title::VARCHAR                                                  AS title,
        teammemberrole::VARCHAR                                         AS team_member_role,
        userid::VARCHAR                                                 AS user_id,
        systemmodstamp::TIMESTAMP                                       AS system_mod_timestamp
      FROM source
  )

SELECT *
FROM renamed
