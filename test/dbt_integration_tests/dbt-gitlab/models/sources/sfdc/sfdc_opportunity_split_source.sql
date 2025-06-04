{{config({
    "materialized": "table",
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
  })
}}

WITH source AS (

    SELECT
      *
    FROM {{ source('salesforce', 'opportunity_split') }}

), renamed AS (

      SELECT
        id::VARCHAR                                                AS opportunity_split_id,
        opportunityid::VARCHAR                                     AS opportunity_id,
        createdbyid::VARCHAR                                       AS created_by_id,
        createddate::TIMESTAMP                                     AS created_date,
        lastmodifiedbyid::VARCHAR                                  AS last_modified_by_id,
        lastmodifieddate::TIMESTAMP                                AS last_modified_date,
        employee_number__c::VARCHAR                                AS employee_number,
        isdeleted::BOOLEAN                                         AS is_deleted,
        opp_owner_different__c::BOOLEAN                            AS is_opportunity_owner_different,
        split::NUMBER                                              AS split,
        split_id__c::VARCHAR                                       AS split_id,
        split_owner_role__c::VARCHAR                               AS split_owner_role,
        splitamount::NUMBER                                        AS split_amount,
        splitnote::VARCHAR                                         AS split_note,
        splitownerid::VARCHAR                                      AS split_owner_id,
        splitpercentage::NUMBER                                    AS split_percentage,
        splittypeid::VARCHAR                                       AS split_type_id,
        team__c::VARCHAR                                           AS team,
        user_role__c::VARCHAR                                      AS user_role,
        systemmodstamp::TIMESTAMP                                  AS system_mod_timestamp
      FROM source
  )

SELECT *
FROM renamed
