{{config({
    "schema": "legacy",
    "database": env_var('SNOWFLAKE_PROD_DATABASE'),
  })
}}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'proof_of_concept') }}

), renamed AS (

    SELECT
    -- keys
      id                                        AS pov_id,
      account__c                                AS account_id,
      opportunity__c                            AS opportunity_id,
      poc_owner__c                              AS pov_owner_id,
      solutions_architect__c                    AS solutions_architect_id,
      technical_account_manager__c              AS technical_account_manager_id,    

    -- dates
      poc_start_date__c                         AS pov_start_date,
      poc_close_date__c                         AS pov_close_date,

    -- info
      name                                      AS pov_name,
      decline_reason__c                         AS reason_for_decline,
      general_notes__c                          AS general_notes,
      poc_length__c                             AS pov_length,
      poc_milestone_in_collaboration_project__c AS link_to_gitlab_milestone,
      poc_type__c                               AS pov_type,
      result__c                                 AS pov_result,
      status__c                                 AS pov_status,
      success_criteria__c                       AS success_criteria,
      unsuccessful_reason__c                    AS unsuccessful_reason,
      sa_asm_leader_approved__c                 AS is_sa_asm_leader_approved,

    -- metadata
      createdbyid                               AS created_by_id,
      createddate                               AS created_date,
      lastmodifiedbyid                          AS last_modified_by_id,
      lastmodifieddate                          AS last_modified_date,
      isdeleted::BOOLEAN                        AS is_deleted

    FROM source

)

SELECT *
FROM renamed
