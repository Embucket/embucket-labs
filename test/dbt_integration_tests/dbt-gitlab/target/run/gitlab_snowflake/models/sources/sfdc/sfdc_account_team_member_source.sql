
  
    

create or replace transient table EMBUCKET.sfdc.sfdc_account_team_member_source
    

    
    as (WITH source AS (

  SELECT *
  FROM EMBUCKET.salesforce_v2_stitch.accountteammember

),

renamed AS (

  SELECT

    --keys
    id                     AS account_team_member_id,
    accountid              AS account_id,
    userid                 AS user_id,

    --info 
    accountaccesslevel     AS account_access_level,
    caseaccesslevel        AS case_access_level,
    contactaccesslevel     AS contact_access_level,
    opportunityaccesslevel AS opportunity_access_level,
    isdeleted              AS is_deleted

  FROM source
)


SELECT *
FROM renamed
    )
;


  