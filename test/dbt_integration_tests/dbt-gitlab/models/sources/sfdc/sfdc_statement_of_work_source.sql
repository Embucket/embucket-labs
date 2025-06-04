WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'statement_of_work') }}

), renamed AS (

    SELECT
    -- keys
      id                                 AS ps_engagement_id,
      opportunity__c                     AS opportunity_id,
      owner__c                           AS owner_id,

    -- dates
      completed_date__c                  AS completed_date,
      kick_off_date__c                   AS kick_off_date,
      go_live_date__c                    AS go_live_date,

    -- info
      name                               AS ps_engagement_name,
      percentcomplete__c                 AS percent_complete,
      signed_acceptance_from_customer__c AS signed_acceptance_from_customer,
      status__c                          AS status,

    -- metadata
      createdbyid                        AS created_by_id,
      lastmodifiedbyid                   AS last_modified_by_id,
      createddate                        AS created_date,
      lastmodifieddate                   AS last_modified_date,
      isdeleted                          AS is_deleted

    FROM source

)

SELECT *
FROM renamed
