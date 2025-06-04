WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'quote') }}

), renamed AS (

    SELECT
      -- keys
      id                                AS quote_id,
      account_id__c                     AS account_id,
      opportunity_id__c                 AS opportunity_id,
      ownerid                           AS owner_id,
      zqu__zuoraaccountid__c            AS zuora_account_id,
      zqu__zuorasubscriptionid__c       AS zuora_subscription_id,
      zqu__BillToContact__c             AS bill_to_contact,
      zqu__SoldToContact__c             AS sold_to_contact,

      -- info
      zqu__startdate__c                 AS contract_effective_date,
      createddate                       AS created_date,
      zqu__primary__c                   AS is_primary_quote,
      lastmodifieddate                  AS last_modified_date,
      name                              AS name,
      quote_tcv__c                      AS quote_tcv,    
      zqu__status__c                    AS status,
      zqu__subscriptiontermstartdate__c AS term_start_date,
      zqu__subscriptiontermenddate__c   AS term_end_date,
      systemmodstamp,
      isdeleted                         AS is_deleted
    FROM source
)

SELECT *
FROM renamed