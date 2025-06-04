WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'executive_business_review') }}



), renamed AS (

    SELECT
         id                            AS ebr_id,
         name                          AS ebr_name,
         ebr_date__c :: date           AS ebr_date,
        --keys
         ebr_account__c                AS account_id,
         ownerid                       AS owner_id,
        --info
         ebr_quarter__c                AS ebr_quarter,
         ebr_number__c                 AS ebr_number,
         ebr_outcome__c                AS ebr_outcome,
         ebr_summary__c                AS ebr_summary,
         ebr_status__c                 AS ebr_status,
         ebr_notes__c                  AS ebr_notes,
         first_date_success_updated__c AS first_date_success_updated,
         ebr_action_items_takeaways__c AS ebr_action_items_takeaways,
         ebr_success__c                AS ebr_success,
        --metadata
         lAStmodifiedbyid              AS last_modified_by_id,
         createdbyid                   AS created_by_id,
         isdeleted                     AS is_deleted

    FROM source
)

SELECT *
FROM renamed
