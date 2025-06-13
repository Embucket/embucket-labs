WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'bizible_person') }}

), deduped AS (

    SELECT 
      base.id                              AS person_id,
      CASE
        WHEN base.bizible2__lead__c IS NULL THEN contact.bizible2__lead__c
        ELSE base.bizible2__lead__c
      END                                  AS bizible_lead_id,
      base.bizible2__lead__c               AS base_lead_id,
      contact.bizible2__lead__c            AS contact_lead_id,
      base.bizible2__contact__c            AS bizible_contact_id,
      base.isdeleted::BOOLEAN              AS is_deleted
      
    FROM source AS base
    LEFT JOIN source AS contact
        ON base.bizible2__contact__c = contact.bizible2__contact__c
    WHERE is_deleted = 'FALSE' 
    QUALIFY (ROW_NUMBER() OVER(PARTITION BY bizible_contact_id ORDER BY base.lastmodifieddate DESC, base.createddate DESC) = 1
        OR ROW_NUMBER() OVER(PARTITION BY bizible_lead_id ORDER BY base.lastmodifieddate DESC, base.createddate DESC) = 1 )

), final AS (

    SELECT
      person_id,
      bizible_lead_id,
      bizible_contact_id,
      is_deleted
      
    FROM deduped
)

SELECT *
FROM final
