WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'vartopia_drs_registration') }}

), renamed AS (
    SELECT
      ID::VARCHAR                                  AS registration_id,
      VartopiaDRS__Vendor_Deal_ID__c::VARCHAR      AS deal_id,
      VartopiaDRS__Opportunity__c::VARCHAR         AS linked_opportunity_id,
      Name::VARCHAR                                AS registration_name,
      VARTOPIADRS__DEAL_NAME__C::VARCHAR           AS deal_registration_name,
      CAM_Approval_Status__c::VARCHAR              AS partner_manager_approval_status,
      CAM_Denial_Reason__c::VARCHAR                   AS partner_manager_denial_reason,
      VartopiaDRS__Approved_Date__c::VARCHAR       AS deal_registration_approval_date,
      VartopiaDRS__Picklist_1__c::VARCHAR          AS deal_registration_type,
      VartopiaDRS__MDF_Campaign__c::VARCHAR        AS gitlab_marketing_campaign,
      VartopiaDRS__Campaign_1__c::VARCHAR          AS distributor_marketing_campaign,
      VARTOPIADRS__DISTRIBUTOR_ACCOUNT__C::VARCHAR AS distributor_account_id,
      VARTOPIADRS__SUBMITTER_TYPE__C::VARCHAR      AS deal_submitter_type,

      -- metadata
      createdbyid::VARCHAR                         AS created_by_id,
      createddate::TIMESTAMP                       AS created_date,
      isdeleted::BOOLEAN                           AS is_deleted,
      lastmodifiedbyid::VARCHAR                    AS last_modified_by_id,
      lastmodifieddate::TIMESTAMP                  AS last_modified_date,
      systemmodstamp::TIMESTAMP                    AS system_mod_stamp

    FROM source
)
SELECT *
FROM renamed
