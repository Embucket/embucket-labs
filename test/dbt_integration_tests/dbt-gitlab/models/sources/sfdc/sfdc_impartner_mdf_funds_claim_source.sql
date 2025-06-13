WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'impartner_mdf_funds_claim') }}

), renamed AS (
    SELECT
      -- ids
      id::VARCHAR                                 AS funds_claim_id,
      name::VARCHAR                               AS funds_claim_name,

      -- info
      impartnermdf__claimamount__c::FLOAT         AS claim_amount,
      impartnermdf__fundsrequest__c::VARCHAR      AS funds_request,
      impartnermdf__invoicenumber__c::VARCHAR     AS invoice_number,
      impartnermdf__notifypartner__c::BOOLEAN     AS notify_partner,
      impartnermdf__paiddate__c::TIMESTAMP        AS paid_date,
      impartnermdf__partneraccount__c::VARCHAR    AS partner_account,
      impartnermdf__partnercontact__c::VARCHAR    AS partner_contact,
      impartnermdf__paymentamount__c::FLOAT       AS payment_amount,
      impartnermdf__paymentmethod__c::VARCHAR     AS payment_method,
      impartnermdf__performanceverified__c::BOOLEAN AS performance_verified,
      impartnermdf__ponumber__c::VARCHAR           AS po_number,
      impartnermdf__popreceived__c::BOOLEAN        AS po_received,
      impartnermdf__rejectionreason__c::VARCHAR    AS rejection_reason,
      impartnermdf__status__c::VARCHAR             AS status,

      -- metadata
      createdbyid::VARCHAR                         AS created_by_id,
      createddate::TIMESTAMP                       AS created_date,
      isdeleted::BOOLEAN                           AS is_deleted,
      lastmodifiedbyid::VARCHAR                    AS last_modified_by_id,
      lastmodifieddate::TIMESTAMP                  AS last_modified_date,
      _sdc_received_at::TIMESTAMP                  AS sfdc_received_at,
      _sdc_extracted_at::TIMESTAMP                 AS sfdc_extracted_at,
      _sdc_table_version::NUMBER                   AS sfdc_table_version,
      _sdc_batched_at::TIMESTAMP                   AS sfdc_batched_at,
      _sdc_sequence::NUMBER                        AS sfdc_sequence,
      systemmodstamp::TIMESTAMP                    AS system_mod_stamp

    FROM source
)
SELECT *
FROM renamed
