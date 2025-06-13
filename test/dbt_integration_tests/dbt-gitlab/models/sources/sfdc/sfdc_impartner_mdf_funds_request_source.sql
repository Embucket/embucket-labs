WITH source AS (
    SELECT *
    FROM {{ source('salesforce', 'impartner_mdf_funds_request') }}
), renamed AS (
    SELECT
      -- ids
      id::VARCHAR                                        AS funds_request_id,
      name::VARCHAR                                      AS funds_request_name,

      -- info
      impartnermdf__activitylocationcity__c::VARCHAR     AS activity_location_city,
      impartnermdf__activitylocationcountry__c::VARCHAR  AS activity_location_country,
      impartnermdf__activitylocationstateprovince__c::VARCHAR AS activity_location_state_province,
      impartnermdf__activityname__c::VARCHAR             AS activity_name,
      impartnermdf__approvalpo__c::VARCHAR               AS approval_po,
      impartnermdf__approvedfundrequestamount__c::FLOAT  AS approved_fund_request_amount,
      impartnermdf__budgetedcost__c::FLOAT               AS budgeted_cost,
      impartnermdf__cancelmdfeventreason__c::VARCHAR     AS cancel_mdf_event_reason,
      impartnermdf__cancelmdfevent__c::BOOLEAN           AS cancel_mdf_event,
      impartnermdf__description__c::VARCHAR              AS description,
      impartnermdf__enddate__c::TIMESTAMP                AS end_date,
      impartnermdf__expectedresults__c::VARCHAR          AS expected_results,
      impartnermdf__notifypartner__c::BOOLEAN            AS notify_partner,
      impartnermdf__partneraccount__c::VARCHAR           AS partner_account,
      impartnermdf__partnercontact__c::VARCHAR           AS partner_contact,
      impartnermdf__partnerinvestment__c::FLOAT          AS partner_investment,
      impartnermdf__requestedamount__c::FLOAT            AS requested_amount,
      impartnermdf__resourcedetail__c::VARCHAR           AS resource_detail,
      impartnermdf__segmenttargettype__c::VARCHAR        AS segment_target_type,
      impartnermdf__startdate__c::TIMESTAMP              AS start_date,
      impartnermdf__status__c::VARCHAR                   AS status,
      impartnermdf__targetnumberofattendees__c::FLOAT    AS target_number_of_attendees,
      impartnermdf__typeofmarketingactivity__c::VARCHAR  AS type_of_marketing_activity,
      impartnermdf__typeofrequest__c::VARCHAR            AS type_of_request,
      impartnermdf__verticalindustrytargettype__c::VARCHAR AS vertical_industry_target_type,
      impartnermdf__willresourcesberequired__c::VARCHAR  AS will_resources_be_required,
      mdf_notes__c::VARCHAR                              AS mdf_notes,
      ownerid::VARCHAR                                   AS owner_id,

      -- metadata
      createdbyid::VARCHAR                               AS created_by_id,
      createddate::TIMESTAMP                             AS created_date,
      isdeleted::BOOLEAN                                 AS is_deleted,
      lastmodifiedbyid::VARCHAR                          AS last_modified_by_id,
      lastmodifieddate::TIMESTAMP                        AS last_modified_date,
      _sdc_received_at::TIMESTAMP                        AS sfdc_received_at,
      _sdc_extracted_at::TIMESTAMP                       AS sfdc_extracted_at,
      _sdc_table_version::NUMBER                         AS sfdc_table_version,
      _sdc_batched_at::TIMESTAMP                         AS sfdc_batched_at,
      _sdc_sequence::NUMBER                             AS sfdc_sequence,
      systemmodstamp::TIMESTAMP                          AS system_mod_stamp

    FROM source
)
SELECT *
FROM renamed
