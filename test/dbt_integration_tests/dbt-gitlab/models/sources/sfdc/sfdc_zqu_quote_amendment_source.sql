{{ config(
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'zqu_quote_amendment') }}

), renamed AS (

    SELECT
      id::VARCHAR                             AS zqu_quote_amendment_id,

      charge_summary_sub_total__c::FLOAT      AS charge_summary_sub_total,
      license_amount__c::FLOAT                AS license_amount,
      NAME::VARCHAR                           AS zqu_quote_amendment_name,
      professional_services_amount__c::FLOAT  AS professional_services_amount,
      true_up_amount__c::FLOAT                AS true_up_amount,
      zqu__autorenew__c::VARCHAR              AS zqu__auto_renew,
      zqu__cancellationpolicy__c::VARCHAR     AS zqu__cancellation_policy,
      zqu__deltamrr__c::FLOAT                 AS zqu__delta_mrr,
      zqu__deltatcv__c::FLOAT                 AS zqu__delta_tcv,
      zqu__initialtermperiodtype__c::VARCHAR  AS zqu__initial_term_period_type,
      zqu__initialterm__c::FLOAT              AS zqu__initial_term,
      zqu__quoteamendmentzuoraid__c::VARCHAR  AS zqu__quote_amendment_zuora_id,
      zqu__renewaltermperiodtype__c::VARCHAR  AS zqu__renewal_term_period_type,
      zqu__renewalterm__c::FLOAT              AS zqu__renewal_term,
      zqu__status__c::VARCHAR                 AS zqu__status,
      zqu__totalamount__c::FLOAT              AS zqu__total_amount,
      zqu__quote__c::VARCHAR                  AS zqu__quote,
      zqu__type__c::VARCHAR                   AS zqu__type,
      zqu__description__c::VARCHAR            AS zqu__description,
      zqu__termstartdate__c::TIMESTAMP_TZ     AS zqu__term_start_date,


      --metadata
      createdbyid::VARCHAR                    AS created_by_id,
      createddate::TIMESTAMP_TZ               AS created_date,
      isdeleted::BOOLEAN                      AS is_deleted,
      lastmodifiedbyid::VARCHAR               AS last_modified_by_id,
      lastmodifieddate::TIMESTAMP_TZ          AS last_modified_date,
      _sdc_received_at::TIMESTAMP_TZ          AS sdc_received_at,
      _sdc_extracted_at::TIMESTAMP_TZ         AS sdc_extracted_at,
      _sdc_table_version::NUMBER              AS sdc_table_version,
      _sdc_batched_at::TIMESTAMP_TZ           AS sdc_batched_at,
      _sdc_sequence::NUMBER                   AS sdc_sequence,
      systemmodstamp::TIMESTAMP_TZ            AS system_mod_stamp

    FROM source

)

SELECT *
FROM renamed