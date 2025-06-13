{{ config(
    tags=["mnpi"]
) }}

with source AS (

    SELECT *
    FROM {{ source('salesforce', 'zqu_zproduct') }}

), renamed AS (

    SELECT

      -- ids
      id::VARCHAR                                  AS zqu_zproduct_id,
      zqu__product__c::VARCHAR                     AS zqu_product_id,
      zqu__sku__c::VARCHAR                         AS zqu_sku,
      zqu__zuoraid__c::VARCHAR                     AS zqu_zuora_id,

      -- info
      name                                         AS zqu_zproduct_name,
      zqu__category__c                             AS zqu_zproduct_category,
      zqu__description__c::VARCHAR                 AS zqu_zproduct_description,
      zqu__active__c::BOOLEAN                      AS zqu_active,
      zqu__allow_feature_changes__c::BOOLEAN       AS zqu_allow_feature_changes,
      lastreferenceddate ::VARCHAR                 AS last_referenced_date,
      lastvieweddate::VARCHAR                      AS last_viewed_date,
      ownerid ::VARCHAR                            AS owner_id,   
      zqu__deleted__c::BOOLEAN                     AS zqu_deleted,
      zqu__effectiveenddatetext__c::VARCHAR        AS zqu_effective_end_date_text,
      zqu__effectiveenddate__c::VARCHAR            AS zqu_effective_end_date,
      zqu__effectivestartdatetext__c::VARCHAR      AS zqu_effective_start_date_text,
      zqu__effectivestartdate__c::VARCHAR          AS zqu_effective_start_date,
      

      -- metadata
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
