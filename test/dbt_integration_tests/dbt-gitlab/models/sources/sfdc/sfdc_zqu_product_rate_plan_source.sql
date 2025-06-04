{{ config(
    tags=["mnpi"]
) }}

with source AS (

    SELECT *
    FROM {{ source('salesforce', 'zqu_product_rate_plan') }}

), renamed AS (

    SELECT

      -- ids
      id::VARCHAR                                  AS zqu_product_rate_plan_id,
      zqu__product__c::VARCHAR                     AS zqu_product_id,
      zqu__zproduct__c::VARCHAR                    AS zqu_zproduct_id,
      zqu__zuoraid__c::VARCHAR                     AS zqu_zuora_id,

      -- info
      name                                         AS zqu_product_rate_plan_name,
      zqu__productrateplanfullname__c::VARCHAR     AS zqu_product_rate_plan_full_name, 
      product_category__c::VARCHAR                 AS product_category,
      product_ranking__c:: FLOAT                   AS product_ranking,
      prpcategory__c::VARCHAR                      AS product_rate_plan_category,
      zqu__description__c::VARCHAR                 AS zqu_product_rate_plan_description,
      rate_plan_family__c::VARCHAR                 AS rate_plan_family, 
      rate_plan_type__c::VARCHAR                   AS rate_plan_type,                   
      deal_desk_viewable__c::BOOLEAN               AS deal_desk_viewable,
      guidedselling__c::VARCHAR                    AS guided_selling,
      is_true_up__c::BOOLEAN                       AS is_true_up,
      lastreferenceddate ::VARCHAR                 AS last_referenced_date,
      lastvieweddate::VARCHAR                      AS last_viewed_date,
      license_type__c ::VARCHAR                    AS license_type,
      mavenlink_project_template_name__c::VARCHAR  AS mavenlink_project_template_name,
      show_in_new_business__c::BOOLEAN             AS show_in_new_business,
      show_in_renewal__c::BOOLEAN                  AS show_in_renewal,
      zqu__activecurrencies__c::VARCHAR            AS zqu_active_currencies,
      zqu__default__c::BOOLEAN                     AS zqu_default,
      zqu__deleted__c::BOOLEAN                     AS zqu_deleted,
      zqu__effectiveenddatetext__c::VARCHAR        AS zqu_effective_end_date_text,
      zqu__effectiveenddate__c::VARCHAR            AS zqu_effective_end_date,
      zqu__effectivestartdatetext__c::VARCHAR      AS zqu_effective_start_date_text,
      zqu__effectivestartdate__c::VARCHAR          AS zqu_effective_start_date,
      zqu__entityid__c::VARCHAR                    AS zqu_entity_id,
      zqu__syncstatus__c ::VARCHAR                 AS zqu_syncstatus,
      

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
