{{ config(
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ source('salesforce', 'zqu_quote_rate_plan') }}

), renamed AS (

    SELECT

      -- ids
      id                                        AS zqu_quote_rate_plan_id,
      zqu__quoterateplanzuoraid__c              AS zqu_quote_rate_plan_zuora_id,
      name                                      AS zqu_quote_rate_plan_name,

      -- info
      one__c                                    AS one,
      admin_subtotal_test__c                    AS admin_subtotal_test,
      admin_subtotal_summary__c                 AS admin_subtotal_summary,
      charge_summary_sub_total__c               AS charge_summary_sub_total,
      license_amount__c                         AS license_amount,
      professional_services_amount__c           AS professional_services_amount,
      true_up_amount__c                         AS true_up_amount,
      ticket_group_numeric__c                   AS ticket_group_numeric,
      zqu__quote__c                             AS zqu_quote_id,
      zqu__quoteamendment__c                    AS zqu_quote_amendment_id,
      zqu__amendmenttype__c                     AS zqu_quote_amendment_type,
      zqu__productrateplan__c                   AS zqu_product_rate_plan_id,
      zqu__productrateplanzuoraid__c            AS zqu_product_rate_plan_zuora_id,
      zqu__quoteproductname__c                  AS zqu_quote_product_name,
      zqu__subscriptionrateplanzuoraid__c       AS zqu_subscription_rate_plan_zuora_id,
      rate_plan_charge_last_modified_time__c    AS rate_plan_charge_last_modified_time,
      zqu__time_product_added__c                AS zqu_time_product_added,

      -- metadata
      createdbyid                               AS created_by_id,
      createddate                               AS created_date,
      isdeleted                                 AS is_deleted,
      lastmodifiedbyid                          AS last_modified_by_id,
      lastmodifieddate                          AS last_modified_date,
      _sdc_received_at                          AS sfdc_received_at,
      _sdc_extracted_at                         AS sfdc_extracted_at,
      _sdc_table_version                        AS sfdc_table_version,
      _sdc_batched_at                           AS sfdc_batched_at,
      _sdc_sequence                             AS sfdc_sequence,
      systemmodstamp                            AS system_mod_stamp

    FROM source

)

SELECT *
FROM renamed
