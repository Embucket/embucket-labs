{{ config(
    tags=["mnpi"]
) }}

with source AS (

    SELECT *
    FROM {{ source('salesforce', 'zqu_quote_rate_plan_charge') }}

), renamed AS (

    SELECT

      -- ids
      id                                           AS zqu_quote_rate_plan_charge_id,
      name                                         AS zqu_quote_rate_plan_charge_name,
      zqu__description__c                          AS zqu_quote_rate_plan_charge_description,

      -- info
      discount_on_quote_formula__c                 AS discount_on_quote_formula,
      discount_text__c                             AS discount_text,
      effective_annual_price__c                    AS effective_annual_price,
      list_annual_price__c                         AS list_annual_price,
      quote__c                                     AS zqu_quote_id,
      discount_on_quote_safe_for_quote__c          AS discount_on_quote_safe_for_quote,
      list_price_safe_for_quote__c                 AS list_price_safe_for_quote,
      one__c                                       AS one,
      mavenlink_project_template_id__c             AS mavenlink_project_template_id,
      undiscounted_tcv__c                          AS undiscounted_tcv,
      zqu__apply_discount_to_one_time_charges__c   AS zqu_apply_discount_to_one_time_charges,
      zqu__apply_discount_to_recurring_charges__c  AS zqu__apply_discount_to_recurring_charges,
      zqu__apply_discount_to_usage_charges__c      AS apply_discount_to_usage_charges,
      zqu__billcycleday__c                         AS zqu_bill_cycle_day,
      zqu__billcycletype__c                        AS zqu_bill_cycle_type,
      zqu__billingdiscount__c                      AS zqu_billing_discount,
      zqu__billingsubtotal__c                      AS zqu_billing_sub_total,
      zqu__billingtax__c                           AS zqu_billing_tax,
      zqu__billingtotal__c                         AS zqu_billing_total,
      zqu__chargetype__c                           AS zqu_charge_type,
      zqu__currency__c                             AS zqu_currency,
      zqu__discount_level__c                       AS zqu_discount_level,
      zqu__discount__c                             AS zqu_discount,
      zqu__effectiveprice__c                       AS zqu_effective_price,
      zqu__enddatecondition__c                     AS zqu_end_date_condition,
      zqu__feetype__c                              AS zqu_fee_type,
      zqu__islastsegment__c                        AS zqu_is_last_segment,
      zqu__listpricebase__c                        AS zqu_list_price_base,
      zqu__listprice__c                            AS zqu_list_price,
      zqu__listtotal__c                            AS zqu_list_total,
      zqu__model__c                                AS zqu_model,
      zqu__mrr__c                                  AS zqu_mrr,
      zqu__period__c                               AS zqu_period,
      zqu__previewedmrr__c                         AS zqu_previewed_mrr,
      zqu__previewedtcv__c                         AS zqu_previewed_tcv,
      zqu__productname__c                          AS zqu_product_name,
      zqu__productrateplancharge__c                AS zqu_product_rate_plan_charge_id,
      zqu__productrateplanchargezuoraid__c         AS zqu_product_rate_plan_charge_zuora_id,
      zqu__quantity__c                             AS zqu_quantity,
      zqu__quoterateplan__c                        AS zqu_quote_rate_plan_id,
      zqu__rateplanname__c                         AS zqu_rate_plan_name,
      zqu__specificbillingperiod__c                AS zqu_specific_billing_period,
      zqu__subscriptionrateplanchargezuoraid__c    AS zqu_subscription_rate_plan_charge_zuora_id,
      zqu__tcv__c                                  AS zqu_tcv,
      zqu__total__c                                AS zqu_total,
      zqu__uom__c                                  AS zqu_uom,
      zqu__upto_how_many_periods_type__c           AS zqu_up_to_how_many_periods_type,
      zqu__upto_how_many_periods__c                AS zqu_up_to_how_many_periods,

      -- metadata
      createdbyid                                  AS created_by_id,
      createddate                                  AS created_date,
      isdeleted                                    AS is_deleted,
      lastmodifiedbyid                             AS last_modified_by_id,
      lastmodifieddate                             AS last_modified_date,
      _sdc_received_at                             AS sfdc_received_at,
      _sdc_extracted_at                            AS sfdc_extracted_at,
      _sdc_table_version                           AS sfdc_table_version,
      _sdc_batched_at                              AS sfdc_batched_at,
      _sdc_sequence                                AS sfdc_sequence,
      systemmodstamp                               AS system_mod_stamp

    FROM source
)

SELECT *
FROM renamed
