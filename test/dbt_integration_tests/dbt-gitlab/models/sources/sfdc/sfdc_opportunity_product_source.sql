{{ config(
    tags=["mnpi", "six_hourly"]
) }}

WITH source AS (

  SELECT *
  FROM {{ source('salesforce', 'opportunity_product') }} 

),

renamed AS (
  
  SELECT 
    -- keys
    id::VARCHAR                            AS opportunity_product_id,
    opportunityid::VARCHAR                 AS opportunity_id,
    createdbyid::VARCHAR                   AS created_by_id,
    quote__c::VARCHAR                      AS quote_id,

    -- product and price fields
    createddate::TIMESTAMP_TZ              AS created_date,
    servicedate::TIMESTAMP_TZ              AS service_date,
    name::VARCHAR                          AS opportunity_name,
    description::VARCHAR                   AS description,
    listprice::FLOAT                       AS list_price,
    pricebookentryid::VARCHAR              AS price_book_entry_id,
    product2id::VARCHAR                    AS product2_id,
    productcode::VARCHAR                   AS product_code,
    product_code_from_products__c::VARCHAR AS product_code_from_products,
    product_name_from_products__c::VARCHAR AS product_name_from_products,
    quantity::FLOAT                        AS quantity,
    sortorder::NUMBER                      AS sort_order,
    ticket_group_numeric__c::FLOAT         AS ticket_group,
    totalprice::FLOAT                      AS total_price,
    unitprice::FLOAT                       AS unit_price,
    type__c::VARCHAR                       AS amendment_type,
    uniqueidentifier__c::VARCHAR           AS unique_identifier,
    annual_recurring_revenue__c::FLOAT     AS annual_recurring_revenue,
    uom__c::VARCHAR                        AS unit_of_measure,
    charge_type__c::VARCHAR                AS charge_type,
    product_rate_plan_charge__c::VARCHAR   AS product_rate_plan_charge,
    list_price__c::FLOAT                   AS list_price_custom, 
    cea_product_type__c::VARCHAR           AS cea_product_type,
    discount__c::FLOAT                     AS discount,
    effective_price__c::FLOAT              AS effective_price,
    included_units__c::FLOAT               AS included_units, 

    -- ramp fields
    isramp__c::BOOLEAN                     AS is_ramp,
    rampintervalstartdate__c::TIMESTAMP_TZ AS ramp_interval_start_date,
    rampintervalenddate__c::TIMESTAMP_TZ   AS ramp_interval_end_date,
    next_interval__c::VARCHAR              AS next_interval,
    quoterampintervalname__c::VARCHAR      AS quote_ramp_interval_name,

    -- quote fields
    quotemodel__c::VARCHAR                 AS quote_model,
    quote_rate_plan_name__c::VARCHAR       AS quote_rate_plan_name,
    quote_rate_plan_charge__c::VARCHAR     AS quote_rate_plan_charge,
    quote_charge_summary__c::VARCHAR       AS quote_charge_summary,
    quote_subscription_type__c::VARCHAR    AS quote_subscription_type,

    -- delta fields
    deltamrr__c::FLOAT                     AS delta_mrr,
    delta_arr__c::FLOAT                    AS delta_arr,
    deltamrrstartdate__c::TIMESTAMP_TZ     AS delta_mrr_start_date,
    deltatcbstartdate__c::TIMESTAMP_TZ     AS delta_tcb_start_date,
    deltamrrenddate__c::TIMESTAMP_TZ       AS delta_mrr_end_date,
    deltatcbenddate__c::TIMESTAMP_TZ       AS delta_tcb_end_date,
    delta_tcb__c::FLOAT                    AS delta_tcb,
    delta_quantity__c::FLOAT               AS delta_quantity,

    -- metadata
    isdeleted::BOOLEAN                     AS is_deleted,
    lastmodifiedbyid::VARCHAR              AS last_modified_by_id,
    lastmodifieddate::TIMESTAMP_TZ         AS last_modified_date,
    lastreferenceddate::TIMESTAMP_TZ       AS last_referenced_date,
    lastvieweddate::TIMESTAMP_TZ           AS last_viewed_date,
    _sdc_batched_at::TIMESTAMP_TZ          AS sdc_batched_at_date,
    _sdc_extracted_at::TIMESTAMP_TZ        AS sdc_extracted_at_date,
    _sdc_received_at::TIMESTAMP_TZ         AS sdc_received_at_date,
    _sdc_sequence::NUMBER                  AS sdc_sequence,
    _sdc_table_version::NUMBER             AS sdc_table_version,
    systemmodstamp::TIMESTAMP_TZ           AS system_mod_stamp
    
  FROM source

)

SELECT *
FROM renamed
