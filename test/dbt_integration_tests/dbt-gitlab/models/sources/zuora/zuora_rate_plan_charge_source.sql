-- depends_on: {{ ref('zuora_excluded_accounts') }}

-- values to consider renaming:
-- mrr
-- dmrc
-- dtcv
-- tcv
-- uom

{{ config(
    tags=["mnpi"]
) }}

WITH source AS (

    SELECT *
    FROM {{ source('zuora', 'rate_plan_charge') }}

), renamed AS(

    SELECT
      id                                                    AS rate_plan_charge_id,
      name                                                  AS rate_plan_charge_name,
      --keys
      originalid                                            AS original_id,
      rateplanid                                            AS rate_plan_id,
      productrateplanchargeid                               AS product_rate_plan_charge_id,
      productrateplanid                                     AS product_rate_plan_id,
      productid                                             AS product_id,

      --recognition
      revenuerecognitionrulename                            AS revenue_recognition_rule_name,
      revreccode                                            AS revenue_recognition_code,
      revrectriggercondition                                AS revenue_recognition_trigger_condition,

      -- info
      effectivestartdate                                    AS effective_start_date,
      effectiveenddate                                      AS effective_end_date,
      date_trunc('month', effectivestartdate)::DATE         AS effective_start_month,
      date_trunc('month', effectiveenddate)::DATE           AS effective_end_month,
      enddatecondition                                      AS end_date_condition,

      mrr,
      quantity                                              AS quantity,
      tcv,
      uom                                                   AS unit_of_measure,

      accountid                                             AS account_id,
      accountingcode                                        AS accounting_code,
      applydiscountto                                       AS apply_discount_to,
      billcycleday                                          AS bill_cycle_day,
      billcycletype                                         AS bill_cycle_type,
      billingperiod                                         AS billing_period,
      billingperiodalignment                                AS billing_period_alignment,
      chargedthroughdate                                    AS charged_through_date,
      chargemodel                                           AS charge_model,
      chargenumber                                          AS rate_plan_charge_number,
      chargetype                                            AS charge_type,
      description                                           AS description,
      discountlevel                                         AS discount_level,
      dmrc                                                  AS delta_mrc, -- delta monthly recurring charge
      dtcv                                                  AS delta_tcv, -- delta total contract value

      islastsegment                                         AS is_last_segment,
      listpricebase                                         AS list_price_base,
      --numberofperiods                                       AS number_of_periods,
      overagecalculationoption                              AS overage_calculation_option,
      overageunusedunitscreditoption                        AS overage_unused_units_credit_option,
      processedthroughdate                                  AS processed_through_date,

      segment                                               AS segment,
      specificbillingperiod                                 AS specific_billing_period,
      specificenddate                                       AS specific_end_date,
      triggerdate                                           AS trigger_date,
      triggerevent                                          AS trigger_event,
      uptoperiods                                           AS up_to_period,
      uptoperiodstype                                       AS up_to_periods_type,
      version                                               AS version,

      --ext1, ext2, ext3, ... ext13

      --metadata
      createdbyid                                           AS created_by_id,
      createddate                                           AS created_date,
      updatedbyid                                           AS updated_by_id,
      updateddate                                           AS updated_date,
      deleted                                               AS is_deleted

    FROM source

)

SELECT *
FROM renamed
