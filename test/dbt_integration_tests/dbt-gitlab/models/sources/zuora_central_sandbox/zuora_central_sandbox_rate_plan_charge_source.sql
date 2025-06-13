-- values to consider renaming:
-- mrr
-- dmrc
-- dtcv
-- tcv
-- uom


WITH source AS (

    SELECT *
    FROM {{ source('zuora_central_sandbox', 'rate_plan_charge') }}

), renamed AS(

    SELECT
      id                                                    AS rate_plan_charge_id,
      name                                                  AS rate_plan_charge_name,
      --keys
      original_id                                           AS original_id,
      rate_plan_id                                          AS rate_plan_id,
      product_rate_plan_charge_id                           AS product_rate_plan_charge_id,
      product_rate_plan_id                                  AS product_rate_plan_id,
      product_id                                            AS product_id,

      --recognition
      revenue_recognition_rule_name                         AS revenue_recognition_rule_name,
      rev_rec_code                                          AS revenue_recognition_code,
      rev_rec_trigger_condition                             AS revenue_recognition_trigger_condition,

      -- info
      effective_start_date                                  AS effective_start_date,
      effective_end_date                                    AS effective_end_date,
      date_trunc('month', effective_start_date)::DATE       AS effective_start_month,
      date_trunc('month', effective_end_date)::DATE         AS effective_end_month,
      end_date_condition                                    AS end_date_condition,

      mrr                                                   AS mrr,
      quantity                                              AS quantity,
      tcv                                                   AS tcv,
      uom                                                   AS unit_of_measure,

      account_id                                            AS account_id,
      accounting_code                                       AS accounting_code,
      apply_discount_to                                     AS apply_discount_to,
      bill_cycle_day                                        AS bill_cycle_day,
      bill_cycle_type                                       AS bill_cycle_type,
      billing_period                                        AS billing_period,
      billing_period_alignment                              AS billing_period_alignment,
      charged_through_date                                  AS charged_through_date,
      charge_model                                          AS charge_model,
      charge_number                                         AS rate_plan_charge_number,
      charge_type                                           AS charge_type,
      description                                           AS description,
      discount_level                                        AS discount_level,
      dmrc                                                  AS delta_mrc, -- delta monthly recurring charge
      dtcv                                                  AS delta_tcv, -- delta total contract value

      is_last_segment                                       AS is_last_segment,
      list_price_base                                       AS list_price_base,
      overage_calculation_option                            AS overage_calculation_option,
      overage_unused_units_credit_option                    AS overage_unused_units_credit_option,
      processed_through_date                                AS processed_through_date,

      segment                                               AS segment,
      specific_billing_period                               AS specific_billing_period,
      specific_end_date                                     AS specific_end_date,
      trigger_date                                          AS trigger_date,
      trigger_event                                         AS trigger_event,
      up_to_periods                                         AS up_to_period,
      up_to_periods_type                                    AS up_to_periods_type,
      version                                               AS version,

      --ext1, ext2, ext3, ... ext13

      --metadata
      created_by_id                                         AS created_by_id,
      created_date                                          AS created_date,
      updated_by_id                                         AS updated_by_id,
      updated_date                                          AS updated_date,
      _FIVETRAN_DELETED                                     AS is_deleted

    FROM source

)

SELECT *
FROM renamed
