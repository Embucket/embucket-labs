WITH sfdc_opportunity_xf AS (

    SELECT
      TO_NUMBER(amount, 38, 2) AS opportunity_amount,
      close_date::DATE         AS close_date,
      invoice_number,
      net_incremental_acv,
      opportunity_id,
      subscription_type
    FROM {{ ref('sfdc_opportunity_xf') }}
    WHERE stage_name = 'Closed Won'
      AND invoice_number IS NOT NULL  

), zuora_invoice_charges AS (

    SELECT
      effective_start_date,
      effective_end_date,
      invoice_amount_without_tax AS invoice_amount,
      invoice_date,
      invoice_item_charge_amount AS item_amount,
      invoice_number,
      is_last_segment_version,
      mrr, 
      delta_tcv,
      {{ product_category('rate_plan_name') }},
      {{ delivery('product_category') }},  
      rate_plan_charge_name      AS charge_name,  
      rate_plan_charge_number    AS charge_number, 
      rate_plan_charge_segment   AS charge_segment, 
      segment_version_order,
      subscription_name_slugify
    FROM {{ ref('zuora_invoice_charges') }}

), filtered_charges AS (

    SELECT
      zuora_invoice_charges.*,
      IFF(
          ROW_NUMBER() OVER (
          PARTITION BY subscription_name_slugify, invoice_number, charge_number, charge_segment
          ORDER BY segment_version_order DESC) = 1,
          TRUE, FALSE
      ) AS is_last_invoice_segment_version
    FROM zuora_invoice_charges
    WHERE effective_end_date > invoice_date
        AND mrr > 0
    QUALIFY is_last_invoice_segment_version    

), true_mrr_periods AS (

    SELECT
      charge_number,
      charge_segment,
      effective_start_date AS true_effective_start_date,
      effective_end_date   AS true_effective_end_date
    FROM zuora_invoice_charges
    WHERE is_last_segment_version  

), aggregate_subscription AS (

    SELECT
      subscription_name_slugify,
      invoice_number,
      invoice_date,
      COUNT(DISTINCT subscription_name_slugify) OVER (PARTITION BY invoice_number)  AS invoiced_subscriptions,
      TO_NUMBER(MAX(invoice_amount), 38, 2)                                         AS invoice_amount,
      TO_NUMBER(SUM(item_amount), 38, 2)                                            AS subscription_amount,
      TO_NUMBER(SUM(delta_tcv), 38, 2)                                              AS delta_tcv,
      TO_NUMBER(SUM(IFF(charge_name IN ('1,000 CI Minutes', '1,000 Compute Minutes'), item_amount, 0)), 38, 2)  AS ci_minutes_amount,
      TO_NUMBER(SUM(IFF(charge_name = 'Trueup', item_amount, 0)), 38, 2)            AS trueup_amount
    FROM zuora_invoice_charges
    GROUP BY 1,2,3  

), charge_join AS (
    -- first join to opportunities based on invoiced subscription charge amount = opportunity TCV

    SELECT
      aggregate_subscription.*,
      close_date,
      net_incremental_acv,
      opportunity_amount,
      opportunity_id,
      subscription_type
    FROM aggregate_subscription
    LEFT JOIN sfdc_opportunity_xf
      ON aggregate_subscription.invoice_number = sfdc_opportunity_xf.invoice_number
      AND aggregate_subscription.subscription_amount = sfdc_opportunity_xf.opportunity_amount  

), tcv_join AS (
    -- next join to opportunities based on change in TCV to subscription = opportunity TCV
    SELECT
      subscription_name_slugify,
      charge_join.invoice_number,
      invoice_date,
      invoiced_subscriptions,
      invoice_amount,
      subscription_amount,
      delta_tcv,
      ci_minutes_amount,
      trueup_amount,
      COALESCE(charge_join.close_date, sfdc_opportunity_xf.close_date)                   AS close_date,
      COALESCE(charge_join.net_incremental_acv, sfdc_opportunity_xf.net_incremental_acv) AS net_incremental_acv,
      COALESCE(charge_join.opportunity_amount, sfdc_opportunity_xf.opportunity_amount)   AS opportunity_amount,
      COALESCE(charge_join.opportunity_id, sfdc_opportunity_xf.opportunity_id)           AS opportunity_id,
      COALESCE(charge_join.subscription_type, sfdc_opportunity_xf.subscription_type)     AS subscription_type,
    FROM charge_join
    LEFT JOIN sfdc_opportunity_xf
      ON charge_join.invoice_number = sfdc_opportunity_xf.invoice_number
      AND charge_join.delta_tcv = sfdc_opportunity_xf.opportunity_amount
      AND charge_join.opportunity_id IS NULL  

), final AS (

    SELECT
      -- keys
      tcv_join.subscription_name_slugify,
      tcv_join.invoice_number,
      filtered_charges.charge_number,
      tcv_join.opportunity_id,

      -- dates
      tcv_join.invoice_date,
      filtered_charges.effective_start_date AS booked_effective_start_date,
      filtered_charges.effective_end_date   AS booked_effective_end_date,
      true_mrr_periods.true_effective_start_date,
      true_mrr_periods.true_effective_end_date,
      tcv_join.close_date,

      -- invoice info
      tcv_join.invoiced_subscriptions,
      tcv_join.invoice_amount,
      tcv_join.subscription_amount,
      tcv_join.delta_tcv,
      tcv_join.ci_minutes_amount,
      tcv_join.trueup_amount,
      filtered_charges.item_amount,
      tcv_join.opportunity_amount,

      -- charge info
      filtered_charges.mrr,
      tcv_join.net_incremental_acv,

      -- metadata
      filtered_charges.charge_name,
      filtered_charges.charge_segment,
      filtered_charges.delivery,
      filtered_charges.product_category,
      tcv_join.subscription_type,
      MAX(is_last_segment_version) AS is_last_segment_version
    FROM tcv_join
    INNER JOIN filtered_charges
      ON tcv_join.subscription_name_slugify = filtered_charges.subscription_name_slugify
      AND tcv_join.invoice_number = filtered_charges.invoice_number
    INNER JOIN true_mrr_periods
      ON filtered_charges.charge_number = true_mrr_periods.charge_number
      AND filtered_charges.charge_segment = true_mrr_periods.charge_segment  
    {{ dbt_utils.group_by(n=25) }}      

)

SELECT *
FROM final