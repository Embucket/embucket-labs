with raw_mrr_totals_levelled AS (

       SELECT * FROM {{ref('mart_arr')}}

), mrr_totals_levelled AS (

      SELECT subscription_name, 
              subscription_name_slugify,
              dim_crm_account_id                                        AS sfdc_account_id,
              oldest_subscription_in_cohort                             AS oldest_subscription_in_cohort,
              subscription_lineage                                      AS lineage,
              arr_month                                                 AS mrr_month,
              subscription_cohort_month                                 AS zuora_subscription_cohort_month,
              subscription_cohort_quarter                               AS zuora_subscription_cohort_quarter,
              months_since_subscription_cohort_start                    AS months_since_zuora_subscription_cohort_start,
              quarters_since_subscription_cohort_start                  AS quarters_since_zuora_subscription_cohort_start,
              sum(mrr)                                                  AS mrr
      FROM raw_mrr_totals_levelled
      {{ dbt_utils.group_by(n=10) }}

), current_arr_segmentation_all_levels AS (

       SELECT * FROM {{ref('current_arr_segmentation_all_levels')}}
       WHERE level_ = 'zuora_subscription_id'

), mapping AS (
      
       SELECT  subscription_name, sfdc_account_id
       FROM mrr_totals_levelled
       {{ dbt_utils.group_by(n=2) }}

), list AS ( --get all the subscription + their lineage + the month we're looking for MRR for (12 month in the future)

       SELECT subscription_name_slugify   AS original_sub,
                     c.value::VARCHAR     AS subscriptions_in_lineage,
                     mrr_month            AS original_mrr_month,
                     dateadd('year', 1, mrr_month) AS retention_month
       FROM mrr_totals_levelled,
       lateral flatten(input =>split(lineage, ',')) C
       {{ dbt_utils.group_by(n=4) }}

), retention_subs AS ( --find which of those subscriptions are real and group them by their sub you're comparing to.

       SELECT original_sub,
               retention_month,
               original_mrr_month,
               sum(mrr) AS retention_mrr
       FROM list
       INNER JOIN mrr_totals_levelled AS subs
       ON retention_month = mrr_month
       AND subscriptions_in_lineage = subscription_name_slugify
       {{ dbt_utils.group_by(n=3) }}

), finals AS (

       SELECT coalesce(retention_subs.retention_mrr, 0) AS net_retention_mrr,
              CASE WHEN net_retention_mrr > 0 
                  THEN least(net_retention_mrr, mrr)
                  ELSE 0 END AS gross_retention_mrr,
              retention_month, 
              mrr_totals_levelled.*
       FROM mrr_totals_levelled
       LEFT JOIN retention_subs
       ON subscription_name_slugify = original_sub
       AND retention_subs.original_mrr_month = mrr_totals_levelled.mrr_month

), joined as (

      SELECT finals.subscription_name             AS zuora_subscription_name,
             finals.oldest_subscription_in_cohort AS zuora_subscription_id,
             mapping.sfdc_account_id              AS salesforce_account_id,
             dateadd('year', 1, finals.mrr_month) AS retention_month, --THIS IS THE RETENTION MONTH, NOT THE MRR MONTH!!
             finals.mrr                           AS original_mrr,
             finals.net_retention_mrr,
             finals.gross_retention_mrr,
             finals.zuora_subscription_cohort_month,
             finals.zuora_subscription_cohort_quarter,
             finals.months_since_zuora_subscription_cohort_start,
             finals.quarters_since_zuora_subscription_cohort_start,
             {{ churn_type('original_mrr', 'net_retention_mrr') }}
      FROM finals
      LEFT JOIN mapping
      ON mapping.subscription_name = finals.subscription_name

)

SELECT joined.*, 
        current_arr_segmentation_all_levels.arr_segmentation
FROM joined
LEFT JOIN current_arr_segmentation_all_levels
ON joined.zuora_subscription_id = current_arr_segmentation_all_levels.id
WHERE retention_month <= dateadd(month, -1, CURRENT_DATE)
