WITH zuora_mrr AS (

    SELECT *
    FROM {{ ref('zuora_base_mrr') }}

), date_table AS (

     SELECT *
     FROM {{ ref('date_details') }}
     WHERE day_of_month = 1

), amortized_mrr AS (

    SELECT country,
           account_number,
           subscription_name,
           subscription_name_slugify,
           oldest_subscription_in_cohort,
           lineage,
           rate_plan_name,
           product_category,
           delivery,
           rate_plan_charge_name,
           mrr,
           date_actual AS mrr_month,
           sub_start_month,
           sub_end_month,
           effective_start_month,
           effective_end_month,
           effective_start_date,
           effective_end_date,
           cohort_month,
           cohort_quarter,
           unit_of_measure,
           quantity
  FROM zuora_mrr b
  LEFT JOIN date_table d
  ON d.date_actual >= b.effective_start_month
  AND d.date_actual <= b.effective_end_month


), final as (

SELECT country,
       account_number,
       subscription_name,
       subscription_name_slugify,
       oldest_subscription_in_cohort,
       lineage,
       rate_plan_name,
       product_category,
       delivery,
       rate_plan_charge_name,
       mrr_month,
       cohort_month,
       cohort_quarter,
       unit_of_measure,
       sum(mrr)       AS mrr,
       sum(quantity)  AS quantity
FROM amortized_mrr
WHERE mrr_month IS NOT NULL
{{ dbt_utils.group_by(n=14) }}

)

SELECT *
FROM final
