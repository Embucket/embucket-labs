WITH base_mrr AS (

    SELECT * FROM {{ ref('zuora_base_mrr_amortized') }}

), service_type AS ( --calculate service_type

    SELECT country,
          account_number,
          subscription_name_slugify,
          subscription_name,
          oldest_subscription_in_cohort,
          lineage,
          mrr_month,
          cohort_month    AS zuora_subscription_cohort_month,
          cohort_quarter  AS zuora_subscription_cohort_quarter,
          mrr,
          product_category,
          delivery,
          rate_plan_name,
          CASE WHEN lower(rate_plan_name) like '%support%' THEN 'Support Only'
            ELSE 'Full Service'
          END             AS service_type,
          unit_of_measure,
          quantity
    FROM base_mrr

), uniqueified as ( -- one row per sub slug for counting x product_category x mrr_month combo, with first of other values

    SELECT {{ dbt_utils.generate_surrogate_key(['mrr_month', 'subscription_name', 'product_category', 'unit_of_measure']) }} AS primary_key,
          country,
          account_number,
          subscription_name_slugify,
          subscription_name,
          oldest_subscription_in_cohort,
          lineage,
          mrr_month,
          zuora_subscription_cohort_month,
          zuora_subscription_cohort_quarter,
          product_category,
          delivery,
          unit_of_measure,
          service_type,
          array_agg(rate_plan_name) AS rate_plan_name,
          sum(quantity)             AS quantity,
          sum(mrr)                  AS mrr
    FROM service_type
    {{ dbt_utils.group_by(n=14) }}

)

SELECT *, -- calculate new values
      datediff(month,zuora_subscription_cohort_month, mrr_month)      AS months_since_zuora_subscription_cohort_start,
      datediff(quarter, zuora_subscription_cohort_quarter, mrr_month) AS quarters_since_zuora_subscription_cohort_start
FROM uniqueified
