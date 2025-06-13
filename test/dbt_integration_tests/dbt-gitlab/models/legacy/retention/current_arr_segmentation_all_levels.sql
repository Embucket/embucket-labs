{% set levels = ['zuora_subscription_id',
                  'sfdc_account_id',
                  'parent_account_id'] %}

WITH base AS (
    SELECT oldest_subscription_in_cohort as zuora_subscription_id,
            dim_parent_crm_account_id as parent_account_id,
            dim_crm_account_id as sfdc_account_id,
          {{ dbt_utils.star(from=ref('mart_arr')) }}
    FROM {{ref('mart_arr')}}

{% for level in levels -%} 
), {{level}}_max_month as (

    SELECT max(arr_month) as most_recent_mrr_month,
              {{ level }}  AS id
    FROM base
    WHERE arr_month < dateadd(month, -1, CURRENT_DATE)
    GROUP BY 2

), {{level}}_get_mrr as(

    SELECT {{level}}_max_month.*, sum(base.mrr) as mrr
    FROM {{level}}_max_month
    LEFT JOIN base
        ON {{level}}_max_month.id =
              base.{{level}}
        AND {{level}}_max_month.most_recent_mrr_month = base.arr_month
    GROUP BY 1, 2

), {{level}}_get_segmentation as (

SELECT id,
      '{{level}}'::varchar as level_,
      mrr*12 as arr,
      RANK() OVER ( PARTITION BY level_ ORDER BY arr DESC) as arr_rank,
      CASE WHEN (mrr*12) < 5000 THEN 'Under 5K'
          WHEN (mrr*12) < 50000 THEN '5K to 50K'
          WHEN (mrr*12) < 100000 THEN '50K to 100K'
          WHEN (mrr*12) < 500000 THEN '100K to 500K'
          WHEN (mrr*12) < 1000000 THEN '500K to 1M'
          ELSE '1M and above'
      END AS arr_segmentation,
      CASE WHEN arr_rank < 26 THEN 'First 25 Customer'
          WHEN arr_rank < 51 THEN '26 - 50 Customer'
          WHEN arr_rank < 101 THEN '51 - 100 Customer'
          WHEN arr_rank < 501 THEN '101 - 500 Customer'
          WHEN arr_rank < 1001 THEN '501 - 1000 Customer'
          WHEN arr_rank < 5001 THEN '1001 - 5000 Customer'
          ELSE '5000+ Customer'
      END AS rank_segmentation
FROM {{level}}_get_mrr
GROUP BY 1, 2, 3

{% endfor -%}
), unioned as (

{% for level in levels -%} 

SELECT * FROM {{level}}_get_segmentation
{%- if not loop.last %} UNION ALL {%- endif %}

{% endfor -%}
)
SELECT * FROM unioned
GROUP BY 1, 2, 3, 4, 5, 6