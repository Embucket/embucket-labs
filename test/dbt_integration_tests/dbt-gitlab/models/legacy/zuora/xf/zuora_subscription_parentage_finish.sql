{{config({
    "schema": "legacy"
  })
}}

{% set partition_statement = "OVER ( PARTITION BY child_sub ORDER BY cohort_month, ultimate_parent_sub)" %} 
                      -- we have this second "order by" in case of two parents having the same cohort month.

with base as (

  SELECT *
  FROM {{ref('zuora_subscription_parentage_start')}}

), new_base as (

  SELECT *
  FROM base
  WHERE child_sub IN (SELECT child_sub 
                      FROM base 
                      GROUP BY 1 
                      HAVING count (*)> 1)

), consolidated_parents as (

  SELECT first_value(ultimate_parent_sub) {{ partition_statement }} as ultimate_parent_sub_2,
         child_sub,
         min(cohort_month) {{ partition_statement }} as cohort_month,
         min(cohort_quarter) {{ partition_statement }} as cohort_quarter,
         min(cohort_year) {{ partition_statement }} as cohort_year
  FROM new_base

), deduped_consolidations as (

  SELECT *
  FROM consolidated_parents
  GROUP BY 1, 2, 3, 4, 5

), unioned as (

  SELECT deduped_consolidations.ultimate_parent_sub_2 as ultimate_parent_sub,
         new_base.ultimate_parent_sub as child_sub,
         deduped_consolidations.cohort_month,
         deduped_consolidations.cohort_quarter,
         deduped_consolidations.cohort_year
  FROM deduped_consolidations
  LEFT JOIN new_base 
  ON new_base.child_sub = deduped_consolidations.child_sub
  WHERE ultimate_parent_sub_2 != ultimate_parent_sub

  UNION ALL

  SELECT * FROM deduped_consolidations

  UNION ALL
  
  SELECT *
  FROM base
  WHERE child_sub NOT IN (SELECT child_sub FROM new_base)

), fix_consolidations as (

  SELECT first_value(ultimate_parent_sub) {{ partition_statement }} as ultimate_parent_sub,
         child_sub,
         min(cohort_month) {{ partition_statement }} as cohort_month,
         min(cohort_quarter) {{ partition_statement }} as cohort_quarter,
         min(cohort_year) {{ partition_statement }} as cohort_year
  FROM unioned

)

SELECT *
FROM fix_consolidations
GROUP BY 1, 2, 3, 4, 5
