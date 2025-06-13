with zuora_subscription_intermediate as (

    SELECT * FROM {{ ref ('zuora_subscription_intermediate')}}

), zuora_subscription_lineage as (

    SELECT * FROM {{ ref ('zuora_subscription_lineage')}}

), zuora_subscription_parentage as (

    SELECT * FROM {{ ref ('zuora_subscription_parentage_finish')}}

)

SELECT zuora_subscription_intermediate.*, 
        zuora_subscription_lineage.lineage,
        coalesce(zuora_subscription_parentage.ultimate_parent_sub,zuora_subscription_intermediate.subscription_name_slugify) AS oldest_subscription_in_cohort,
        coalesce(zuora_subscription_parentage.cohort_month, zuora_subscription_intermediate.subscription_month) as cohort_month,
        coalesce(zuora_subscription_parentage.cohort_quarter,zuora_subscription_intermediate.subscription_quarter) as cohort_quarter,
        coalesce(zuora_subscription_parentage.cohort_year, zuora_subscription_intermediate.subscription_year) as cohort_year
FROM zuora_subscription_intermediate
LEFT JOIN zuora_subscription_lineage
ON zuora_subscription_intermediate.subscription_name_slugify = 
    zuora_subscription_lineage.subscription_name_slugify
LEFT JOIN zuora_subscription_parentage
ON zuora_subscription_intermediate.subscription_name_slugify = 
	zuora_subscription_parentage.child_sub 