with source as (

	SELECT * FROM {{ref('prep_subscription_lineage')}}

), zuora_subscription_intermediate as (

	SELECT * FROM {{ref('prep_subscription_lineage_intermediate')}}

), flattened as (

    SELECT subscription_name_slugify, 
    		c.value::VARCHAR as subscriptions_in_lineage, 
    		c.index as child_index
    FROM source,
    lateral flatten(input =>split(lineage, ',')) C

), find_max_depth as (

    SELECT subscriptions_in_lineage, 
    		max(child_index) as child_index
    FROM flattened
    GROUP BY 1

), with_parents as (

    SELECT subscription_name_slugify as ultimate_parent_sub, 
    		find_max_depth.subscriptions_in_lineage as child_sub, 
    		find_max_depth.child_index as depth
    FROM find_max_depth
    LEFT JOIN flattened
    ON find_max_depth.subscriptions_in_lineage = flattened.subscriptions_in_lineage
    AND find_max_depth.child_index = flattened.child_index

), finalish as (

    SELECT with_parents.ultimate_parent_sub,
           with_parents.child_sub,
           zuora_subscription_intermediate.subscription_month as cohort_month,
           zuora_subscription_intermediate.subscription_quarter as cohort_quarter,
           zuora_subscription_intermediate.subscription_year as cohort_year
    FROM with_parents
    LEFT JOIN  zuora_subscription_intermediate
    ON zuora_subscription_intermediate.subscription_name_slugify = with_parents.ultimate_parent_sub

)
SELECT * 
FROM finalish
