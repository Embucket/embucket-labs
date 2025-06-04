{% docs gitlab_dotcom_namespace_historical_daily %}
 
ultimate_parent_plan_id column: 

Assumes if ultimate_parent_plan_id is null that it is a free plan, plan id 34. Also, accounts for gold/ultimate plans in the past that did not have a trial plan id or trial name. The logic checks for plan names that are ultimate/gold AND have trial set to true and conforms them to plan id 102 which is the ultimate trial plan. After a trial expires, it is moved to a free plan, plan id 34. Therefore, after accounting for the gold/ultimate plans that had trial = TRUE, we can rely on the plan id and plan name out of the subscription and plan source tables to identify trials. The ultimate_trial plan name is plan id 102 and the premium_trial plan name is plan id 103. In a future iteration, this plan information should be conformed with the dim_product_tier dimension to have a single source of truth for plan information at GitLab.

{% enddocs %}