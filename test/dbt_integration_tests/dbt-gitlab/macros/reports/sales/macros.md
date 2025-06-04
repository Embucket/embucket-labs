{% docs rpt_retention %}

This macro contains the shared logic between all the rpt_retention models, such as `rpt_retention_parent_account_product`. It works by taking the fields by which retention needs to be calculated at.

The reason for needing to create retention analysis at different grains instead of having a base grain and aggregating from there is that the definition of churn changes based on the grain of the analysis.
For example, at the parent|product grain churns happens when a parent cancels a product, even if the parent has another product.
At the parent grain churn only happens when the parent leaves ALL of the products.

{% enddocs %}