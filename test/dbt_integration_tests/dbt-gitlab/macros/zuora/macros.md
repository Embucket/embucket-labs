{% docs churn_type %}
This macro compares MRR values and buckets them into retention categories.
{% enddocs %}


{% docs delivery %}
This macro maps product categories to [delivery](https://about.gitlab.com/handbook/marketing/product-marketing/tiers/#delivery).
{% enddocs %}


{% docs monthly_price_per_seat_change %}
This macro calculates the difference between monthly price per seat, but only when the unit_of_measure of the plan is seats.
{% enddocs %}


{% docs plan_change %}
This macro compares product rankings and returns whether it was upgraded/downgraded/maintained/cancelled, and when the product ranking is 0 (which means it's an old plan) it returns Not Valid
{% enddocs %}


{% docs product_category %}
This macro maps SKUs to their product categories.
{% enddocs %}


{% docs retention_reason %}
This macro compares MRR values, Product Category, Product Ranking and Amount of seats and gives back the reason as to why MRR went up or down.
{% enddocs %}


{% docs retention_type %}
This macro compares MRR values and buckets them into retention categories.
{% enddocs %}


{% docs seat_change %}
This macro compares the amount of seats and categorizes it into Maintained/Contraction/Expansion/Cancelled, and when the unit_of_measure of the plans isn't seats, Not Valid
{% enddocs %}


{% docs zuora_excluded_accounts %}
This macro returns a list of zuora account_ids that are meant to be excluded from our base models. Account IDs can be filtered out because they were created for internal testing purposes (permanent filter) or because there's a data quality issue ([like a missing CRM](https://gitlab.com/gitlab-data/analytics/tree/master/transform/snowflake-dbt/tests#test-zuora_account_has_crm_id)) that we're fixing (temporary filter).
{% enddocs %}


{% docs zuora_slugify %}
This macro replaces any combination of whitespace and 2 pipes with a single pipe (important for renewal subscriptions), replaces any multiple whitespace characters with a single whitespace character, and then it replaces all non alphanumeric characters with dashes and casts it to lowercases as well. The end result of using this macro on data like "A-S00003830 || A-S00013333" is "a-s00003830|a-s00013333".

The custom test `zuora_slugify_cardinality` tests the uniqueness of the `zuora_subscription_slugify` (eg. 2 different subscription names will result to 2 different `zuora_subscription_name_slugify`)
{% enddocs %}


