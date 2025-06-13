{% docs mart_product_usage_health_score %}

**Description:** Use Case Adoption Scoring data. This model contains all Use Case Adoption Scores, the metrics that those scores are based on, and identifying fields for the Account, Subscription, and Instance. This model is used for Use Case Adoption reporting/analysis, Platform Value reporting/analysis, Data Coverage reporting/analysis, and populating Gainsight Scorecard values. 

**Data Grain:** 1 row of usage data per instance_identifier, per snapshot_month. That row of data will be from the ping with the greatest ping_created_at value in each given snapshot_month

- primary_key

**Filters Applied to Model:** 

- license_user_count <> 0
- greatest ping_created_at value for each instance_identifier per snapshot_month

**Business Logic in this Model:** 

- Each instance must be associated with a subscription and sending usage data in order to be included in this table
- More detail about Calculations, Thresholds and Methodology can be found on this [Handbook Page](https://about.gitlab.com/handbook/customer-success/product-usage-data/use-case-adoption/)
- If a usage metric is not available for any reason, the score that uses that Metric is set to null to prevent that instance from incorrectly being marked as Red


**Other Comments:** 

- Use Case Adoption Scores are calcualted for all instances showing in this table regardless of instance_type. For reporting and analysis the filter WHERE instance_type = 'Production' must be used for accurate results
- For subscriptions with multiple Production instances a primary instance will need to be selected. This is commonly done with a QUALIFY statement that chooses the Production instance with the highest billable_user_count per delivery_type, per dim_subscription_id_original. (QUALIFY ROW_NUMBER() OVER (PARTITION BY snapshot_month, dim_subscription_id_original, delivery_type ORDER BY billable_user_count desc nulls last, ping_created_at desc nulls last) = 1)
    - This is especially important when joining this table to mart_arr so that subscription ARR amounts are not counted multiple times
- When joining this table to mart_arr the join should be: ON arr_month = snapshot_month AND dim_subscription_id_original = dim_subscription_id_original AND product_delivery_type = delivery_type followed by the QUALIFY statement mentioned above
- If additional usage metrics are needed for analysis beyond the metrics in this table the PROD.common_mart_product.mart_product_usage_paid_user_metrics_monthly table can be joined using the primary_key field on both tables for a 1 to 1 match. 

{% enddocs %}

{% docs wk_license_billable_users %}

**Description:**
This model provides a comprehensive view of license and billable users for GitLab Duo Pro, GitLab Duo Enterprise, and Enterprise Agile Planning add-ons across different data sources (Zuora, Seat Link, Service Ping, and Postgres Assignment tables for GitLab.com). It combines data to offer insights for Self-Managed, Dedicated and GitLab.com deployments, including daily license and billable user counts for GitLab.com Duo Pro and Duo Enterprise add-ons.

**Data Grain:**
- reporting_date
- dim_installation_id
- dim_namespace_id

**Filters Applied to Model:**
- Excludes records where both dim_installation_id and dim_namespace_id are NULL
- Includes data for GitLab Duo Pro, GitLab Duo Enterprise and Enterprise Agile Planning add-ons only

**Business Logic in this Model:**
1. Combines license user data from Zuora, Seat Link, Service Ping, and Postgres Assignment tables (for GitLab.com)
2. Includes billable user data from Seat Link and Service Ping and Postgres Assignment tables (for GitLab.com)
3. Handles both Self-Managed, Dedicated (using dim_installation_id) and GitLab.com (using dim_namespace_id) deployments
4. For Self-Managed instances, preserves data points from both Seat Link and Service Ping when available on the same day

**Other Comments:**
- This model **should not** be used to calculate the total number of billable or license seats across Self-Managed installations, as it will lead to overcounting if a single license key is used for multiple installations, as described [here](https://about.gitlab.com/pricing/licensing-faq/#multiple-instances)
- Self-Managed customers can apply their license key to multiple installations, resulting in the same number of purchased seats for every installation with the same license key applied. However, each of those installations may have a different number of assigned seats. Rather than summing purchased or assigned seats across installations (which would produce an incorrect result), the correct method is to pick one record for each customer. Generally, the preferred method is to pick the production installation.
- Some installations or namespaces may have NULL values, which are excluded from the final output.
- The model is intended for analyzing license and billable user counts at the installation or namespace level for GitLab Duo Pro, GitLab Duo Enterprise and Enterprise Agile Planning add-ons. It can be used to track license utilization, identify renewal risks, and identify upsell opportunities for these products.
- Currently, Enterprise Agile Planning does not have any assigned seats metrics instrumented. As the data for those starts flowing, they will be added to this table.

{% enddocs %}
