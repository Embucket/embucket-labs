{% docs prep_charge_mrr %}

Prep model that takes prep_charge and perform the necessary filtering of the model to later be used to derive fct_mrr.

{% enddocs %}

{% docs prep_crm_account %}

Prep model for the live crm account dimension.

{% enddocs %}

{% docs prep_crm_opportunity %}

Prep model for the live crm opportunity dimension and fact models. This model is refreshed on a six hourly schedule using the `dbt_six_hourly` airflow DAG.

{% enddocs %}

{% docs prep_subscription_opportunity_mapping %}

 Prep table for the mapping table for subscriptions to opportunities. This incorporates logic from previous processes which includes taking opportunity-subscription connections from quotes, invoices, and the subscription object itself. Based on the created date, term dates, or subscription name, we fill in missing opportunity ids.

 The process is described in this [flow diagram](https://lucid.app/lucidchart/e7661694-61ed-4317-b648-d054be9aff0e/edit?viewport_loc=-76%2C296%2C3590%2C1856%2C0_0&invitationId=inv_f50ea2e1-1ea8-47ca-b950-75b723273b00)

 The final result yields the distinct combination of subscriptions and opportunities with the best match based on the rules defined in the flow diagram.

 For self-service subscriptions:
 - Take the opportunity_id from the subscription object
 - Using the quote_number from the subscription's invoice, get the opportunity_id from the opportunity which has that quote_number on it.
 - Out of all quotes, take any opportunity_id where the quote has the subscription_id on it.
 - Fill forward/backwards when the subscription created dates are the same.
 - Fill forward/backwards when the subscription term dates are the same.
 - Fill forward when the subscription name is the same.

 For sales-assisted subscriptions:
 - Take the opportunity_id from the subscription object if it was created after 2021-04-11. This is when automation was set up to reliably relate subscriptions and opportunities.
 - Using the quote_number from the subscription object, get the opportunity_id from the opportunity which has that quote_number on it.
 - Using the quote_number from the subscription's invoice, get the opportunity_id from the opportunity which has that quote_number on it.
 - Out of all quotes, take any opportunity_id where the quote has the subscription_id on it.
 - Fill forward/backwards when the subscription created dates are the same.
 - Fill forward/backwards when the subscription term dates are the same.
 - Fill forward when the subscription name is the same.

 In both cases there where multiple opportunities are assigned to a single subscription. This can be caused by a variety of reasons, and we apply the following logic to all subscriptions to determine which opportunity is the best match:
 - Select the opportunity whith the amount that matches the amount on the subscription's invoice. This is the best solution for when a subscription is one of many on an invoice, and we need to match up each subscription with the appropriate opportunity.
 - Select the opportunity created first if all of the possible opportunities' amounts sums to the amount on the subscription's invoices. This is the best solution for when a subscription is billed on multiple invoices and new opportunity is created for each invoice. Ex. Ramp deals
 - If the subscription is self-service and has an opportunity_id on the subscription object, take the opportunity_id from the subscription object.
 - If the subscription is sales_assisted and there is a quote_number on the subscription, take the opportunity_id from the opportunity associated with this quote_number.
 - If all of the methods for finding an opportunity-subscription mapping match for one record, select this subscription-opportunity pair.

{% enddocs %}


{% docs prep_lead %}

This model is used to store hand raise and trial leads sourced from leads table(tap postgres) from customers.gitlab.com.

{% enddocs %}

{% docs prep_sales_dev_user_hierarchy %}

Prep model that captures a snapshotted history of the Sales Dev Org's user hierarchy with appropriate dimensions. 

{% enddocs %}

{% docs prep_charge_mrr_daily %}

This model can be used to determine what products/MRR were associated with a subscription on a given date.

In order to find the charges associated with a subscription, filter to the most recent subscription version for the original subscription/subscription name. To expand this to the daily grain, fan out the most recent subscription version by the effective dates of the charges.
This represents the actual effective dates of the products, as updated with each subscription version and carried through as
a history in the charges.

{% enddocs %}

{% docs prep_charge_mrr_daily_with_namespace_and_installation %}

This model extends prep_charge_mrr_daily by adding namespace and installation identifiers to create a unified daily MRR dataset.

It can be used to determine what products/MRR were associated with a subscription, namespace, or installation on a given date. The model combines subscription data with both GitLab.com (namespace) and Self-Managed/Dedicated (installation) identifiers, streamlining the data lineage for downstream mapping models.

To find charges associated with a subscription, filter to the most recent subscription version for the original subscription/subscription name. The model fans out the most recent subscription version by the effective dates of the charges, representing the actual effective dates of the products as updated with each subscription version.

{% enddocs %}

{% docs prep_crm_opportunity_calcs %}

This model applies all calculated fields and internal transformations to the source opportunity data, ensuring standardized data definitions and calculations as a foundation for downstream modeling. Joins with other tables are excluded at this stage to focus on opportunity data transformations. 

This model pulls all fields from the `sfdc_opportunity_snapshots_source` table, so any new fields added to that table will automatically be included. Avoid adding fields directly to this model unless a specific transformation is required. Fields can be added to the exclude list in the `star` macro to prevent it from being pulled in.

{% enddocs %}

{% docs prep_crm_opportunity_daily %}

This model fans out opportunity data to a daily grain, creating a row for each day an opportunity is valid.

It expands the data from `opportunity_id | dbt_valid_from | dbt_valid_to` to `opportunity_id | snapshot_date`.

{% enddocs %}

{% docs prep_crm_opportunity_live %}

This model combines the most recent (live) data for each opportunity and account with opportunity historical records. It serves as an intermediate step in the opportunity data preparation pipeline.

{% enddocs %}