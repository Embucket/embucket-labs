{% docs current_arr_segmentation_all_levels %}
This model recognizes that there are three levels of analysis: Zuora Subscription, SFDC Account, and SFDC Ultimate Parent Account. It calculates the MRR for the most recent completed month for which a customer was active. We use the MRR value to calculate ARR. Finally, we use ARR to segment into four categories. We also rank ARR to create a customer rank.

{% enddocs %}

{% docs mrr_totals_levelled %}

This model extends `zuora_mrr_totals` by joining it to SFDC account data through `zuora_account`'s `crm_id` value.  From the account, we get the ultimate parent account information. It now holds details for _every_ level of analysis, including cohort months and cohort quarters for each level and product information.

We want to guarantee that each subscription has a linked SFDC account. Every zuora subscription is linked to a SFDC account through the `crm_id`. This is done in the CTE `initial_join_to_sfdc`. The `LEFT JOIN` can potentially return a null value for the column `sfdc_account_id_int`. SFDC accounts can be deleted/merged, see the doc [here](https://help.salesforce.com/articleView?id=000320023&language=en_US&type=1&mode=1) and hence filtered out from our model `sfdc_accounts_xf`. In this case, the `LEFT JOIN` will return `NULL` values for `sfdc_account_id_int`.  

For the subscriptions that fail to be joined with `sfdc_accounts_xf` in the CTE `initial_join_to_sfdc` because they were referencing a deleted SFDC account, we use the model `sfdc_deleted_accounts` that stores all deleted SFDC accounts and a `sfdc_master_record_id` which is the SFDC account they were merged to. We get this SFDC master record as the SFDC account linked to the subscription.

All retention analyses start at this table.

{% enddocs %}

{% docs retention_sfdc_account_ %}
The process is the same as it is for `retention_parent_account_`. Please visit that documentation for details.
{% enddocs %}

{% docs retention_parent_account_ %}
This models is very similar to `retention_zuora_subscription`, but adjusted for a different level of analysis, primarily in that we need to aggregate `mrr_totals_levelled` appropriately before joining it.

The `list` CTE does exactly that aggregation over `mrr_totals_levelled` creating 1 row per month per parent account. The `retention_subs` CTE joins `list` onto itself to isolate all parent accounts that have MRR that also exist 12 months in the future. The `finals` CTE creates the net retention and gross retention MRR values that will be compared to the original MRR value (MRR 12 months prior to the retention month). Finally we join the culled list of columns to to the ARR Segmentation calculated in the `current_arr_segmentation` model and limit the analysis to completed months.
{% enddocs %}

{% docs retention_reasons_for_retention %}
This model is based on the principles for retention outlined in the zuora_subscription analysis but comparing product category and number of units. The notable difference from previous versions of this analysis is that we are using the principles of subscription lineage, instead of the oldest subscription in cohort value.
{% enddocs %}

{% docs retention_zuora_subscription_ %}

This model is to be used for calculated retention at all levels of analysis for the GitLab organization.

We start at `mrr_totals_levelled`, which is `zuora_mrr_totals` joined to SFDC data so we can aggregate at each of the different levels of our [Customers](https://about.gitlab.com/handbook/finance/operating-metrics/#customers). We aggregate this to the subscription level first.

I will proceed of with the example of `subscription_name_slugify = 'a-s00003114'`. The lineage on it is `a-s00005209,a-s00009998`. The MRR months ran from  2016-01-01 through 2016-12-01.

The `list` CTE produces a flattened version of the lineage values. The 12 rows flatten into 24 (because there are two subscriptions in the lineage).

|ORIGINAL_SUB|SUBSCRIPTIONS_IN_LINEAGE|ORIGINAL_MRR_MONTH|RETENTION_MONTH|
|------|------|------|------|
|a-s00003114|a-s00005209|2016-01-01|2017-01-01|
|a-s00003114|a-s00009998|2016-01-01|2017-01-01|

The approach here is that we will be looking for an active subscription for each of the subscriptions in the lineage in the retention month. If it doesn't exist, we don't need it, but if it does exist, we will need it for the retention analysis. We actually make this join in `retention_subs` CTE. We inner join `mrr_totals_levelled` to `list` to isolate the subscription values that exist at their retention months AND get their MRR amounts.

|ORIGINAL_SUB|RETENTION_MONTH|ORIGINAL_MRR_MONTH|RETENTION_MRR|
|------|------|------|------|
|a-s00003114|2017-08-01|2016-08-01|650|
|a-s00003114|2017-09-01|2016-09-01|650|
|a-s00003114|2017-10-01|2016-10-01|650|
|a-s00003114|2017-11-01|2016-11-01|650|
|a-s00003114|2017-12-01|2016-12-01|650|

Once we have isolated these renewal subscriptions that exist in the future, we can compare the retention subscriptions with subscriptions. This looks like (Some columns have been excluded from this example):

|NET_RETENTION_MRR|GROSS_RETENTION_MRR|RETENTION_MONTH|MRR_MONTH|
|------|------|------|------|
|0|0||2016-01-01|408.333333333|
|0|0||2016-02-01|408.333333333|
|0|0||2016-03-01|408.333333333|
|0|0||2016-04-01|408.333333333|
|0|0||2016-05-01|408.333333333|
|0|0||2016-06-01|408.333333333|
|0|0||2016-07-01|408.333333333|
|650|408.333333333|2017-08-01|2016-08-01|408.333333333|
|650|408.333333333|2017-09-01|2016-09-01|408.333333333|
|650|408.333333333|2017-10-01|2016-10-01|408.333333333|
|650|408.333333333|2017-11-01|2016-11-01|408.333333333|
|650|408.333333333|2017-12-01|2016-12-01|408.333333333|


Notably, this CTE distiguishes gross & net retention numbers, based on their relationship to MRR.

The final select statement does no additional transformation, except for anchoring the analysis to retention month (as opposed to mrr_month), and reordering columns.

The final result for this subscription is:

|RETENTION_MONTH|ORIGINAL_MRR|GROSS_RETENTION_MRR|NET_RETENTION_MRR|
|------|------|------|------|
|2017-01-01|408.333333333|0|0|
|2017-02-01|408.333333333|0|0|
|2017-03-01|408.333333333|0|0|
|2017-04-01|408.333333333|0|0|
|2017-05-01|408.333333333|0|0|
|2017-06-01|408.333333333|0|0|
|2017-07-01|408.333333333|0|0|
|2017-08-01|408.333333333|408.333333333|650|
|2017-09-01|408.333333333|408.333333333|650|
|2017-10-01|408.333333333|408.333333333|650|
|2017-11-01|408.333333333|408.333333333|650|
|2017-12-01|408.333333333|408.333333333|650|

Like the `mrr_totals_levelled` analysis, this analysis can be aggregated at the Zuora subscription, Zuora Account, SFDC Account, or SFDC Ultimate Parent Account level.

{% enddocs %}
