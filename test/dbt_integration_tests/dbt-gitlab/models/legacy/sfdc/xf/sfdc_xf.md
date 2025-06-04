{% docs sfdc_accounts_xf %}

This table contains information about individual accounts (organizations and persons), their sales segmentation, and parentage. 

Fields prefixed with TSP_ are related to [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp) and intended as staging fields updated from a variety of data sources, and at given intervals copied over to the "Actual" set of fields for general use. The relationship between "Actual" and TSP fields can be found [here](https://about.gitlab.com/handbook/sales/field-operations/sales-systems/gtm-technical-documentation/). Note that these fields are defined in Salesforce and brought into the data warehouse as-is.

Fields prefixed with CP_ are related to [Command Plan](https://about.gitlab.com/handbook/sales/#opportunity-management-guidelines).

`sales_segment` is the canonized field for an account's [Sales Segment](https://gitlab.my.salesforce.com/00N6100000IOi8o). Note that this is based on the ultimate parent of the account per [Segmentation](https://about.gitlab.com/handbook/business-ops/resources/#segmentation). `account_segment` is used to determine the segmentation of the account according to the same logic.

{% enddocs %}

{% docs sfdc_accounts_xf_col_ultimate_parent_account_id %}

Salesforce Account ID for Ultimate Parent Account

{% enddocs %}

{% docs sfdc_accounts_xf_col_ultimate_parent_account_name %}

Salesforce Account Name for Ultimate Parent Account

{% enddocs %}

{% docs sfdc_lead_xf %}

This table contains information about leads, who are prospects or potential Opportunities.

Fields prefixed with TSP_ are related to [Territory Success Planning](https://about.gitlab.com/handbook/sales/field-operations/sales-operations/go-to-market/#territory-success-planning-tsp) and intended as staging fields updated from a variety of data sources, and at given intervals copied over to the "Actual" set of fields for general use. The relationship between "Actual" and TSP fields can be found [here](https://about.gitlab.com/handbook/sales/field-operations/sales-systems/gtm-technical-documentation/). Note that these fields are defined in Salesforce and brought into the data warehouse as-is.

{% enddocs %}

{% docs sfdc_opportunity_field_historical %}

This model transforms sfdc_opportunity_field_history to construct the history of changes to opportunities prior to 2019-10-01 in the same structure as sfdc_opportunity_snapshot_history.

For each opportunity_id, the state of each field is taken from sfdc_opportunity_snapshot_history at 2019-10-01. The value for each field is backfilled until a corresponding change in sfdc_opportunity_field_history is found, or until the created date of the opportunity in the event of no changes to the field history.

NOTE: Only opportunities that were not deleted (see [Salesforce Documentation](https://help.salesforce.com/articleView?id=home_delete.htm&type=5)) as of 2019-10-01 are included in this model. Hard deleted opportunities are not present in current extracts of opportunity and opportunityfieldhistory, so there is no mechanism for constructing historical records for these opportunities.

{% enddocs %}