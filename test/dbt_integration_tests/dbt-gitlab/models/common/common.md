{% docs bdg_crm_opportunity_contact_role %}

A fact table bridging opportunities with contacts. One opportunity can have multiple contacts and one can be flagged as the primary.

{% enddocs %}

{% docs bdg_epic_user_request %}

A bridge table that connects epics user requests, these epics being in the `Gitlab-org` group, with SFDC Opportunities / Accounts and Zendesk tickets links. It also picks the priorities that have been assigned to the epic request.

This table combines the requests that were done directly in the `Gitlab-org` group by pasting the SFDC / Zendesk links directly in the epic description / notes (`prep_epic_user_request`), with the requests that were done by pasting the epic links in the customer collaboration projects (`prep_epic_user_request_collaboration_project`). If the combination of epic and link is found in both the `Gitlab-org` group and the customer collaboration project, the `Gitlab-org` will take precedence. If the request is only in the customer collaboration project then the flag `is_user_request_only_in_collaboration_project` will be equal to `True`.

{% enddocs %}

{% docs bdg_issue_user_request %}

A bridge table that connects issues user requests, these issues being in the `Gitlab-org` group, with SFDC Opportunities / Accounts and Zendesk tickets links. It also picks the priorities that have been assigned to the issue request.

This table combines the requests that were done directly in the `Gitlab-org` group by pasting the SFDC / Zendesk links directly in the issue description / notes (`prep_issue_user_request`), with the requests that were done by pasting the issue links in the customer collaboration projects (`prep_issue_user_request_collaboration_project`). If the combination of issue and link is found in both the `Gitlab-org` group and the customer collaboration project, the `Gitlab-org` will take precedence. If the request is only in the customer collaboration project then the flag `is_user_request_only_in_collaboration_project` will be equal to `True`.

{% enddocs %}

{% docs bdg_namespace_order_subscription_monthly %}

The purpose of this table is three-fold:
1. Connect **Ultimate Parent** Namespace ID to Subscription (and hence Zuora billing account and CRM Account)
2. Connect Customer DB Customer ID to Subscription for self managed purchases. This helps with marketing efforts.
3. Provide a historical record the above connections by month.

This table expands the functionality of the orders by improving the join to ultimate parent namespaces and subscriptions. Namespaces are listed in this table with prior trials and currently paid plans. Subscriptions listed in this table are all SaaS (determined by the `product_rate_plan_id` from `zuora_rate_plan_source`) and the `is_active_subscription` column can be used to filter to subscription that are currently active (status is Active or Cancelled with a recurring charge in the current month). Orders in this table are all SaaS (determined by the `product_rate_plan_id` from `customers_db_orders_source`) and the `is_active_order` column can be used to filter to orders that are currently active (`order_end_date` is NULL or greater than the date that this table was refreshed).

The tier(s) connected to the subscription are determined using the underlying Zuora recurring charges. This view uses a `FULL OUTER JOIN` to show all three sides of the Venn diagram. (namespace, orders, subscriptions)
In doing so exceptions are noted within `namespace_order_subscription_match_status` to identify rows that do not match between systems.

{% enddocs %}

{% docs bdg_namespace_order_subscription %}

The purpose of this table is two-fold:
1. Connect **Ultimate Parent** Namespace ID to Subscription (and hence Zuora billing account and CRM Account)
2. Connect Customer DB Customer ID to Subscription for self managed purchases. This helps with marketing efforts.

This table expands the functionality of the orders by improving the join to ultimate parent namespaces and subscriptions. Namespaces are listed in this table with prior trials and currently paid plans. Subscriptions listed in this table are all SaaS (determined by the `product_rate_plan_id` from `zuora_rate_plan_source`) and the `is_active_subscription` column can be used to filter to subscription that are currently active (status is Active or Cancelled with a recurring charge in the current month). Orders in this table are all SaaS (determined by the `product_rate_plan_id` from `customers_db_orders_source`) and the `is_active_order` column can be used to filter to orders that are currently active (`order_end_date` is NULL or greater than the date that this table was refreshed).

The tier(s) connected to the subscription are determined using the underlying Zuora recurring charges. This view uses a `FULL OUTER JOIN` to show all three sides of the Venn diagram. (namespace, orders, subscriptions)
In doing so exceptions are noted within `namespace_order_subscription_match_status` to identify rows that do not match between systems.

{% enddocs %}

{% docs bdg_self_managed_order_subscription %}

The purpose of this table to connect Order IDs from Customer DB to Subscription for Self-Managed and SaaS Dedicated purchases. This table expands the functionality of the subscriptions by improving the join to orders. Subscriptions listed in this table are all Self-Managed (determined by the `product_rate_plan_id` from `zuora_rate_plan_source`) and the `is_active_subscription` column can be used to filter to subscription that are currently active (status is Active or Cancelled with a recurring charge in the current month). Orders in this table are all Self-Managed (determined by the `product_rate_plan_id` from `customers_db_orders_source`) and the `is_active_order` column can be used to filter to orders that are currently active (`order_end_date` is NULL or greater than the date that this table was refreshed).

The tier(s) connected to the subscription are determined using the underlying Zuora recurring charges. This view uses a `FULL OUTER JOIN` to show all three parts of the Venn diagram (orders, subscriptions, and the overlap between the two).In doing so exceptions are noted within `order_subscription_match_status` to identify rows that do not match between systems.

{% enddocs %}

{% docs bdg_subscription_product_rate_plan %}
The goal of this table is to build a bridge from the entire "universe" of subscriptions in Zuora (`zuora_subscription_source` without any filters applied) to all of the [product rate plans](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plan) to which those subscriptions are mapped. This provides the ability to filter subscriptions by delivery type ('SaaS' or 'Self-Managed') and deployment_type ('GitLab.com', 'Dedicated' or 'Self-Managed').

{% enddocs %}

{% docs dim_accounting_event %}

Events from Zuora Revpro. The current iteration includes performance obligation events, but will eventually include hold events as well.

{% enddocs %}

{% docs dim_accounting_type %}

Model to map revenue from Zuora Revenue to the appropriate account (revenue, contract liability, etc.) per accounting practices.

{% enddocs %}

{% docs dim_alliance_type_scd %}
[Slowly changing dimension type 2](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-2/#:~:text=Slowly%20changing%20dimension%20type%202,multiple%20rows%20describing%20each%20member.) to identify Channel partners groupings. Can be joined to either `dim_alliance_type_id` to get the historical information on channel partners or to `dim_alliance_type_current_id` to get the most recent state of channel partners.

Technology Partners are identified and discussed in the handbook link referenced below. The specific groupings to report out on were determined by FP&A and Sales Analytics.

[Technology Partners Handbook Reference](https://about.gitlab.com/handbook/alliances/#technology-partners)

{% enddocs %}

{% docs dim_crm_account %}
Dimensional customer table representing all existing and historical customers from SalesForce. There are customer definitions for external reporting and additional customer definitions for internal reporting defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer).

The Customer Account Management business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#1-customer-account-management-and-conversion-of-lead-to-opportunity).

The grain of the table is the SalesForce Account, also referred to as `DIM_CRM_ACCOUNT_ID`.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_crm_account_daily_snapshot %}
Dimensional customer table representing all existing and historical customers from SalesForce and their attributes on a given day. There are customer definitions for external reporting and additional customer definitions for internal reporting defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer).

The Customer Account Management business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#1-customer-account-management-and-conversion-of-lead-to-opportunity).

The grain of the table is the SalesForce Account and day, also referred to as `CRM_ACCOUNT_SNAPSHOT_ID`, which is a combination of the `DIM_CRM_ACCOUNT_ID` and `DIM_DATE_ID`

{% enddocs %}

{% docs dim_crm_command_plan %}
A dimension table containing command plan fields from CRM opportunities. Command plans track key aspects of the sales 
process including discovery, stakeholder information, decision criteria, and risk assessment. Each record maps to an 
opportunity and captures the state of various sales methodology checkpoints, helping teams track deal progression and 
health. The fields are primarily notes taken by sales team members in our CRM. See Handbook here https://handbook.gitlab.com/handbook/sales/command-of-the-message/command-plan/

{% enddocs %}

{% docs dim_crm_touchpoint %}
Descriptive fields for both attribution and non-attribution Bizible touchpoints.

{% enddocs %}

{% docs dim_crm_opportunity %}
Model for all dimensional opportunity columns from salesforce opportunity object. This model is refreshed on a six hourly schedule using the `dbt_six_hourly` airflow DAG.

{% enddocs %}



{% docs dim_crm_opportunity_deal %}
Contains information on sales opportunities related to deal size and category.

{% enddocs %}

{% docs dim_crm_opportunity_flags %}

A junk dimension containing boolean flags related to CRM opportunities. This dimension consolidates various opportunity 
attributes including status, deal classification, pipeline metrics, and eligibility criteria into a single table to 
reduce the number of columns in the fact table. The model takes distinct combinations of these flags from 
prep_crm_opportunity and assigns a surrogate key to each unique combination.

This table serves as a reference for opportunity attributes to provide additional context and filtering capabilities.

{% enddocs %}

{% docs dim_crm_opportunity_partner %}

Contains information about partners that assisted in either bringing in or closing an opportunity.

{% enddocs %}

{% docs dim_crm_opportunity_source_and_path %}
Contains columns for opportunities indicating where the opportunity came from in terms of campaign and source.

{% enddocs %}

{% docs dim_crm_person %}
Dimension that combines demographic data from salesforce leads and salesforce contacts. They are combined with a union and a filter on leads excluding converted leads and leads where there is a corresponding contact.

{% enddocs %}

{% docs dim_crm_user %}

Dimension representing the associated user from salesforce. Most often this will be the record owner, which is a ubiquitous field in salesforce.

{% enddocs %}

{% docs dim_crm_user_daily_snapshot %}

Dimension representing the associated user from salesforce on any day.

The grain of this table is `DIM_CRM_USER_SNAPSHOT_ID` which is a combination of `DIM_CRM_USER_ID` and `DIM_DATE_ID`.

{% enddocs %}

{% docs dim_crm_user_hierarchy %}
Dimension table representing the sales hierarchy at the time of a closed opportunity, including the user segment. These fields are stamped on the opportunity object on the close date and are used in sales funnel analyses.

{% enddocs %}

{% docs dim_billing_account %}
Dimensional table representing each individual Zuora account with details of person to bill for the account.

The Zuora account creation and maintenance is part of the broader Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#3-quote-creation).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Accounts).

The grain of the table is the Zuora Account.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_hold %}

There are multiple kinds of holds which can be applied to a transaction in the accounting process. This dimension lists the distinct types of holds which may be applied in a revenue contract.

{% enddocs %}

{% docs dim_invoice %}

Dimension table providing invoice details at the single invoice grain.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

{% enddocs %}

{% docs dim_location_country %}

Dimensional table for countries mapped to larger regions.

{% enddocs %}

{% docs dim_location_region %}

Dimensional table for geographic regions.

{% enddocs %}

{% docs dim_manual_journal_entry_header %}
High-level details of manual updates made to adjust final totals in accounting reporting.

{% enddocs %}

{% docs dim_manual_journal_entry_line %}

Line-level details of manual updates made to adjust final totals in accounting reporting. This can be mapped directly to a performance obligation in a revenue contract line.

{% enddocs %}

{% docs dim_product_detail %}
Dimensional table representing GitLab's Product Catalog. The Product Catalog is created and maintained through the Price Master Management business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#2-price-master-management).

The Rate Plan Charge that is created on a customer account and subscription inherits its value from the Product Catalog.

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plan-Charges).

The grain of the table is the Product Rate Plan Charge.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_product_tier %}
Dimensional table representing [GitLab Tiers](https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/). Product [delivery type](https://about.gitlab.com/handbook/marketing/strategic-marketing/tiers/#delivery) and ranking are also captured in this table.

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Product-Rate-Plans).

The grain of the table is the Product Tier Name.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_project %}
Dimensional table representing [GitLab Projects](https://docs.gitlab.com/ee/user/project/). Parent ID (dim_namespace_id) and Ultimate Parent ID (dim_ultimate_parent_id) are also stored in the table

Data comes from [Gitlab Postgres db](https://gitlab.com/gitlab-org/gitlab/-/blob/master/db/structure.sql).

The grain of the table is the Project ID.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_revenue_contract%}

This model contains high-level attributes for all revenue contracts. These can be connected to the corresponding revenue contract lines.

{% enddocs %}

{% docs dim_revenue_contract_hold%}

This model contains attributes for all holds applied to revenue contracts.

{% enddocs %}

{% docs dim_revenue_contract_line%}

This model contains attributes for all revenue contract line items.

{% enddocs %}

{% docs dim_revenue_contract_performance_obligation %}

This model contains attributes for performance obligations that are tied to a revenue contract line.

{% enddocs %}

{% docs dim_revenue_contract_schedule %}

An accounting schedule defines when the company will recognize the revenue of the performance obligation tied to a line in a revenue contract. This model contains the attributes of the schedule that is connected to a give line item.

{% enddocs %}

{% docs dim_subscription %}
Dimension table representing subscription details. The Zuora subscription is created and maintained as part of the broader Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#3-quote-creation).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Subscriptions).

The grain of the table is the version of a Zuora subscription.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_date %}
Dimensional table representing both calendar year and fiscal year date details.

The grain of the table is a calendar day.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_dr_partner_engagement %}
Model to identify the type of business engagement relationship a Partner has with GitLab. The Partner definitions are discussed in the handbook.

[Partner Definitions Handbook Reference](https://about.gitlab.com/handbook/alliances/#partner-definitions)

{% enddocs %}

{% docs fct_available_to_renew %}

Captures ATR(available to renew) amounts for all the Subscriptions. ATR refers to what/how much is up for renewal in a given month (whether or not it actually ends up renewing).

{% enddocs %}

{% docs fct_campaign %}

Fact table representing marketing campaign details tracked in SFDC.

{% enddocs %}

{% docs fct_crm_account %}
Factual customer table representing all existing and historical customers from SalesForce. There are customer definitions for external reporting and additional customer definitions for internal reporting defined in the [handbook](https://about.gitlab.com/handbook/sales/#customer).

The Customer Account Management business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#1-customer-account-management-and-conversion-of-lead-to-opportunity).

The grain of the table is the SalesForce Account, also referred to as `DIM_CRM_ACCOUNT_ID`.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_crm_attribution_touchpoint %}
Fact table for attribution Bizible touchpoints with shared dimension keys relating these touchpoints to dim_crm_person, dim_crm_opportunity, and dim_crm_account. These touchpoints have revenue associated with them.

{% enddocs %}

{% docs fct_crm_touchpoint %}
Fact table for non-attribution Bizible touchpoints with shared dimension keys relating these touchpoints to dim_crm_person and dim_crm_account.

{% enddocs %}

{% docs fct_crm_opportunity %}

A fact table for salesforce opportunities with keys to connect opportunities to shared dimensions through the attributes of the crm account. This model is refreshed on a six hourly schedule using the `dbt_six_hourly` airflow DAG.

{% enddocs %}

{% docs fct_crm_opportunity_v2 %}

The second version of the opportunity fact table. This fact table for salesforce opportunities has keys to connect opportunities to shared dimensions through the attributes of the crm account. This model is refreshed on a six hourly schedule using the `dbt_six_hourly` airflow DAG. 

{% enddocs %}

{% docs fct_crm_opportunity_daily_snapshot %}

A daily snapshot fact table for salesforce opportunities with keys to connect opportunities to shared dimensions. This table shows the state of the opportunity as it was on the day of the snapshot.

{% enddocs %}

{% docs fct_crm_person %}

A fact table for Salesforce unconverted leads and contacts. The important stage dates have been included to calculate the velocity of people through the sales funnel. A boolean flag has been created to indicate leads and contacts who have been assigned a Marketo Qualified Lead Date, and a Bizible person id has been included to pull in the marketing channel based on the first touchpoint of a given lead or contact.

{% enddocs %}

{% docs fct_invoice %}

Fact table providing invoice details at the single invoice grain.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

{% enddocs %}

{% docs fct_invoice_item %}
Fact table providing invoice line item details.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Billing/Reporting_and_Analytics/D_Data_Sources_and_Exports/C_Data_Source_Reference/Invoice_Item_Data_Source).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_invoice_payment %}

Fact table providing invoice payment details at the single grain of a payment applied to an invoice.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Zuora_Payments/Payment_Operations/AA_payment_operation_overview).

{% enddocs %}

{% docs fct_refund_invoice_payment %}

Fact table providing refund invoice payment details at the single grain of a refund made on an invoice.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Zuora_Central_Platform/Reporting/D_Data_Sources_and_Exports/C_Data_Source_Reference/Refund_Invoice_Payment_Data_Source).

{% enddocs %}

{% docs fct_refund %}

Fact table providing refund details made on an invoice or billing account.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Zuora_Payments/Payment_Operations/CC_Refunds).

{% enddocs %}

{% docs fct_credit_balance_adjustment %}

Fact table providing credit balance adjustment details made on an invoice or billing account.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Zuora_Central_Platform/API/G_SOAP_API/E1_SOAP_API_Object_Reference/CreditBalanceAdjustment).

{% enddocs %}

{% docs fct_invoice_item_adjustment %}

Fact table providing invoice item adjustment details. One invoice may have several invoice items. The grain is the item adjustment.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Zuora_Billing/Bill_your_customers/Adjust_invoice_amounts/Invoice_Item_Adjustments/AA_Overview_of_Invoice_Item_Adjustments).

{% enddocs %}

{% docs fct_payment %}

Fact table providing payment details at the single grain of a payment received for a single invoice or multiple invoices.

The invoicing to customers business process can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://knowledgecenter.zuora.com/Zuora_Payments/Payment_Operations/AA_payment_operation_overview).

{% enddocs %}

{% docs fct_charge %}
Factual table with all rate plan charges coming from subscriptions or an amendment to a subscription.

Rate Plan Charges are created as part of the Quote Creation business process and can be found in the [handbook](https://about.gitlab.com/handbook/finance/sox-internal-controls/quote-to-cash/#6-invoicing-to-customers).

Data comes from [Zuora Documentation](https://www.zuora.com/developer/api-reference/#tag/Rate-Plan-Charges).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_gitlab_dotcom_gitlab_emails %}
Dimensional table representing the best email address for GitLab employees from the GitLab.com data source

The grain of the table is a GitLab.com user_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}


{% docs dim_gitlab_ops_gitlab_emails %}
Dimensional table representing the best email address for GitLab team members from the Ops.GitLab.Net data source using the gitlab email address to identify GitLab team members

The grain of the table is a Ops.GitLab.Net user_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_gitlab_releases %}
Dimensional table representing released versions of GitLab.

The grain of the table is a major_minor_version.

Additional information can be found on the [GitLab Releases](https://about.gitlab.com/releases/categories/releases/) page.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_app_release_major_minor %}
Dimensional table representing released versions (major and minor) of GitLab. Here `app` stands for application.

The grain of the table is a major_minor_version.

Additional information can be found on the [GitLab Releases](https://about.gitlab.com/releases/categories/releases/) page.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_app_release %}
Dimensional table representing released versions of an application (app). Currently, it only holds releases from GitLab.

The grain of the table is the major, minor and patch version together with the application that these represent.

Additional information specific to the GitLab Releases can be found in the following [page](https://about.gitlab.com/releases/categories/releases/).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_manual_journal_entry_line %}

A fact table of manual journal entry lines which can be connected to a revenue contract line or revenue contract header. These are adjustments made manually as part of the accounting process.

{% enddocs %}

{% docs fct_quote_item %}

A fact table of quote amendments which have quotes and product rate plan charges associated with them. This model connected opportunities to quotes, quote amendments, and products.

{% enddocs %}

{% docs fct_quote %}

Fact table representing quotes pulled from the Zuora billing system. These are associated with crm accounts, billing accounts, opportunities, and subscriptions.

{% enddocs %}

{% docs fct_revenue_contract_hold %}

Details of holds placed on revenue contracts. In the future this will also connect to revenue contract lines that have been placed on hold, but the business does not currently operate this way.

{% enddocs %}

{% docs fct_revenue_contract_line %}
Revenue contract line details including the transaction amount, functional amount, and connections to subscription, performance obligation, crm account, and product details.

{% enddocs %}

{% docs fct_revenue_contract_schedule %}

Schedule showing when revenue will be recognized for all performance obligations connected to a given revenue contract line.

{% enddocs %}

{% docs fct_sales_funnel_actual %}

Actual sales funnel values are presented in the following table, designed to conform to the format of fct_sales_funnel_target, where the format is defined as `target_date || kpi_name`.

This table is derived from the data in both `fct_crm_person` and `fct_crm_opportunity`, with each metric associated with its corresponding date and sales hierarchy. For instance, when dealing with the KPI `Net ARR`, we use the `close_date` as the date field, employ the stamped user hierarchy for the sales hierarchy, and apply a filter based on the `is_net_arr_closed_deal` flag, selecting only opportunities related to Net ARR.

{% enddocs %}

{% docs fct_sales_funnel_target %}

Sales funnel targets set by the Finance team to measure performance of important KPIs against goals, broken down by sales hierarchy, and order attributes.

{% enddocs %}

{% docs fct_sales_funnel_target_daily %}

Derived fact table from `fct_sales_funnel_target` that transforms the monthly targets specified in the aforementioned model into daily targets. This transformation is achieved by dividing the monthly target by the number of days in the corresponding month.

{% enddocs %}


{% docs fct_usage_ci_minutes %}

This table replicates the Gitlab UI logic that generates the CI minutes Usage Quota for both personal namespaces and top level group namespaces. The codebase logic used to build this model can be seen mapped in [this diagram](https://app.lucidchart.com/documents/view/0b8b66e6-8536-4a5d-b992-9e324581187d/0_0).

Namespaces from the `namespace_snapshots_monthly_all` CTE that are not present in the `namespace_statistics_monthly_all` CTE are joined into the logic with NULL `shared_runners_seconds` since these namespaces have not used CI Minutes on GitLab-provided shared runners. Since these CI Minutes are neither trackable nor monetizable, they can be functionally thought of as 0 `shared_runners_minutes_used_overall`. The SQL code has been implemented with this logic as justification.

It also adds two additional columns which aren't calculated in the UI, which are `limit_based_plan` and `status_based_plan` which are independent of whether there aren't projects with `shared_runners_enabled` inside the namespaces and only take into account how many minutes have been used from the monthly quota based in the plan of the namespace.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_product_usage_wave_1_3_metrics_latest %}
This table builds on the set of all Zuora subscriptions that are associated with a **Self-Managed** rate plans. Seat Link data from Customers DB (`fct_usage_self_managed_seat_link`) are combined with high priority Usage Ping metrics (`prep_usage_ping_subscription_mapped_wave_2_3_metrics`) to build out the set of facts included in this table. Only the most recently received Usage Ping and Seat Link per `dim_subscription_id` payload are reported included.

The data from this table will be used to create a mart table (`mart_product_usage_wave_1_3_metrics_latest`) for Gainsight Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_saas_product_usage_metrics_monthly %}
This table builds on the set of all Zuora subscriptions that are associated with a **SaaS GitLab.com** rate plans. Historical namespace seat charges and billable user data (`gitlab_dotcom_gitlab_subscriptions_snapshots_namespace_id_base`) are combined with high priority Usage Ping metrics (`prep_saas_usage_ping_subscription_mapped_wave_2_3_metrics`) to build out the set of facts included in this table. Only the most recently collected namespace "Usage Ping" and membership data per `dim_subscription_id` each month are reported in this table.

The data from this table will be used to create a mart table (`mart_saas_product_usage_monthly`) for Gainsight Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

**Note**: This model DOES NOT include data from SaaS Dedicated instances, as the grain of the data is different (namespace_id vs instance_id). From a data perspective, SaaS Dedicated, is more closely related to Self-Managed data.

{% enddocs %}

{% docs fct_event_valid %}

**Description:** Atomic level GitLab.com usage event data with only valid events
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.

**Data Grain:**
- event_pk

**Filters Applied to Model:**
- Include valid events for standard analysis and reporting:
  - Exclude events where the event created date < the user created date (`days_since_user_creation_at_event_date >= 0`)
    - These are usually events from projects that were created before the GitLab.com user and then imported after the user is created
  - Exclude events from blocked users (based on the current user state)
- Rolling 36 months of data

**Business Logic in this Model:**
- A namespace's plan information (ex: `plan_name_at_event_date`) is determined by the plan for the last event on a given day
- The ultimate parent namespace's subscription, billing, and account information (ex: `dim_latest_subscription_id`) reflects the most recent available attributes associated with that namespace
- `dim_latest_product_tier_id` reflects the _current_ product tier of the namespace
- Not all events have a user associated with them (ex: 'milestones'), and not all events have a namespace associated with them (ex: 'users_created'). Therefore it is expected that `dim_user_sk` or `dim_ultimate_parent_namespace_id` will be NULL for these events
- `section_name`, `stage_name`, `group_name`, and xMAU metric flags (ex: `is_gmau`) are based on the _current_ event mappings and may not match the mapping at the time of the event

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_event %}

**Description:** Atomic level GitLab.com usage event data
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.
- Atomic (lowest grain) data

**Data Grain:**
- event_pk

**Filters Applied to Model:**
- None - `ALL Data` at the Atomic (`lowest level/grain`) is brought through from the source for comprehensive analysis

**Business Logic in this Model:**
- None - this model does not contain any business logic

**Other Comments:**
- The `fct_event` table is built directly from the [prep_event lineage](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_event) which brings all of the different types of events together. A handbook page on this table can be found [here](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-product-events-data/) .
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_event_daily %}

**Description:** GitLab.com usage event data for valid events, grouped by date, user, ultimate parent namespace, and event name

The purpose of this table is to serve as an intermediary table between the upstream `fct_event` table, and the downstream `fct_event_user_daily`, `fct_event_namespace_daily`, and `fct_event_namespace_monthly` tables.

This table has the exact same grain as the `fct_event_user_daily` table, but it includes NULL users as well, as these are used in the downstream namespace tables.

**Data Grain:**
- event_date
- dim_user_id
- dim_ultimate_parent_namespace_id
- event_name

{% enddocs %}

{% docs fct_event_user_daily %}

**Description:** GitLab.com usage event data for valid events, grouped by date, user, ultimate parent namespace, and event name
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.

**Data Grain:**
- event_date
- dim_user_id
- dim_ultimate_parent_namespace_id
- event_name

**Filters Applied to Model:**
- `Inherited` - Include valid events for standard analysis and reporting:
  - Exclude events where the event created date < the user created date (`days_since_user_creation_at_event_date >= 0`)
    - These are usually events from projects that were created before the GitLab.com user and then imported after the user is created
  - Exclude events from blocked users (based on the current user state)
- Rolling 24 months of data
- Exclude events not associated with a user (ex: 'milestones')

**Business Logic in this Model:**
- `Inherited` - A namespace's plan information (ex: `plan_name_at_event_date`) is determined by the plan for the last event on a given day
- `Inherited` - The ultimate parent namespace's subscription, billing, and account information (ex: `dim_latest_subscription_id`) reflects the most recent available attributes associated with that namespace
- `Inherited` - `dim_latest_product_tier_id` reflects the _current_ product tier of the namespace
- `Inherited` - `section_name`, `stage_name`, `group_name`, and xMAU metric flags (ex: `is_gmau`) are based on the _current_ event mappings and may not match the mapping at the time of the event

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_event_namespace_daily %}

**Description:** GitLab.com usage event data for valid events, grouped by date, event name, and ultimate parent namespace
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.

**Data Grain:**
- event_date
- event_name
- dim_ultimate_parent_namespace_id

**Filters Applied to Model:**
- `Inherited` - Include valid events for standard analysis and reporting:
  - Exclude events where the event created date < the user created date (`days_since_user_creation_at_event_date >= 0`)
    - These are usually events from projects that were created before the GitLab.com user and then imported after the user is created
  - Exclude events from blocked users (based on the current user state)
- Rolling 24 months of data
- Exclude events not associated with a namespace (ex: 'users_created')

**Business Logic in this Model:**
- `Inherited` - A namespace's plan information (ex: `plan_name_at_event_date`) is determined by the plan for the last event on a given day
- `Inherited` - The ultimate parent namespace's subscription, billing, and account information (ex: `dim_latest_subscription_id`) reflects the most recent available attributes associated with that namespace
- `Inherited` - `dim_latest_product_tier_id` reflects the _current_ product tier of the namespace
- `Inherited` - `section_name`, `stage_name`, `group_name`, and xMAU metric flags (ex: `is_gmau`) are based on the _current_ event mappings and may not match the mapping at the time of the event

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_usage_ping_metric_all_time %}
Factual table on the grain of an individual metric received as part of a usage ping payload.  This model specifically includes only metrics that represent usage over the entire lifetime of the instance.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_self_managed_seat_link %}

Self-managed EE instances send [Seat Link](https://docs.gitlab.com/ee/subscriptions/self_managed/#seat-link) usage data to [CustomerDot](https://gitlab.com/gitlab-org/customers-gitlab-com) on a daily basis. This information includes a count of active users and a maximum count of users historically in order to assist the [true up process](https://docs.gitlab.com/ee/subscriptions/self_managed/#users-over-license). Counts are reported from the last day of the month for historical months, and the most recent `reported_date` for the current month. Additional details can be found in [this doc](https://gitlab.com/gitlab-org/customers-gitlab-com/-/blob/staging/doc/reconciliations.md).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_usage_storage %}
This table replicates the Gitlab UI logic that generates the Storage Usage Quotas for top level group namespaces. The logic used to build this model is explained in [this epic](https://gitlab.com/groups/gitlab-org/-/epics/4237). The specific front end logic is described [here](https://gitlab.com/groups/gitlab-org/-/epics/4237#note_400257377).

Storage usage is reported in bytes in source and this is reflected in the `_size` columns. These sizes are then converted into GiB (1 GiB = 2^30 bytes = 1,073,741,824 bytes), and MiB (1 MiB = 2^20 bytes = 1,048,576 bytes), which is most often displayed in the UI. Since storage limits are allocated in GiB, they were left as such in the `_limit` columns.

Since this table reports at the top level namespace grain, aggregation of the individual underlying repositories is required. To increase visibility of the underlying repositories, two count columns (and their associated flags) are added that aren't calculated in the UI: which are `repositories_above_free_limit_count` and `capped_repositories_count`. These columns can serve as helpful indicators for when a customer will likely need to purchase extra storage.

For the purpose of this table, all child namespaces under a top level namespace with unlimited storage are also assumed to have unlimited storage. Also, storage sizes are converted to MiB and GiB in this table because these are the values being reported under the hood, even though on a project page storage is reported as "MB" or "GB".

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_waterfall_summary %}

A derived model using the revenue contract schedule to spread the recognized revenue across from the revenue start date to the revenue end date as defined by the revenue contract performance obligation's schedule.

{% enddocs %}

{% docs dim_host_instance_type %}

Dimension table providing instance type for a given UUID/HostName pair or Namespace Id for Self-Managed and SaaS data respectively.

{% enddocs %}

{% docs dim_instances %}
Dimension that contains statistical data for instances from usage ping data

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_sales_qualified_source %}

Sales qualified source dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_deal_path %}

Deal path dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_bizible_marketing_channel_path %}

Bizible marketing channel path dimension, based off a grouping of Bizible marketing channel paths in `map_bizible_marketing_channel_path`.

{% enddocs %}

{% docs dim_sales_segment %}

Dimension table for sales segment built off Ultimate_Parent_Sales_Segment_Employees__c in SFDC field in account data. Example values: SMB, Mid-Market, Large

{% enddocs %}

{% docs dim_sales_territory %}

Sales territory dimension, based off of salesforce account data, using the `generate_single_field_dimension_from_prep` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_industry %}

Industry dimension, based off of salesforce account data, using the `generate_single_field_dimension_from_prep` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_installation %}

Installation dimension, based off of version ping data, version host table, and Seat Link data. The primary key comes from `prep_ping_instance` and `Seat Link` and is built as a surrogate key based off of the `dim_host_id` and the `dim_instance_id`. Importantly, this means that any installation that has never sent a Service Ping or Seat Link record will not appear in this table.

An installtion can change from one delivery/deployment type to another. For example, an installation can start as a Self-Managed installation, but later transition to GitLab Dedicated. In this case, the installation would have multiple valid `product_delivery_type` and `product_deployment_type` values. To account for this, in this model, these fields are [Type 1 Slowly Changing Dimensions](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/type-1/), meaning that we always populate the field with the most recent value.

{% enddocs %}

{% docs dim_order %}

In Zuora Billing, an [Order](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders) represents a complete transaction record. Multiple order actions can be taken in a single order. For example, a subscription can be created and other subscriptions can be managed in a single order for a given customer.

{% enddocs %}

{% docs dim_order_action %}

[Order Actions](https://knowledgecenter.zuora.com/Billing/Subscriptions/Orders/AA_Overview_of_Orders/Order_Actions) are the tasks which can be performed on subscriptions in a single Order (see `dim_order` for more details).

The following actions are supported in the Orders module:

- Create Subscription
- Terms And Conditions
- Renewal
- Cancellation
- Owner Transfer
- Add Product
- Update Product
- Remove Product
- Suspend
- Resume

Multiple order actions can be grouped under a single Order. Previously multiple amendments would have been created to accomplish the same result. Now there can be a single `composite` amendment which encompasses all order actions taken in a single order.

{% enddocs %}


{% docs dim_order_type %}

Order type dimension, based off of salesforce opportunity data, using the `generate_single_field_dimension` macro to create the final formatted SQL

{% enddocs %}

{% docs dim_sales_funnel_kpi %}

Sales funnel KPI dimension, based on the sales funnel target file KPI name data.

{% enddocs %}

{% docs dim_namespace_hist %}

Table containing GitLab namespace snapshots.

The grain of this table is one row per namespace per valid_to/valid_from combination. The Primary Key is `namespace_snapshot_id`.

{% enddocs %}

{% docs dim_namespace_plan_hist %}

Slowly Changing Dimension Type 2 that records changes into namespace's plan subscriptions.

Easily to join with the following tables:

- `dim_namespace` through `dim_namespace_id`

{% enddocs %}

{% docs dim_namespace%}

Includes all columns from the namespaces base model. The plan columns in this table (gitlab_plan_id, gitlab_plan_title, gitlab_plan_is_paid) reference the plan that is inheritted from the namespace's ultimate parent.

This table add a count of members and projects currently associated with the namespace.
Boolean columns: gitlab_plan_is_paid, namespace_is_internal, namespace_is_ultimate_parent

A NULL namespace type defaults to "Individual".
This table joins to common product tier dimension via dim_product_tier_id to get the current product tier.

{% enddocs %}

{% docs dim_order_hist %}

Table containing GitLab order snapshots.

The grain of this table is one row per order per valid_to/valid_from combination.

{% enddocs %}

{% docs dim_quote %}

Dimensional table representing Zuora quotes and associated metadata.

The grain of the table is a quote_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_license %}

Dimensional table representing generated licenses and associated metadata.

The grain of the table is a license_id.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs dim_key_xmau_metric %}

A fact table that contains only the metrics that is a UMAU, SMAU, or GMAU metric that appears on the [Stages and Groups Performance Indicator handbook page](https://about.gitlab.com/handbook/product/stage-and-group-performance-indicators/)

{% enddocs %}

{% docs dim_ci_pipeline %}

A dim table that contains all CI Pipelines run on Gitlab.com application.

Easy joins available with:

* dim_project through `dim_project_id`
* dim_namespace through `dim_namespace_id` and `ultimate_parent_namespace_id`
* dim_date through `ci_pipeline_creation_dim_date_id`
{% enddocs %}

{% docs dim_issue %}

Dimensional table recording all issues created in our Gitlab.com SaaS instance. This table is easily joinable with other EDM dim tables:

- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id`
- `dim_plan` through `dim_plan_id`
- `dim_date` through `created_date_dim_id`

More info about issues in GitLab product [available here](https://docs.gitlab.com/ee/user/project/issues/)

{% enddocs %}

{% docs dim_merge_request %}

Dimensional table recording all merge requests created in our Gitlab.com SaaS instance. This table is easily joinable with other EDM dim tables:

- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id`
- `dim_plan` through `dim_plan_id`
- `dim_date` through `created_date_dim_id`

More info about issues in GitLab product [available here](https://docs.gitlab.com/ee/user/project/merge_requests/)

{% enddocs %}

{% docs dim_ci_build %}

Dimension table that contains all CI build data.

Easy to join with the following tables:

- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id` and `ultimate_parent_namespace_id`
- `dim_date` through `ci_build_creation_dim_date_id`
- `dim_plan` through `dim_plan_id`

{% enddocs %}

{% docs dim_user %}

Dimension table that contains all Gitlab.com Users.

Missing Column Values:
* Unknown - Value is Null in source data
* Not Found - Row Not found in source data
The following Columns have a Varchar Data Type and are set up to handle Missing Column Values:
* role
* last_activity_date
* last_sign_in_date
* setup_for_company
* jobs_to_be_done
* for_business_use
* employee_count
* country
* state

{% enddocs %}

{% docs dim_ci_runner %}

A Dimension table that contains all data related to CI runners.

It includes keys to join to the below tables:

- `dim_ci_build` through `dim_ci_build_id`
- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id` and `ultimate_parent_namespace_id`
- `dim_date` through `created_at`
- `dim_date` through `created_date_id `

{% enddocs %}

{% docs dim_ci_stage %}

A dim table that contains all CI Stages run in Gitlab.com CI Pipelines.

Easy joins available with:

* dim_project through `dim_project_id`
* dim_ci_pipeline through `dim_ci_pipeline_id`
* dim_date through `created_date_id`
{% enddocs %}

{% docs fct_ci_runner_activity %}

Fact table containing quantitative data related to CI runner activity on GitLab.com.

{% enddocs %}

{% docs dim_epic %}

Dimensional table representing epics created by groups on Gitlab.com instance. [More info about epics here](https://docs.gitlab.com/ee/user/group/epics/)

The grain of the table is the `dim_event_id`. This table is easily joinable with:

- `dim_plan` through `dim_plan_id`
- `dim_user` through `author_id`
- `dim_namespace` through `group_id` and `ultimate_namespace_id`

{% enddocs %}

{% docs dim_note %}

Dimensional table representing events recorded by the Events API. [More info about events tracked here](https://docs.gitlab.com/ee/api/notes.html)

2 kinds of notes are recorded in the notes table:
- system notes
- users' notes

System notes are notes automatically created based on status changes of the issue/snippet/merge request/epic.

For example, when a user is tagged as a reviewer, a system note is automatically created in the notes table. They are easily identifiable through the `is_system_id` boolean flag.

The grain of the table is the `dim_note_id`. This table is easily joinable with:

- `dim_plan` through `dim_plan_id`
- `dim_user` through `dim_user_id`
- `dim_project` through `dim_project_id`
- `dim_namespace` through `dim_namespace_id` and `ultimate_namespace_id`
{% enddocs %}

{% docs fct_daily_event_400 %}

Factual table built on top of prep_events tables that allows to explore usage data of free and paid users and namespaces from our SaaS instance gitlab.com.

The granularity is one event per day per user per ultimate parent namespace.

That means if a user creates the same day an issue on the Gitlab Data Team project and 2 issues in the main gitlab-com project, 2 rows will be recorded in the table.

If 2 users A and B create on the same day 1 merge request on the GitLab Data Team projectm 2 rows will be also recorded in the table.

{% enddocs %}

{% docs dim_issue_links %}

Dimensional table representing links between GitLab Issues recorded by the Events API. [More info about issue links can be found here](https://docs.gitlab.com/ee/user/project/issues/related_issues.html)

Issue Links are created when relationships are defined between issues. This table has slowly changing dimensions, as issue links/relationships can be removed over time

The grain of the table is the `dim_issue_link_id`. This table is easily joinable with:

- `dim_issue` through `dim_issue_sk` on `dim_issue_sk_source` & `dim_issue_sk_target`
{% enddocs %}

{% docs dim_locality %}

Dimensional table representing the [location_factor](https://about.gitlab.com/handbook/total-rewards/compensation/compensation-calculator/#location-factor) for a given locality and a given time range.

This table is derived from data files and logic of the [compensation calculator](https://gitlab.com/gitlab-com/people-group/peopleops-eng/compensation-calculator), specifically the location_factors.yml and the geo_zones.yml

The grain of the table is the `dim_locality_id` and the `valid_from` date filed.

{% enddocs %}

{% docs dim_ping_instance %}

**Description:** Atomic level instance Service Ping data including installation settings and metadata, along with JSON payloads with usage metrics
- Atomic (lowest grain) data with a single record per ping

**Data Grain:**
- dim_ping_instance_id

**Filters Applied to Model:**
- `Inherited` - `uuid IS NOT NULL` (uuid is synonymous with dim_instance_id)
- `Inherited` - `version NOT LIKE '%VERSION%`

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per installation (`dim_installation_id`)
- `is_last_ping_of_week` = last ping created per calendar week per installation (`dim_installation_id`)
- `is_internal` = TRUE WHERE:
  - ping_deployment_type = 'GitLab.com'
  - (OR) installation_type = 'gitlab-development-kit'
  - (OR) hostname = 'gitlab.com'
  - (OR) hostname LIKE '%.gitlab.com'
- `is_staging` = TRUE WHERE:
  - hostname LIKE 'staging.%'
  - (OR) hostname IN ('staging.gitlab.com','dr.gitlab.com')
- `is_trial` = `IFF(ping_created_at < license_trial_ends_on, TRUE, FALSE)`
- `major_minor_version` = `major_version || '.' || minor_version`
- `app_release_major_minor_id` = `major_version * 100 + minor_version` (helpful for sorting or filtering versions)
- `version_is_prerelease` = `IFF(version ILIKE '%-pre', TRUE, FALSE)`
- `cleaned_edition` = `IFF(license_expires_at >= ping_created_at OR license_expires_at IS NULL, ping_edition, 'EE Free')`

**Other Comments:**
- The `dim_ping_instance` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs dim_ping_metric %}

This model replaces `dim_usage_ping_metric` table that maps directly to the [Gitlab Metrics Dictionary](https://metrics.gitlab.com/). In addition to all metrics currently in the Service Ping, it also contains metrics that have been removed.

Some other enhancements in this model include : addition of a surrogate key, exclusion and cleaning of some Product groups, and renaming Usage ping to Service Ping.

{% enddocs %}

{% docs dim_ping_metric_daily_snapshot %}

This slowly changing dimension type 2 model allows for historic reporting of the  `dim_ping_metric` table that maps directly to the [Gitlab Metrics Dictionary](https://metrics.gitlab.com/). `snapshot_id` has been included to be used in the join.

For this reason `metrics_path` is not unique.

{% enddocs %}

{% docs fct_performance_indicator_targets %}

**Description:** Monthly performance indicator targets, based on the dates and targets provided in [performance indicator .yml files](https://gitlab.com/internal-handbook/internal-handbook.gitlab.io/-/tree/main/data/performance_indicators). This model is used to generate target values in the `[td_xmau]` and other related snippets.

**Data Grain:**
- reporting_month
- pi_metric_name

**Filters Applied to Model:**
- Use currently valid records from files - `valid_to_date = MAX(valid_to_date)`
- Include metrics with targets - `pi_monthly_estimated_targets IS NOT NULL` (field from .yml file)

**Business Logic in this Model:**
- `reporting_month` is derived from the date provided in the `pi_monthly_estimated_targets` field of the .yml file
  - Target end date: date specified in `pi_monthly_estimated_targets`
  - Target start date: The day after the previous target end date _OR_, if a previous target is not specified, `2020-03-01` (date of earliest PIs we are tracking)

Example: `pi_monthly_estimated_targets`: `{"2022-02-28":1000,"2022-03-31":2000,"2022-05-31":3000}`

| reporting_month | target_value |
| --- | --- |
| 2020-03-01 | 1000 |
| ... | 1000 |
| 2022-02-01 | 1000 |
| 2022-03-01 | 2000 |
| 2022-04-01 | 3000 |
| 2022-05-01 | 3000 |

**Other Comments:**
- More information on how to set these values on the [Product Manager Toolkit handbook page](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/xmau-analysis/product-manager-toolkit.html#how-to-show-dynamic-targets-from-pi-yaml-files)

{% enddocs %}

{% docs fct_ping_instance_metric %}

**Description:** Atomic level instance Service Ping data by ping and metric for all metrics, including basic identifiers for easy joins out to dimension tables
- The data includes a single row per ping and metric
- Includes installation, instance, date, product, billing, and subscription identifiers

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- `Inherited` - `uuid IS NOT NULL` (uuid is synonymous with dim_instance_id)
- `Inherited` - `version NOT LIKE '%VERSION%`

**Business Logic in this Model:**
- Includes all metrics, regardless of time frame or whether they can be located in `dim_ping_metric`
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- The `fct_ping_instance_metric` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_rolling_13_months %}

**Description:** Atomic level instance Service Ping data by ping and metric for all metrics for the last 13 months, including basic identifiers for easy joins out to dimension tables. This model filters `fct_ping_instance_metric` to the last 13 months to improve query performance during data exploration and analysis.
- The data includes a single row per ping and metric
- Includes installation, instance, date, product, billing, and subscription identifiers

**Note:** This model is set to never full refresh. This means that new data will update daily but changes to past events will not be updated.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- `Inherited` - `uuid IS NOT NULL` (uuid is synonymous with dim_instance_id)
- `Inherited` - `version NOT LIKE '%VERSION%`
- Rolling 13 months of Service Pings data by the ping_created_date

**Business Logic in this Model:**
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- The `fct_ping_instance_metric` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_rolling_6_months %}

**Description:** Atomic level instance Service Ping data by ping and metric for all metrics for the last 6 months, including basic identifiers for easy joins out to dimension tables. This model filters `fct_ping_instance_metric` to the last 6 months to improve query performance during data exploration and analysis.
- The data includes a single row per ping and metric
- Includes installation, instance, date, product, billing, and subscription identifiers

**Note:** This model is set to never full refresh. This means that new data will update daily but changes to past events will not be updated.

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- `Inherited` - `uuid IS NOT NULL` (uuid is synonymous with dim_instance_id)
- `Inherited` - `version NOT LIKE '%VERSION%`
- Rolling 6 months of Service Pings data by the ping_created_date

**Business Logic in this Model:**
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- The `fct_ping_instance_metric` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance %}

**Description:** Atomic level instance Service Ping data by ping, including basic identifiers for easy joins out to dimension tables. Metrics are not included in this model
- Atomic (lowest grain) data with a single record per ping
- Includes installation, instance, date, product, billing, and subscription identifiers

**Data Grain:**
- dim_ping_instance_id

**Filters Applied to Model:**
- `Inherited` - `uuid IS NOT NULL` (uuid is synonymous with dim_instance_id)
- `Inherited` - `version NOT LIKE '%VERSION%`

**Business Logic in this Model:**
- `is_trial` = `IFF(ping_created_at < license_trial_ends_on, TRUE, FALSE)`
- `dim_subscription_id` = `COALESCE(ping_payload.license_subscription_id, prep_subscription.dim_subscription_id)`
- `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`

**Other Comments:**
- The `fct_ping_instance` table is built directly from the [prep_ping_instance table](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_ping_instance) which brings in Instance Service Ping data one record per Service Ping.  Along with the Instance information a 'Payload' column with an array of Metrics is captured in the Service Ping.
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Multiple Instances can be hosted on each Implementation. Multiple Installations can be included within each Instance which is determined by Host_id. (Instance_id || Host_id = Installation_id)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_7_day %}

**Description:** Atomic level instance Service Ping data by ping and metric for 7-day metrics, including basic identifiers for easy joins out to dimension tables. This is a filtered version of `fct_ping_instance_metric`
- The data includes a single row per ping and metric
- Includes installation, instance, date, product, billing, and subscription identifiers

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- Include 7-day metrics (`time_frame = '7d'`)

**Business Logic in this Model:**
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `Inherited` - `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_28_day %}

**Description:** Atomic level instance Service Ping data by ping and metric for 28-day metrics, including basic identifiers for easy joins out to dimension tables. This is a filtered version of `fct_ping_instance_metric`
- The data includes a single row per ping and metric
- Includes installation, instance, date, product, billing, and subscription identifiers

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- Include 28-day metrics (`time_frame = '28d'`)

**Business Logic in this Model:**
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `Inherited` - `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_none_null %}

**Description:** Atomic level instance Service Ping data by ping and metric for `none` and NULL timeframe metrics, including basic identifiers for easy joins out to dimension tables. This is a filtered version of `fct_ping_instance_metric`
- The data includes a single row per ping and metric
- Includes installation, instance, date, product, billing, and subscription identifiers

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- Include `none` and `NULL` metrics (`time_frame = 'none' or NULL`)

**Business Logic in this Model:**
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `Inherited` - `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_monthly %}

**Description:** Atomic level instance Service Ping data for the last ping of the month per installation by ping and metric for 28-day and all-time metrics. This includes basic identifiers for easy joins out to dimension tables. This is a filtered version of `fct_ping_instance_metric`
- The data includes a single row per ping and metric. Moreover, we filter down to the last ping of the month.
  - Alternatively stated, there is a single row per installation, month, and metric.
- Includes installation, instance, date, product, billing, and subscription identifiers

**Data Grain:**
- dim_installation_id
- ping_created_date_month
- metrics_path

**Filters Applied to Model:**
- Exclude metrics that timed out during ping generation (`has_timed_out = FALSE`)
- Include 28-day and all-time metrics (`time_frame IN ('28d', 'all')`)
- Include metrics from the 'Last Ping of the Month' pings (`is_last_ping_of_month = TRUE`)
- Exclude metrics without values (`metric_value IS NOT NULL`)

**Business Logic in this Model:**
- `is_last_ping_of_month` = last ping created per calendar month per installation (`dim_installation_id`)
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `Inherited` - `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_weekly %}

**Description:** Atomic level instance Service Ping data for the last ping of the week per installation by ping and metric for 7-day metrics. This includes basic identifiers for easy joins out to dimension tables. This is a filtered version of `fct_ping_instance_metric`
- The data includes a single row per ping and metric. Moreover, we filter down to the last ping of the week.
  - Alternatively stated, there is a single row per installation, week, and metric.
- Includes installation, instance, date, product, billing, and subscription identifiers

**Data Grain:**
- dim_installation_id
- ping_created_date_week
- metrics_path

**Filters Applied to Model:**
- Exclude metrics that timed out during ping generation (`has_timed_out = FALSE`)
- Include 7-day metrics (`time_frame = '7d'`)
- Include metrics from the 'Last Ping of the Week' pings (`is_last_ping_of_week = TRUE`)
- Exclude metrics without values (`metric_value IS NOT NULL`)

**Business Logic in this Model:**
- `is_last_ping_of_week` = last ping created per calendar week per installation (`dim_installation_id`)
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `Inherited` - `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs fct_ping_instance_metric_all_time %}

**Description:** Atomic level instance Service Ping data by ping and metric for all-time metrics, including basic identifiers for easy joins out ot dimension tables
- The data includes a single row per ping and metric
- Includes installation, instance, date, product, billing, and subscription identifiers

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- Include all-time metrics (`time_frame = 'all'`)

**Business Logic in this Model:**
- `Inherited` - `has_timed_out` = `IFF(value = -1, TRUE, FALSE)`
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `umau_value` = metric value from `usage_activity_by_stage_monthly.manage.events`
- `Inherited` - `dim_subscription_id` = `COALESCE(prep_subscription.dim_subscription_id, ping_payload.license_subscription_id)`

**Other Comments:**
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- Service Ping data is captured at a particular point in time with `all-time, 7_day and 28_day` metrics.  The metrics are only pertinent to the Ping Date and Time and can not be aggregated across Ping Dates. Service Pings are normally compared WoW, MoM, YoY,  etc.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}


{% docs fct_ping_instance_metric_wave %}

The purpose of this data model is to identify the service pings, from Self-Managed and SaaS Dedicated, that can be mapped to a subscription and to pivot thet set of [wave metrics required for Gainsight](https://gitlab.com/gitlab-data/analytics/-/blob/master/transform/snowflake-dbt/macros/version/ping_instance_wave_metrics.sql) from the `fct_ping_instance_metric` model, strip all the sensitive data out, and then report one value for each metric in that column.

{% enddocs %}


{% docs fct_ping_instance_metric_wave_monthly %}

This table builds on the set of all Zuora subscriptions that are associated with **Self-Managed**  or **SaaS Dedicated** rate plans. Seat Link data from Customers DB (`fct_usage_self_managed_seat_link`) are combined with high priority Usage Ping metrics (`fct_ping_instance_metric_wave`) to build out the set of facts included in this table.

The grain of this table is `hostname` per `dim_instance_id(uuid)` per `dim_subscription_id` per `snapshot_month`. Since there are Self-Managed subscriptions that do not send Usage Ping payloads, it is possible for `dim_instance_id` and `hostname` to be null.

The data from this table will be used to create a mart tables -  `mart_product_usage_paid_user_metrics_monthly` and `mart_product_usage_wave_1_3_metrics_monthly_diff` for Gainsight Customer Product Insights.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs fct_mrr_snapshot_model %}

Daily [snapshot](https://handbook.gitlab.com/handbook/enterprise-data/platform/dbt-guide/#snapshots) model of the [fct_mrr](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.fct_mrr) model

{% enddocs %}

{% docs dim_subscription_snapshot_model %}

Daily [snapshot](https://handbook.gitlab.com/handbook/enterprise-data/platform/dbt-guide/#snapshots) model of the [dim_subscription](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.dim_subscription) model

{% enddocs %}


{% docs bdg_metrics_redis_events %}
## Overview
This model records the many-to-many relationship between Service Ping Metrics, internal events, and internal event filters.
The grain is one record per metric, event, and filter. It pulls from the metrics dictionary yml files via `dim_ping_metric`, and contains the
metric name, and the internal event name. It will be joined to Snowplow events that contain the Service Ping Context to get Gitlab.com product usage data at the namespace level.

## Aggregation Strategies
[This thread](https://gitlab.com/gitlab-org/gitlab/-/issues/376244#note_1167575425) has a nice summary of the possible aggregation strategies. The important thing to know from an analyst perspective is that internal events metrics come in three basic varities:
1. Have only one associated internal event; if that event occurs, count the metric
1. Have multiple associated internal events; count the metric if _any_ internal event in that list occurs
1. Have multiple associated internal events; count the metric if _all_ internal events in that list occur

As a result, this bridge table will be used a bit differently to count intersection metrics compared to how it will be used to count union metrics.

{% enddocs %}

{% docs dim_crm_task %}

Dimension model of all [Salesforce Tasks](https://help.salesforce.com/s/articleView?id=sf.tasks.htm&type=5) that record activities related to leads, contacts, opportunities, and accounts.

{% enddocs %}

{% docs fct_crm_task %}

Fact model of all [Salesforce Tasks](https://help.salesforce.com/s/articleView?id=sf.tasks.htm&type=5) that record activities related to leads, contacts, opportunities, and accounts.

{% enddocs %}

{% docs fct_ping_instance_free_user_metrics %}

Table containing **free** Self-Managed and SaaS Dedicated users in preparation for free user service ping metrics fact table.

The grain of this table is one row per uuid-hostname combination per month.


{% enddocs %}

{% docs fct_ping_instance_free_user_metrics_monthly %}

This table unions the sets of all Self-Managed and SaaS **free users**. The data from this table will be used to create a mart table (`mart_product_usage_free_user_metrics_monthly`) for Customer Product Insights.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)


{% enddocs %}

{% docs dim_behavior_browser %}

**Description:** Dimension containing browser attributes for the analysis of Snowplow events.

**Data Grain:** dim_behavior_browser_sk
- browser_name
- browser_major_version
- browser_minor_version
- browser_language

**Filters Applied to Model:**
- Include events where at least one of browser_name, browser_major_version, browser_minor_version, _OR_ browser_language is available (`browser_name IS NOT NULL OR browser_major_version IS NOT NULL OR browser_minor_version IS NOT NULL OR browser_language IS NOT NULL`)

**Tips for use:**
- Join this model to facts (ex. `fct_behavior_website_page_view`, `fct_behavior_structured_event`) using `dim_behavior_browser_sk` to get browser-level attributes about page views or events

{% enddocs %}

{% docs dim_behavior_event %}

**Description:** Dimensional model containing distinct events types from Snowplow.

**Data Grain:** dim_behavior_event_sk

This ID is generated in [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) using `event`, `event_name`, `platform`, `gsc_environment`, `se_category`, `se_action`, `se_label` and `se_property`.


**Other Comments:**
- [Snowplow column definitions](https://docs.snowplow.io/docs/understanding-your-pipeline/canonical-event/)

{% enddocs %}

{% docs fct_behavior_structured_event %}

**Description:** Fact table containing quantitative data for both staging and non-staging snowplow structured events. Structured events are custom events implemented with five parameters: event_category, event_action, event_label, event_property and event_value. Snowplow documentation on [types of events](https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/).

**Data Grain:** behavior_structured_event_pk

This ID is generated using `event_id` from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all)

**Filters Applied to Model:**
- This model only includes Structured events (when `event=struct` from `dim_behavior_event` )

**Tips for use:**
- There is a cluster key on `behavior_at::DATE`. Using `behavior_at` in a WHERE clause or INNER JOIN will improve query performance.
- Join this model to `dim_behavior_event` using `dim_behavior_event_sk` in order to filter the fact on event_action, event_category, etc.
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser

{% enddocs %}

{% docs fct_behavior_structured_event_30 %}

**Description:** Derived fact table containing quantitative data for both staging and non-staging snowplow structured events for the **last 30 days**.

**Data Grain:** behavior_structured_event_pk

This ID is generated using event_id from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

**Filters Applied to Model:**

- This model only includes structured events for the last 30 days

**Tips for use:**

- Join this model to `dim_behavior_event` using `dim_behavior_event_sk` in order to filter the fact on event_action, event_category, etc.
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser

{% enddocs %}

{% docs fct_behavior_structured_event_90 %}

**Description:** Derived fact table containing quantitative data for both staging and non-staging snowplow structured events for the **last 90 days**.

**Data Grain:** behavior_structured_event_pk

This ID is generated using event_id from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

**Filters Applied to Model:**

- This model only includes structured events for the last 30 days

**Tips for use:**

- Join this model to `dim_behavior_event` using `dim_behavior_event_sk` in order to filter the fact on event_action, event_category, etc.
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser

{% enddocs %}

{% docs dim_behavior_operating_system %}

**Description:** Dimension containing operating system and device attributes for the analysis of Snowplow events.

**Data Grain:** dim_behavior_operating_system_sk
- os_name
- os_timezone

**Filters Applied to Model:**
- Include events where os_name _OR_ os_timezone is available (`os_name IS NOT NULL OR os_timezone IS NOT NULL`)

**Tips for use:**
- Join this model to facts (ex. `fct_behavior_website_page_view`, `fct_behavior_structured_event`) using `dim_behavior_operating_system_sk` to get OS and device-level attributes about page views or events

{% enddocs %}

{% docs dim_behavior_website_page %}

**Description:** Dimensional model containing distinct page types from Snowplow events.

**Data Grain:** dim_behavior_website_page_sk

This ID is generated in [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) using `page_url_host_path`, `app_id` and `page_url_scheme`.

**Filters Applied to Model:**
- Include pages from page view, structured, and unstructured events (`event IN ('struct', 'page_view', 'unstruct')`)

{% enddocs %}

{% docs fct_behavior_website_page_view %}

**Description:** Fact table containing quantitative data for Page views. Page views are a subset of Snowplow events and are fired by the Javascript tracker.

**Data Grain:** fct_behavior_website_page_view_sk

This ID is generated using `event_id` and `page_view_end_at` from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

**Filters Applied to Model:**
- This model only includes Pageview events (when `event=page_view` from `dim_behavior_event` )

**Tips for use:**
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser

{% enddocs %}

{% docs fct_behavior_unstructured_event %}

**Description:** Fact table containing quantitative data for both staging and non-staging snowplow unstructured events. These events include [Snowplow-authored "out of the box" events](https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/#snowplow-authored-events) like `link_click`, `focus_form`, `change_form`, and `submit_form`. Unstructured event data is based on a JSON schema.

**Data Grain:** fct_behavior_unstructured_sk (generated in [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all))
- event_id
- behavior_at

**Filters Applied to Model:**
- Include unstructured events (`event = 'unstruct'`)

**Business Logic in this Model:**
- A selection of key value pairs from Snowplow-authored events are parsed out into their own columns:
  - `link_click`: target URL
  - `submit_form`: form ID
  - `change_form`: form ID, form type, form element ID
  - `focus_form`: form element ID, form node name

**Other Comments:**
- Any self-describing event (ex. not a structured event, page view, etc) is considered to be "unstructured", but GitLab's current instrumentation focuses on the [Snowplow-authored "out of the box" events](https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/#snowplow-authored-events).

{% enddocs %}

{% docs fct_event_namespace_monthly %}

**Description:** GitLab.com usage event data for valid events, grouped by month, event name, and ultimate parent namespace

**Data Grain:**
- event_calendar_month
- event_name
- dim_ultimate_parent_namespace_id

**Filters Applied to Model:**
- Exclude events not associated with a namespace (ex: 'users_created')
- `Inherited` - Include valid events for standard analysis and reporting:
  - Exclude events where the event created date < the user created date (`days_since_user_creation_at_event_date >= 0`)
    - These are usually events from projects that were created before the GitLab.com user and then imported after the user is created
  - Exclude events from blocked users (based on the current user state)
- `Inherited` - Rolling 36 months of data

**Business Logic in this Model:**
- `Inherited` - A namespace's plan information (ex: `plan_name_at_event_month`) is determined by the plan for the last event on a given month
- `Inherited` - The ultimate parent namespace's subscription, billing, and account information (ex: `dim_latest_subscription_id`) reflects the most recent available attributes associated with that namespace
- `Inherited` - `dim_latest_product_tier_id` reflects the _current_ product tier of the namespace
- `Inherited` - `section_name`, `stage_name`, `group_name`, and xMAU metric flags (ex: `is_gmau`) are based on the _current_ event mappings and may not match the mapping at the time of the event

**Other Comments:**
- The current month is _included_ in this model. Be mindful of potentially including incomplete data
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs fct_behavior_structured_event_experiment %}

**Description:** Derived fact table containing quantitative data for both staging and non-staging snowplow structured events related to experiments.

**Data Grain:** behavior_structured_event_pk

This ID is generated using event_id from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

**Filters Applied to Model:**

This model only includes structured events implemented for experiments. Experiment events are defined as any event that includes the `gitlab_experiment` context

**Tips for use:**

- Unlike the previous legacy version, you do _not_ need to join this model back to the fact. It includes all columns on the fact, in addition to experiment-specific columns (ex. `experiment_name`, etc).
- Join this model to `dim_behavior_event` using `dim_behavior_event_sk` in order to filter the fact on event_action, event_category, etc.
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS details
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser details

{% enddocs %}

{% docs fct_behavior_structured_event_without_assignment %}

**Description:** Derived fact table containing quantitative data for both staging and non-staging snowplow structured events **excluding assignment events**. Assignment events are events that signifies a user was enrolled into an Experiment.

**Data Grain:** behavior_structured_event_pk

This ID is generated using event_id from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

**Filters Applied to Model:**

- This model excludes assignment events (`event_action = 'assignment'`)

**Tips for use:**

- Join this model to `dim_behavior_event` using `dim_behavior_event_sk` in order to filter the fact on event_action, event_category, etc.
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser

{% enddocs %}

{% docs fct_behavior_structured_event_without_assignment_190 %}

**Description:** Derived fact table containing quantitative data for both staging and non-staging snowplow structured events **excluding assignment events** for the **last 190 days**. Assignment events are events that signifies a user was enrolled into an Experiment.

**Data Grain:** behavior_structured_event_pk

This ID is generated using event_id from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

**Filters Applied to Model:**

- This model only includes structured events for the last 190 days
- This model excludes assignment events (`event_action = 'assignment'`)

**Tips for use:**

- Join this model to `dim_behavior_event` using `dim_behavior_event_sk` in order to filter the fact on event_action, event_category, etc.
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser

{% enddocs %}

{% docs fct_behavior_structured_event_without_assignment_400 %}

**Description:** Derived fact table containing quantitative data for both staging and non-staging snowplow structured events **excluding assignment events** for the **last 400 days**. Assignment events are events that signifies a user was enrolled into an Experiment.

**Data Grain:** behavior_structured_event_pk

This ID is generated using event_id from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

**Filters Applied to Model:**

- This model only includes structured events for the last 400 days
- This model excludes assignment events (`event_action = 'assignment'`)

**Tips for use:**

- Join this model to `dim_behavior_event` using `dim_behavior_event_sk` in order to filter the fact on event_action, event_category, etc.
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser

{% enddocs %}

{% docs fct_behavior_structured_event_redis_hll_counters %}

**Description:** Derived fact table containing quantitative data for both staging and non-staging snowplow structured events related to redis hll metrics.

**Data Grain:** behavior_structured_event_pk

This ID is generated using event_id from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all).

**Filters Applied to Model:**

- This model only includes structured events implemented for redis hll metrics.
- Redis hll metric events are defined as any event that includes certain event actions (`event_action IN ('g_analytics_valuestream', 'action_active_users_project_repo' 'push_package', 'ci_templates_unique', 'p_terraform_state_api_unique_users', 'i_search_paid')`)

**Tips for use:**

- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_website_page` using `dim_behavior_referrer_page_sk` in order to pull in information about the referring URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS details
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser details

{% enddocs %}

{% docs fct_delta_arr_subscription_lineage_product_monthly %}

Delta ARR is a measure of changes to ARR compared to the prior month. The [ARR Analysis Framework](https://internal-handbook.gitlab.io/handbook/sales/annual-recurring-revenue-arr/#arr-analysis-framework) handbook page provides more details on the analysis.

The model uses the subscription_lineage grain to calculate the Delta ARR. This is a fundamental change from previous Delta ARR models at the subscription grain. When looking at the subscription grain, debook-book scenarios are not captured. Therefore, it is necessary to analyze the subscription_lineage grain for accurate product level Delta ARR changes. This model provides a method to analyze the 6 subscription linkage scenarios provided in this [Linking Subscriptions for Data Retention](https://docs.google.com/spreadsheets/d/1SYFy0Xqau1dbOm2YXmp0NvWDEkL_vIcWaFhKzwcocCM/edit#gid=0) file.

The model ERD can be found [HERE](https://lucid.app/lucidchart/invitations/accept/inv_07d25d39-3076-408f-b768-67d1895ea064).

Model Validation:

The model ties out 100% to mart_arr with the below 3 exceptions:

1. The model removes subscriptions with data quality problems in the subscription_name_slugify field that is used for the subscription lineage. These were 5 subscriptions at the time of model creation and they do not have a material impact on the model insights.

1. The model removes Storage product charges. The model is intended to focus on the paid plan tiers only.

1. At the time of model creation, there were 23 subscriptions that were part of lineages where subscriptions in the lineage roll up to different Salesforce ultimate parent accounts. All of the ARR for these subscriptions is in the model; however, the model rolls up the ARR for these subscriptions to the ultimate parent account of the oldest subscription in the lineage. This results in these 23 parent accounts not tieing out 100% to mart_arr.

Model Caveat:

1. It should be that a subscription only has 1 paid tier plan attached to it. However, there are a small minority of subscriptions that have more than 1 product. Therefore, it is necessary to put the product tiers into an array in the model for completeness. In virtually all cases, it is 2 product tiers on the subscription with many of them having old Bronze/Starter plans in addition to Premium plans.

{% enddocs %}

{% docs dim_team_member %}

This table contains team members work and personal information. Sensitive columns are masked and only visible by team members with the analyst_people role assigned in Snowflake. The table includes information regarding current team members, new hires who have records created in Workday before their start date and team members who were terminated in 2021 onwards. Team members who were terminated before 2021 are not captured in this model at this time. The grain of this table is one row per employee_id per valid_to/valid_from combination.

{% enddocs %}

{% docs fct_team_member_position %}

This table contains team members' job history, including any changes in their job profile or team. The table joins the staffing_history_approved_source and job_profiles_source. The grain of this table is one row per employee_id, team_id, effective_date and date_time_initiated combination.

The reason why the date and time when the change was initiated was added to the grain is because there are changes that are scheduled to take effect on the same date. In order to make sure that we capture the most up-to-date change and ensure the records are unique, we need to add take this timestamp into account.

{% enddocs %}

{% docs fct_team_member_status %}

This table contains the termination reason, type, exit impact and employment_status. Sensitive columns are masked and only visible by team members with the analyst_people role assigned in Snowflake. This table contains only past terminations. Future terminations are not included at this time. We will evaluate the possibility of making future terminations available to people with the analyst_people role in a future iteration. The grain of this table is one row per employee_id, employment_status and status_effective_date combination.

{% enddocs %}

{% docs fct_team_status %}

This table is a derived fact from fct_team_member_status and fct_team_member_position. Sensitive columns are masked and only visible by team members with the analyst_people role assigned in Snowflake.

This table only contains one change in the team member's position per effective date, as opposed to the fct_team_member_position table which contains all changes to a team member's position profile, regardless of whether they became effective or not. This table doesn't include future hires, only people working at GitLab as of today's date.

The grain of this table is one row per employee_id and valid_from combination

{% enddocs %}

{% docs dim_trial_latest %}

This table summarizes all the trial Orders information for a specific namespace.

We utilize the `customers_db_orders_snapshots_base` model in order to isolate/filter out all the trials.

This model does the following:

* It isolates the orders that are flagged with the column `is_trial = TRUE`
* It deduplicates by taking the latest row created for a specific `gitlab_namespace_id`
* We can join this model with `customers_db_customers` in the downstream models in order to get information about country, company_size of the User who started the trial

{% enddocs %}


{% docs fct_trial %}

This model collects all trials that start from the subscription portal, then limits the model to the `internal_order_id`, `dim_namespace_id`, `subscription_name`, `order_updated_at` grain.

{% enddocs %}

{% docs fct_trial_first %}

This model is a derived fact table that is built using the `fct_trial` model, which contains all trial orders that start from the subscription portal. This model additionally deduplicates by taking the first row that was created for customers, namespaces and users based on the `order_updated_at` column.

The grain of this model is `trial order per namespace`.

This model identifies if a trial order has been converted or not. We exclude ci_minutes/compute_minutes orders from this model.

Finally, only valid subscriptions that are not refunded are identified by filtering out subscriptions that have (mrr <= 0 and tcv <=0). The subscriptions that are cancelled instantly or fully refunded after a certain period are excluded.

The `customers_db_orders_snapshots_base` model has reliable data from the 1st of September, 2019, therefore only the orders that have a `start_date` after this date are included this model.


{% enddocs %}


{% docs fct_trial_latest %}

This model captures information about the latest trial Orders for a specific namespace.

The grain of this model is `trial order per namespace`.

## Context

To understand the context, the following information is important:
* Before 2019-09-16, a namespace could subscribe to a trial several times. That was a bug corrected by the fulfillment team in September 2019. More info [here](https://gitlab.com/gitlab-org/customers-gitlab-com/merge_requests/458).
* All snapshots tables have also been created in September 2019. Before that we don't have historical information.
* The Customers_db ETL was unstable before October 2019. We improved the logic at the end of October by changing from incremental model to a daily full "drop and create" to the raw database.

## Process

Trial Information is collected in 2 tables (one in the subscription portal database - customer_db, the other in the .com database - main app). These 2 tables don't talk to each other and have incomplete information. We join them together to create a more consistent and complete picture of trials started.

For the gitlab_dotcom database, information is stored in `gitlab_dotcom_gitlab_subscriptions` table. As described [here](https://gitlab.com/gitlab-data/analytics/merge_requests/1983#note_249268694), rows can be deleted in this table, so we use the `gitlab_dotcom_gitlab_subscriptions_snapshot` for higher reporting accuracy.  In this model, we do the following operations:
* We isolate trials by looking at a specific column `gitlab_subscription_trial_ends_on` which is filled only when a specific subscription was a trial before.
* We then group by the namespace_id in order to only select the latest trials started for a specific namespace.
* One weird behaviour of this table is the way it deals with expired orders. It is explained [here](/model.gitlab_snowflake.gitlab_dotcom_gitlab_subscriptions). That means that the `start_date` is NOT a reliable source for us in order to find the trial start date. We therefore use the `gitlab_subscription_trial_ends_on` column in order to estimate when the trial has been started (30 days before the end of the trials in most cases)

The data for latest trial per namespace is derived from `fct_trial` model. We then join the 2 CTEs created on `gitlab_namespace_id`.

This model identifies if a trial has been converted or not. The logic for which has been included in `fct_trial` model. We exclude ci_minutes orders from this model.

Finally, only valid subscriptions that are not refunded are identified by filtering out subscriptions that have (mrr <= 0 and tcv <=0). The subscriptions that are cancelled instantly or fully refunded after a certain period are excluded.

{% enddocs %}

{% docs dim_snippet %}

[Snippets](https://docs.gitlab.com/ee/user/snippets.html) are pieces of code which can be stored and shared with others on GitLab. This model stores dimensional attributes about snippets including:

  - Author
  - Project
  - Ultimate Namespace
  - Plan
  - Type (project/personal)
  - Created Date
  - Updated Date

{% enddocs %}

{% docs dim_package %}

Customers can publish and share packages in using GitLab's [package registry](https://docs.gitlab.com/ee/user/packages/package_registry/). This models contains dimensional attributes relating to the packages in the package registry.

{% enddocs %}

{% docs dim_integration %}

A dimensional model describing the integration associated with GitLab namespaces.

From the GitLab.com [documentation](https://docs.gitlab.com/ee/user/project/integrations/), we know integrations are like plugins, which give customers the freedom to add functionality to GitLab.

{% enddocs %}

{% docs dim_requirement %}

As described in the [GitLab documentation](https://docs.gitlab.com/ee/user/project/requirements/):

With requirements, you can set criteria to check your products against. They can be based on users, stakeholders, system, software, or anything else you find important to capture. A requirement is an artifact in GitLab which describes the specific behavior of your product. Requirements are long-lived and don’t disappear unless manually cleared.

This dimension model holds all requirement records and provides dimensional data around their state, creation dates, etc.

{% enddocs %}

{% docs dim_milestone %}

All milestones created within a namespace, with details including the start date, due date, description, and title.

{% enddocs %}

{% docs fct_latest_seat_link_installation %}

Contains the latest Seat Link record for every installation in the source Seat Link model.

{% enddocs %}

{% docs fct_behavior_structured_event_code_suggestion %}

Fact derived from `fct_behavior_structured_event`, limited to only Snowplow events with the [Code Suggestions context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/code_suggestions_context/jsonschema) and columns, which indicates they are Code Suggestions events.

{% enddocs %}

{% docs fct_behavior_structured_event_ide_extension_version %}

Fact derived from `fct_behavior_structured_event`, limited to only Snowplow events with the [IDE Extension Version context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/ide_extension_version/jsonschema) and columns, which indicates they are IDE Extension Version events.

{% enddocs %}

{% docs fct_behavior_structured_event_service_ping %}

Fact derived from `fct_behavior_structured_event`, limited to only Snowplow events with the [Service Ping context](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/gitlab_service_ping/jsonschema) and columns, which indicates they are Service Ping events.

{% enddocs %}


{% docs fct_crm_opportunity_7th_day_weekly_snapshot %}

Derived fact table from `fct_crm_opportunity_daily_snapshot`. This new table will not include every daily snapshot.  Instead, it will feature a snapshot once every week — specifically, one snapshot every 7 days throughout each quarter. This approach is designed to observe and analyze the key trends and activities occurring each week within the quarter.

{% enddocs %}

{% docs fct_crm_opportunity_7th_day_weekly_snapshot_aggregate %}

Derived fact table from `fct_crm_opportunity_daily_snapshot`. This new table will not include every daily snapshot.  Instead, it will feature a snapshot once every week — specifically, one snapshot every 7 days throughout each quarter.

This query aggregates key sales metrics and attributes from the actuals table to the weekly grain and across multiple dimensions like sales source, order type, hierarchy, stage, deal path. This table also computes quarterly totals and adds them as non additive measures to each row, to facilitate pipeline velocity and coverage calculation in Tableau.

{% enddocs %}

{% docs fct_crm_opportunity_daily_snapshot_aggregate %}
   
Derived fact table from `fct_crm_opportunity_daily_snapshot`. 

This table aggregates key sales metrics and attributes from the actuals table to the daily grain and across multiple dimensions like sales source, order type, hierarchy, stage, deal path. 

{% enddocs %}

{% docs fct_targets_actuals_7th_day_weekly_snapshot %}

Derived fact table from `fct_crm_opportunity_daily_snapshot` and `fct_sales_funnel_target_daily`. This fct table simplifies tracking sales performance by showing targets and actuals combined, every seven days within a quarter.

It combines daily targets, daily actuals along with the total quarterly targets and actuals. This model includes both the numerator and denominator for calculating coverage throughout a quarter

{% enddocs %}

{% docs fct_team_member_absence %}

This table is a derived fact from fct_team_status and gitlab_pto. Sensitive columns are masked and only visible by team members with the `analyst_people` role assigned in Snowflake.
The grain of this table is one row per pto_uuid per absence_date per dim_team_member_sk combination.

{% enddocs %}

{% docs fct_team_member_history %}

Contains the team members' employment history.

{% enddocs %}

{% docs fct_ping_namespace_metric %}

Unions together Gitlabdotcom Namespace Service Ping prep data for both database metrics and internal events metrics; each record represents one metric in one ping for one namespace. It is the Gitlabdotcom-equivalent model of `fct_ping_instance_metric`, which contains metric-level data for Self-Managed.

{% enddocs %}

{% docs fct_project_statistics_daily_snapshot %}

Daily snapshot of metrics related to a GitLab project

{% enddocs %}

{% docs dim_ai_gateway_unit_primitives %}

Dimensional model to enrich unit primitive information coming from the AI Gateway. This provides contextual information about the unit primitive, when it was introduced, what add-ons and licenses can be associated with the unit primitive, etc.

{% enddocs %}
