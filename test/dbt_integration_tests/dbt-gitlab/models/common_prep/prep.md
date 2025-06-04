{% docs prep_alliance_type_scd %}

Creates a base view with generated keys for the alliance type shared dimension and references in facts.

{% enddocs %}

{% docs prep_app_release %}

Creates base view with generated keys for application releaes. 

{% enddocs %}

{% docs prep_dr_partner_engagement %}

Creates a base view with generated keys for the dr partner engagement shared dimension and references in facts.

{% enddocs %}

{% docs prep_epic_user_request_collaboration_project %}

Parses epic links to the `Gitlab-org` group in the description and notes of epics inside the customer collaboration projects. These epics links are related to user feature requests from the product.

{% enddocs %}

{% docs prep_epic_user_request %}

Parses SFDC Opportunity / Accounts and Zendesk tickets links in the description and notes of epics inside the `Gitlab-org` group, together with its priority represented by the label `~"customer priority::[0-10]"` . These epics are related to user feature requests from the product.

For Opportunity and Zendesk tickets links found, the associated SFDC Account id is filled into the record.

If the same link is found twice in the description and the notes of the same epic, then the link that will be taken, together with its priority, will be the one in the note. If the same link is found in two different notes in the same epic, then the link that will be taken, together with its priority, will be the one in the latest updated note.

This model assumes that only one priority is placed in a given description or note.

{% enddocs %}

{% docs prep_issue_user_request_collaboration_project %}

Parses issue links to the `Gitlab-org` group in the description and notes of issues inside the customer collaboration projects. These issues links are related to user feature requests from the product.

It also looks for the issue links to the `Gitlab-org` group in the related issue links.

{% enddocs %}

{% docs prep_issue_user_request %}

Parses SFDC Opportunity / Accounts and Zendesk tickets links in the description and notes of issues inside the `Gitlab-org` group, together with its priority represented by the label `~"customer priority::[0-10]"` . These issues are related to user feature requests from the product.

For Opportunity and Zendesk tickets links found, the associated SFDC Account id is filled into the record.

If the same link is found twice in the description and the notes of the same issue, then the link that will be taken, together with its priority, will be the one in the note. If the same link is found in two different notes in the same issue, then the link that will be taken, together with its priority, will be the one in the latest updated note.

This model assumes that only one priority is placed in a given description or note.

{% enddocs %}

{% docs prep_ptp_scores_by_user %}

Takes the scores from prep_ptpt_scores_by_user_historical and returns the most recent score for each user.

A user will appear in this table only if:

1. They are in a trial 
1. They have a score in the "Free" model of 3-stars or higher
1. They have a score in the "Leads" model of 3-stars or higher.

The scores of this model are then used in mart_marketing_contact and the marketing pump to later be synced with Marketo and SFDC.

{% enddocs %}

{% docs prep_ptp_scores_by_user_historical %}

Takes scores from ptpt_scores, ptpf_scores, ptpl_scores and combines using the following logic to construct each user's score over time.

The rules for de-duplication of scores are:

1. If user only has PtP trial score then use that score
1. If user only has PtP free score then use that score
1. If user has multiple scores then:

   a. If active Trial PTP Score is 4 or 5 stars then use Trial PtP
   
   b. If active Free PtP Score is 5 stars then use Free Ptp

   c. If active Lead PtP Score is 5 stars then use Lead Ptp

   d. If active Free PtP Score is 4 stars then use Free Ptp

   e. If active Lead PtP Score is 4 stars then use Lead Ptp

   f. Else use Trial, Free or Lead Score, in that order

The resulting table is unique at the dim_marketing_contact_id and valid_from columns. The most recent scores for each user will have a NULL valid_to column

A new row is added for each dim_marketing_contact_id whenever:
- Their star rating changes
- Their model source (Trial, Free, Lead) changes

{% enddocs %}

{% docs prep_ptpt_scores_by_user %}

Takes the scores from ptpt_scores, transforms it to user / email address grain and uses the latest score date available.

{% enddocs %}

{% docs prep_ptpf_scores_by_user %}

Takes the scores from ptpf_scores, transforms it to user / email address grain and uses the latest score date available. It only syncs contacts with a `score_group >= 3`.

{% enddocs %}

{% docs prep_ptpl_scores_by_user %}

Takes the scores from ptpl_scores (Propensity to Purchase: Leads), transforms it to user / email address grain and uses the latest score date available. It only syncs contacts with a `score_group >= 3`.

{% enddocs %}

{% docs prep_sfdc_account %}

SFDC Account Prep table, used to clean and dedupe fields from a common source for use in further downstream dimensions.
Cleaning operations vary across columns, depending on the nature of the source data. See discussion in [MR](https://gitlab.com/gitlab-data/analytics/-/merge_requests/3782) for further details

{% enddocs %}

{% docs prep_campaign %}

Creates a base view with generated keys for the campaign shared dimension and fact and references in facts.

{% enddocs %}

{% docs prep_crm_person %}

Creates a base table containing contacts and leads from Salesforce joined to bizible and marketo data.

{% enddocs %}

{% docs prep_crm_user %}

Creates a base view with generated keys for the user and live crm sales hierarchy shared dimensions and references in facts.

{% enddocs %}

{% docs prep_crm_user_hierarchy %}

Creates a base view with generated keys for the CRM user hierarchy (live and historical) shared dimensions and references in facts. This is built from the stamped fields in the opportunity object, user roles, and, sales hierarchy areas used in target setting. It will be used in sales funnel analyses.

{% enddocs %}

{% docs prep_gitlab_dotcom_application_settings_monthly %}

This model captures a historical record of GitLab's default application settings for CI minutes and storage at a monthly grain.

{% enddocs %}

{% docs prep_app_release_major_minor %}

Creates base view with generated keys for application major and minor versions. 

{% enddocs %}

{% docs prep_gitlab_dotcom_plan %}

Creates a base view with generated keys for the plans shared dimension and fact and references in facts.

{% enddocs %}

{% docs prep_industry %}

Creates a base view with generated keys for the industry shared dimension and references in facts.

{% enddocs %}

{% docs prep_location_country %}

Creates a base view with generated keys for the geographic country shared dimension and references in facts. It also maps countries to geographic regions.

{% enddocs %}

{% docs prep_location_region %}

Creates a base view with generated keys for the geographic region shared dimension and references in facts.

{% enddocs %}

{% docs prep_namespace_plan_hist %}

dim_plan_id column:

Assumes if dim_plan_id is null that it is a free plan, plan id 34. Also, accounts for gold/ultimate plans in the past that did not have a trial plan id or trial name. The logic checks for plan names that are ultimate/gold AND have trial set to true and conforms them to plan id 102 which is the ultimate trial plan. After a trial expires, it is moved to a free plan, plan id 34. Therefore, after accounting for the gold/ultimate plans that had trial = TRUE, we can rely on the plan id and plan name out of the subscription and plan source tables to identify trials. The ultimate_trial plan name is plan id 102 and the premium_trial plan name is plan id 103. In a future iteration, this plan information should be conformed with the dim_product_tier dimension to have a single source of truth for plan information at GitLab.

{% enddocs %}

{% docs prep_bizible_marketing_channel_path %}

Creates a base view with generated keys for the marketing channel path shared dimension and references in facts.

{% enddocs %}

{% docs prep_sales_qualified_source %}

Creates a base view with generated keys for the sales qualified source (source of an opportunity) shared dimension and references in facts.

{% enddocs %}

{% docs prep_order_type %}

Creates a base view with generated keys for the order type shared dimension and references in facts.

{% enddocs %}

{% docs prep_sales_funnel_kpi %}

Creates a base view with generated keys for the sales funnel kpi dimension and references in facts.

{% enddocs %}

{% docs prep_deal_path %}

Creates a base view with generated keys for the deal path shared dimension and references in facts.

{% enddocs %}

{% docs prep_recurring_charge_subscription_monthly %}

Sums MRR and ARR charges by subscription by month. MRR and ARR values are also broken out by delivery type (Self-Managed, SaaS, Others) at the same grain.

To align the subscriptions in this table with `prep_recurring_charge`, filter on `subscription_status IN ('Active', Cancelled')`.

{% enddocs %}

{% docs prep_charge %}

Creates a base view of recurring charges that are not amortized over the months. This prep table is used for transaction line analyses that do not require amortization of charges.

{% enddocs %}

{% docs prep_sales_segment %}

Creates a base view with generated keys for the sales segment shared dimension and references in facts.

{% enddocs %}

{% docs prep_sales_territory %}

Creates a base view with generated keys for the sales territory shared dimension and references in facts.

{% enddocs %}

{% docs prep_subscription %}

Creates a base view with generated keys for the subscription shared dimension and references in facts.

{% enddocs %}

{% docs prep_product_tier %}

 This table creates keys for the common product tier dimension that is used across gitlab.com and Zuora data sources.

 The granularity of the table is product_tier.

{% enddocs %}

{% docs prep_quote %}

Creates a Quote Prep table for representing Zuora quotes and associated metadata for shared dimension and references in facts.

The grain of the table is quote_id.

{% enddocs %}

{% docs prep_license %}

Creates a License Prep table for representing generated licenses and associated metadata for shared dimension and references in facts.

The grain of the table is license_id.

{% enddocs %}

{% docs prep_usage_self_managed_seat_link %}

This prep table contains Seat Link data at a daily grain for downstream aggregation and summarization, as well as flags for data quality.

Self-managed EE instances send [Seat Link](https://docs.gitlab.com/ee/subscriptions/self_managed/#seat-link) usage data to [CustomerDot](https://gitlab.com/gitlab-org/customers-gitlab-com) on a daily basis. This information includes a count of active users and a maximum count of users historically in order to assist the [true up process](https://docs.gitlab.com/ee/subscriptions/self_managed/#users-over-license). Additional details can be found in [this doc](https://gitlab.com/gitlab-org/customers-gitlab-com/-/blob/staging/doc/reconciliations.md).

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs prep_subscription_lineage_intermediate %}

The `zuora_subs` CTE de-duplicates Zuora subscriptions. Zuora keeps track of different versions of a subscription via the field "version". However, it's possible for there to be multiple version of a single Zuora version. The data with account_id = '2c92a0fc55a0dc530155c01a026806bd' in the base zuora_subscription table exemplifies this. There are multiple rows with a version of 4. The CTE adds a row number based on the updated_date where a value of 1 means it's the newest version of that version. It also filters subscriptions down to those that have either "Active" or "Cancelled" statuses since those are the only ones that we care about.

The `renewal_subs` CTE creates a lookup table for renewal subscriptions, their parent, and the earliest contract start date. The `contract_effective_date` field was found to be the best identifier for a subscriptions cohort, hence why we're finding the earliest relevant one here. The renewal_row is generated because there are instances where multiple subscriptions point to the same renewal. We generally will want the oldest one for info like cohort date.

The final select statement creates a new field specifically for counting subscriptions and generates appropriate cohort dates. Because we want to count renewal subscriptions as part of their parent, we have the slug for counting so that we don't artificially inflate numbers. It also pickes the most recent version of a subscription.

The subscription_end_month calculation is taken as the previous month for a few reasons. Technically, on Zuora's side, the effective end date stored in the database the day _after_ the subscription ended. (More info here https://community.zuora.com/t5/Subscriptions/How-to-get-ALL-the-products-per-active-subscription/td-p/2224) By subtracting the month, we're guaranteed to get the correct month for an end date. If in the DB it ends 7/31, then in reality that is the day before and is therefore not in effect for the month of July (because it has to be in effect on the last day to be in force for that month). If the end date is 8/1, then it is in effect for the month of July and we're making the proper calculation.

{% enddocs %}

{% docs prep_subscription_lineage %}

Connects a subscription to all of the subscriptions in its lineage. To understand more about a subscription's relationship to others, please see [the handbook under Zuora Subscription Data Management](https://about.gitlab.com/handbook/finance/accounting/)

The `flattening` CTE flattens the intermediate model based on the array in the renewal slug field set in the base subscription model. Lineage is initially set here as the values in the parent slug and any renewal slugs. The OUTER => TRUE setting is like doing an outer join and will return rows even if the renewal slug is null.  

The recursive CTE function generate the full lineage. The anchor query pulls from the flattening CTE and sets up the initial lineage. If there is a renewal subscription then it will continue to the next part of the CTE, but if there are no renewals then the recursive clause will return no additional results.

The recursive clause joins the renewal slug from the anchor clause to the subscription slug of the next iteration of the recursive clause. We're keeping track of the parent slug as the "root" for the initial recursion (this is the "ultimate parent" of the lineage). Within the recursive clause we're checking if there are any further renewals before setting the child count.

The next CTE takes the full union of the results and finds the longest lineage for every parent slug based on the children_count. This CTE is overexpressive and could most likely be simplified with the deduplication CTE. The final dedupe CTE returns a single value for every root and it's full downstream lineage.

{% enddocs %}

{% docs prep_subscription_lineage_parentage_start %}
This is the first part of a two-part model. (It is in two parts because of memory constraints.)

The `flattened` CTE takes the data from lineage, which starts in the following state:


|SUBSCRIPTION_NAME_SLUGIFY|LINEAGE|
|:-:|:-:|
|a-s00011816|a-s00011817,a-s00011818|
|a-s00011817|a-s00011818|
|a-s00003063|a-s00011816,a-s00011817,a-s00011818|


This flattens them to be be in one-per row. Rxample:

|SUBSCRIPTION_NAME_SLUGIFY|SUBSCRIPTIONS_IN_LINEAGE|CHILD_INDEX|
|:-:|:-:|:-:|
|a-s00011817|a-s00011818|0|
|a-s00011816|a-s00011817|0|
|a-s00011816|a-s00011818|1|
|a-s00003063|a-s00011816|0|
|a-s00003063|a-s00011817|1|

Then we identify the version of the `subscriptions_in_lineage` with the max depth (in the `find_max_depth` CTE) and join it to the `flattened` CTE in the `with_parents` CTE. This allows us to identify the ultimate parent subscription in any given subscription.

For this series of subscriptions, the transformation result is:

|ULTIMATE_PARENT_SUB|CHILD_SUB|DEPTH|
|:-:|:-:|:-:|
|a-s00003063|a-s00011816|0|
|a-s00003063|a-s00011817|1|
|a-s00003063|a-s00011818|2|

Of note here is that parent accounts _only_ appear in the parents column. `a-s00003063` does not appear linked to itself. (We correct for this in `subscriptions_xf` when introducing the `subscription_slug_for_counting` value and coalescing it with the slug.)

In the final CTE `finalish`, we join to intermediate to retreive the cohort dates before joining to `subscription_intermediate` in `subscription_xf`.

The end result of those same subscriptions:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003063|a-s00011816|2014-08-01|2014-07-01|2014-01-01|
|a-s00003063|a-s00011817|2014-08-01|2014-07-01|2014-01-01|
|a-s00003063|a-s00011818|2014-08-01|2014-07-01|2014-01-01|

This transformation process does not handle the consolidation of subscriptions, though, which is what `zuora_subscription_parentage_finish` picks up.

{% enddocs %}

{% docs prep_subscription_lineage_parentage_finish %}

This is the second part of a two-part model. (It is in two parts because of memory constraints.) For the first part, please checkout the docs for zuora_subscription_parentage_start.

Some accounts are not a direct renewal, they are the consolidation of many subscriptions into one. While the lineage model is build to accomodate these well, simply flattening the model produces one parent for many children accounts, for example:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|
|a-s00003873|a-s00005209|2017-01-01|2017-01-01|2017-01-01|

Since the whole point of ultimate parent is to understand cohorts, this poses a problem (not just for fan outs when joining) because it is inaccurate.

The `new_base` CTE identifies all affected subscriptions, while `consolidated_parents` and `deduped_parents` find the oldest version of the subscription.

This produces

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|

but drops the subscriptions that are not the ultimate parent but had not previously been identified as children, in this case `a-s00003873`.

The first part of the `unioned` CTE isolates these subscriptions, naming them children of the newly-minted ultimate parent subscription (really just the oldest in a collection of related subscriptions), producing

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|


It unions this to the results of `deduped_consolidations` and all original base table where the subscriptions were not affected by consolidations. Finally we deduplicate one more time.  

The final result:

|ULTIMATE_PARENT_SUB|CHILD_SUB|COHORT_MONTH|COHORT_QUARTER|COHORT_YEAR|
|:-:|:-:|:-:|:-:|:-:|
|a-s00003114|a-s00009998|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00003873|2016-01-01|2016-01-01|2016-01-01|
|a-s00003114|a-s00005209|2016-01-01|2016-01-01|2016-01-01|


{% enddocs %}


{% docs prep_gainsight_source_model_counts %}
This data model is used to capture the counts for all the source tables used for Gainsight.

{% enddocs %}

{% docs prep_saas_usage_ping_subscription_mapped_wave_2_3_metrics %}

A recreation of `prep_usage_ping_subscription_mapped_wave_2_3_metrics` for _SaaS_ users.

{% enddocs %}


{% docs prep_ping_instance_flattened_uploaded_at %}

Column `uploaded_at` (`TIMESTAMP` data type) represent the moment WHEN the record is ingested into Snowflake. 
The main motivation for introducing this column is for a few reasons:
1. Be able to track back the exact date and time of data ingesting _(this information wasn't known to us)_
1. Improving incremental load using `uploaded_at` column 
1. Support "late_arriving" ping automatically, without the need to full-refresh a full lineage

{% enddocs %}

{% docs prep_saas_usage_ping_namespace %}

fct table from the usage_ping_namespace. Granularity of one row per namespace per metric per run.

{% enddocs %}

{% docs prep_internal_events_ping_namespace %}

fct table from the internal_events_namespace_metrics. Granularity of one row per namespace per metric per run.

{% enddocs %}

{% docs prep_saas_usage_ping_free_user_metrics %}

Table containing **free** SaaS users in preparation for free user usage ping metrics fact table.

The grain of this table is one row per namespace per month.

{% enddocs %}

{% docs prep_event_all %}

Prep table that unions together all of the monthly partitions created from the [prep_event model](https://gitlab-data.gitlab.io/analytics/#!/model/model.gitlab_snowflake.prep_event)

{% enddocs %}

{% docs prep_ci_pipeline %}

Creates a base view of CI pipelines. More info about CI pipelines [is available here](https://docs.gitlab.com/ee/ci/pipelines/)

{% enddocs %}

{% docs prep_action %}

Prep table for the dim table `dim_action`.

More info about [events tracked](https://docs.gitlab.com/ee/api/events.html)

{% enddocs %}

{% docs prep_user %}
Prep table for the dim table `dim_user`.

This table is currently the first iteration. This is a relatively narrow table. A lot of metadata needs to be added.

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

{% docs prep_issue %}

Prep table used to build `dim_merge_request`

More information about [Issues](https://docs.gitlab.com/ee/user/project/issues/)

{% enddocs %}

{% docs prep_merge_request %}

Prep table used to build `dim_merge_request`

More information about [CI Pipelines here](https://docs.gitlab.com/ee/user/project/merge_requests/)

{% enddocs %}

{% docs prep_member_accepted_invites %}

Prep table used to capture user accepted invites to any namespace.
An 'accept_invite' event is captured only when the user takes an action to accept the invite to a namespace, in this case the INVITE_ACCEPTED_AT IS NOT NULL.
Existing users who're granted the access to a namespace are not included as they don't need to take an action to 'accept' the invite, In such cases, the INVITE_ACCEPTED_AT IS NULL.

{% enddocs %}

{% docs prep_ci_build %}

Prep table used to build the `dim_ci_build` table.

More information about [CI Pipelines here](https://docs.gitlab.com/ee/ci/pipelines/)

{% enddocs %}

{% docs prep_ci_runner %}

Prep table used to build the `dim_ci_runner` table.

More information about [CI Pipelines here](https://docs.gitlab.com/ee/ci/pipelines/)

{% enddocs %}

{% docs prep_epic %}

Prep table for the dim table `dim_epic`.

{% enddocs %}

{% docs prep_note %}

Prep table for the dim table `dim_note`.

{% enddocs %}

{% docs prep_deployment %}

Prep table for the dim table `dim_deployment` that is not yet created.

{% enddocs %}

{% docs uploaded_at %}

Column `uploaded_at` (`TIMESTAMP` data type) represent the moment WHEN the record is ingested into Snowflake. 
The main motivation for introducing this column is for a few reasons:
1. Be able to track back the exact date and time of data ingesting _(this information wasn't known to us)_
1. Improving incremental load using `uploaded_at` column 
1. Support "late_arriving" ping automatically, without the need to full-refresh a full lineage

{% enddocs %}

{% docs prep_package %}

Prep table for the dim table `dim_package` that is not yet created. It is also used in the `prep_event` table

{% enddocs %}

{% docs prep_issue_severity %}

Prep table used to get Severity field from GitLab Incident issues for the `dim_issue` table.

More information about [GitLab Incidents here](https://docs.gitlab.com/ee/operations/incident_management/incidents.html)

{% enddocs %}

{% docs prep_label_links %}

Prep table used to join GitLab Labels to Issues, Merge Requests, & Epics

More information about [labels here](https://docs.gitlab.com/ee/user/project/labels.html)

{% enddocs %}

{% docs prep_labels %}

Prep table used to build `dim_issues`, `dim_merge_requests`, `dim_epics` tables. Holds detailed information about the labels used across GitLab

More information about [labels here](https://docs.gitlab.com/ee/user/project/labels.html)

{% enddocs %}

{% docs prep_issue_links %}

Prep table used to build `dim_issue_links` This table shows relationships of GitLab issues to other GitLab issues. It represents linked issues, which you can learn more about [here](https://docs.gitlab.com/ee/user/project/issues/related_issues.html)

{% enddocs %}

{% docs prep_release %}

Prep table for the dim table `dim_release` that is not yet created. It is also used in the `prep_event` table

{% enddocs %}

{% docs prep_requirement %}

Prep table for the dim table `dim_requirement`. It is also used in the `prep_event` table.
{% enddocs %}

{% docs prep_geozone %}

Prep table applying business logic to the geozone source data to prepare to be combined with the location factor data in the `dim_locality` table.

{% enddocs %}

{% docs prep_location_factor %}

Prep table applying business logic to the location factor source data to prepare to be combined with the geozone data in the `dim_locality` table.

The source data contains several versions of source data with different format.  This table conforms all of the formats into a single format.  The business logic contains an intermediate step that classifies each type of formatting used:

| Type | Description | Example |
| ---- | ----------- | ------- |
| Type 1 | Format used prior to 2020-12-10 | Everywhere else, Maine |
| Type 2 | Format used when there is a metro area under a state or province | Port Townsend, Washington |
| Type 3 | Format used when there is a sublocation given for a metro area | Sydney, New South Wales |
| Type 4 | Format used when a state or province is given with not metro area | Hawaii |
| Type 5 | Format used when a metro area is given with out a state or province or a sublocation  | Paris |

```yaml
# Type 1
- country: United States
  area: Everywhere else, Maine
  locationFactor: 65.0
# Type 2
- country: United States
  states_or_provinces:
    - name: Washington
      metro_areas:
        - name: Port Townsend
          factor: 77.00
# Type 3
- country: Australia
  metro_areas:
    - name: Sydney
      factor: 70.00
      sub_location: New South Wales
# Type 4
- country: United States
  states_or_provinces:
    - name: Hawaii
      factor: 86.00
# Type 5
- country: France
  metro_areas:
    - name: Paris
      factor: 67.00
```

{% enddocs %}

{% docs prep_ping_instance %}

Prep table to read Service ping data from Versions app and to build `dim_ping_instance` table.

Below are some additional details about the table:
* Type of Data: `Instance-level Service Ping from Versions app`
* Aggregate Grain: `One record per service ping (dim_ping_instance_id)`
* Time Grain: `None`
* Use case: `Service Ping prep table`

{% enddocs %}

{% docs prep_ping_instance_flattened %}

Prep table to flatten the Service ping JSON payload that is sourced from Versions app and to build `fct_ping_instance_metric` table.

Below are some additional details about the table:
* Type of Data: `Instance-level Service Ping from Versions app`
* Aggregate Grain: `One record per service ping (dim_ping_instance_id) per metric (metrics_path)`
* Time Grain: `None`
* Use case: `Service Ping metric-level prep table`

{% enddocs %}

{% docs prep_crm_task %}

Prep model of all [Salesforce Tasks](https://help.salesforce.com/s/articleView?id=sf.tasks.htm&type=5) that record activities related to leads, contacts, opportunities, and accounts.

{% enddocs %}

{% docs prep_performance_indicators_yaml %}

Prep table to UNION all performance indicator yaml files and to build [`fct_performance_indicator_targets`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.fct_performance_indicator_targets). This table replaces [`legacy.performance_indicators_yaml_historical`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.performance_indicators_yaml_historical).

This is modeled like a Type 2 Slowly Changing Dimension and therefore contains historical values from the PI files, starting on 2020-08-18 (when we started capturing this data). Instead of providing a snapshot for each day, this model captures a new record when changes occurred in the file, as noted by the `valid_from_date` and `valid_to_date`.

All columns are pulled directly from the yaml files, with the exception of the following metadata columns: `performance_indicator_pk`, `_dbt_source_relation`, `unique_key`, `snapshot_date`, `date_first_added`, `valid_from_date`, `valid_to_date`.

{% enddocs %}

{% docs has_ci_build_failed_context %}

A flag to indicate if the event has additional information in the context field related to `ci_build_failed`.  This context is defined in the Gitlab [iglu project](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab).

{% enddocs %}

{% docs has_wiki_page_context %}

A flag to indicate if the event has additional information in the context field related to `wiki_page`.  This context is defined in the Gitlab [iglu project](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab)

{% enddocs %}

{% docs has_email_campaigns_context %}

A flag to indicate if the event has additional information in the context field related to `email_campaigns`.  This context is defined in the Gitlab [iglu project](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab)

{% enddocs %}

{% docs has_design_management_context %}

A flag to indicate if the event has additional information in the context field related to `design_management`.  This context is defined in the Gitlab [iglu project](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab)

{% enddocs %}

{% docs has_customer_standard_context %}

A flag to indicate if the event has additional information in the context field related to `customer_standard`.  This context is defined in the Gitlab [iglu project](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab)

{% enddocs %}

{% docs has_secure_scan_context %}

A flag to indicate if the event has additional information in the context field related to `secure_scan`.  This context is defined in the Gitlab [iglu project](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab)

{% enddocs %}

{% docs has_subscription_auto_renew_context %}

A flag to indicate if the event has additional information in the context field related to `subscription_auto_renew`.  This context is defined in the Gitlab [iglu project](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab)

{% enddocs %}

{% docs dim_behavior_contexts_sk %}

A surrogate key for each distinct combination of context flags.  This is built as a conceptual [junk dimension](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/junk-dimension/) and can be used to build a dimension table to limit the number of columns on a fct table.

{% enddocs %}

{% docs user_city %}

The city associated with the user related to the event.

{% enddocs %}

{% docs user_country %}

The country code associated with the user related to the event.

{% enddocs %}

{% docs user_region %}

The region code associated with the user related to the event.

{% enddocs %}

{% docs user_region_name %}

The region name associated with the user related to the event.

{% enddocs %}

{% docs user_timezone_name %}

The name of the timezone associated with the user related to the event.

{% enddocs %}

{% docs dim_user_location_sk %}

A surrogate key for the attributes of the user location.  This is built as a conceptual [dimension](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/dimension-table-structure/) and can be used to build a dimension table to limit the number of columns on a fact table.

{% enddocs %}

{% docs prep_snowplow_unnested_events_all %}

This is the primary events view which is the union of the Fishtown and GitLab tables across the last 25 months of snowplow_YYYY_MM schemas. All of the unstructured including both staging and non-staging events are unpacked - [link click tracking](https://github.com/snowplow/snowplow/wiki/2-Specific-event-tracking-with-the-Javascript-tracker#39-link-click-tracking), [form tracking](https://github.com/snowplow/snowplow/wiki/2-Specific-event-tracking-with-the-Javascript-tracker#3101-enableformtracking), and [time tracking](https://github.com/snowplow/snowplow/wiki/2-Specific-event-tracking-with-the-Javascript-tracker#timing).

{% enddocs %}

{% docs prep_snowplow_unnested_events_all_30 %}

This model prepares Snowplow event data by unioning data from the Fishtown and GitLab tables across the last 30 days of snowplow_YYYY_MM schemas. All of the unstructured including both staging and non-staging events are unpacked. It is similar to `prep_snowplow_unnested_events_all` key difference is that `prep_snowplow_unnested_events_all_30` unions data from the last 30 days of Snowplow event schemas (snowplow_YYYY_MM), while `prep_snowplow_unnested_events_all` unions data from the last 800 days (approximately 26 months).

{% enddocs %}

{% docs prep_billing_account %}

Prep model for merging the billing accounts data from both Zuora and CDot sources. This model will be used as a source model for creating `dim_billing_account` core business data object downstream.

{% enddocs %}

{% docs dim_billing_account_sk %}

A surrogate key that uniquely identifes each row of the billing account table.  It is currently formed by hashing the billing account IDs from Zuora that uniquely identify a Zuora account associated with a given Subscription ID. This is built as a conceptual [dimension](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/dimension-table-structure/) and can be used to build a dimension table to limit the number of columns on a fact table.

{% enddocs %}

{% docs link_click_element_id %}

The element id from the unstructured link click event

{% enddocs %}

{% docs is_staging_event %}

Flag to indicate whether the event is staging or not. Staging events are defined as events where `app_id = 'gitlab-staging'` or the `page_url` indicates that the event comes from a staging environment.

{% enddocs %}

{% docs prep_user_trial %}

Prep table to store information about our users, trial users are also included. The data is sourced from an underlying tap-postgres customers table from customers.gitlab.com.

{% enddocs %}

{% docs dim_user_sk %}

A surrogate key that uniquely identifes each row of the User table.  This is built as a conceptual [dimension](https://www.kimballgroup.com/data-warehouse-business-intelligence-resources/kimball-techniques/dimensional-modeling-techniques/dimension-table-structure/) and can be used to build a dimension table to limit the number of columns on a fact table.

{% enddocs %}

{% docs prep_team_member_position %}

This table contains team members' job history, including any changes in manager, supervisory organization, job family, job specialty, department, division, entity, management level and job grade.

This table includes BambooHR and Workday data. There are some fields that don't exist in the BHR data that will show up as NULL prior to 2022-06-16: team_id, suporg, job_code, job_family, is_position_active.


{% enddocs %}

{% docs prep_namespace_order_trial %}

This model contains data for all trial orders for each namespace from CDot trial histories and CDot orders that are being sourced from customers.gitlab.com.

{% enddocs %}

{% docs prep_order %}

This table stores information about the subscription purchased by the customer plus some additional details used for syncing purposes with GitLab.com. The data is sourced from tap-postgres from the orders table from customers.gitlab.com.

{% enddocs %}

{% docs prep_cloud_activation %}

This model contains data for the cloud activations sourced from tap-postgres table from customers.gitlab.com. It stores information about all the activation codes that were generated for Cloud licenses. Customers use this code after the installation of their GitLab instance. 

This model contains other join keys like `billing_account_id`, `subscription_name` etc.. to be able to join back to Salesforce, Zuora, dimdate data respectively.

{% enddocs %}

{% docs prep_license_subscription %}

This model contains the logic for connecting product licenses and subscriptions for use in connecting service ping data to customer accounts.

{% enddocs %}


{% docs prep_milestone %}

All milestones created within a namespace, with details including the start date, due date, description, and title.

{% enddocs %}

{% docs prep_latest_seat_link_installation %}

Contains the latest Seat Link record for every installation in the source Seat Link model.

{% enddocs %}

{% docs prep_seat_link_installation_daily %}

Contains a Seat Link record for every installation and day from the source Seat Link model. We exclude records that have no add_on_type, host_id and instance_id information.

{% enddocs %}

{% docs add_on_metrics_user_count %}

A JSON array continaing the latest add_on_type, purchased_seats and assigned_seats from the source Seat Link model.

{% enddocs %}

{% docs add_on_type_original %}

Original add-on type from source data. May contain values like `code_suggestions`, which is [declared](https://gitlab.com/gitlab-org/gitlab/-/blob/master/ee/app/models/gitlab_subscriptions/add_on.rb#L20) as an add-on in our codebase but renamed to `GitLab Duo Pro` in data models for accuracy.

This field is retained to monitor upstream changes via dbt tests, while `add_on_type` contains renamed versions.

Note: Plans exist to rename `code_suggestions` to `duo_pro` in the future. [More info](https://gitlab.com/gitlab-org/customers-gitlab-com/-/issues/?sort=updated_desc&state=opened&search=rename%20code_suggestions&first_page_size=20)

{% enddocs %}

{% docs purchased_seats %}

Number of seats purchased for the specified product. Also known as "licensed users".

{% enddocs %}

{% docs assigned_seats %}

Number of seats assigned in the installation for the specified product. 
Importantly, a license code can be applied to multiple installations, 
so a single license can have assigned seats greater than purchased seats. 
For the main GitLab application, we do not prevent customers from 
assigning more seats than seats purchased. Also known as "billable users".

{% enddocs %}

{% docs prep_pto %}

Prep table to store approved absences that fall on a working day and an additional absence_status column. 

{% enddocs %}

{% docs prep_crm_case %}

This table contains data about SFDC case objects.

{% enddocs %}

{% docs prep_crm_case_history %}

This table contains all changes to SFDC Case fields if field history tracking is enabled for that field within SFDC settings.

{% enddocs %}

{% docs prep_snowplow_sessions_all %}

Unioned monthly partitions for all Snowplow sessions.

{% enddocs %}

{% docs prep_team_member_history %}

Directory of team_members' employment dates.

{% enddocs %}

{% docs prep_trial %}

Grain: `internal_order_id` - `dim_namespace_id` - `dim_subscription_id` - `order_updated_at`

This model collects all trials that start from the subscription portal. For this we use the `customers_db_orders_snapshots_base` model in order to isolate them. This model does the following:

* It isolates the orders that are flagged with the column `is_trial = TRUE`
* It joins with customers, users and namespaces. 

Finally, this model identifies if a trial has been converted or not. To achieve that, we join the trials to the `order_snapshots` by selecting only the orders that converted to subscription after the trial starting date (an example has been provided below). We exclude ci_minutes/compute_minutes orders from the `order_snapshots`.   

In order to identify which subscriptions are actually valid and not refunded, we join to `zuora_rate_plan` and `zuora_base_mrr` models to filter out subscriptions that have (mrr <= 0 and tcv <=0). In this case, we also filter out those subscriptions that are cancelled instantly or fully refunded after a certain period.

Examples:

| ORDER_ID | ORDER_UPDATED_AT        | ORDER_START_DATE  | ORDER_END_DATE | ORDER_IS_TRIAL | SUBSCRIPTION_NAME_SLUGIFY |
|----------|-------------------------|-------------------|----------------|----------------|---------------------------|
| 32177    | 2019-09-06 23:09:21.858 | 2019-08-17        | 2019-09-15     | TRUE           |                           |
| 32177    | 2019-09-13 22:39:18.916 | 2019-08-17        | 2019-09-27     | TRUE           |                           |
| 32177    | 2019-09-26 21:26:23.227 | 2019-08-17        | 2019-10-02     | TRUE           |                           |
| 32177    | 2019-10-02 16:32:45.664 | 2019-10-02        | 2019-10-04     | TRUE           |                           |
| 32177    | 2019-10-02 00:00:00.075 | 2019-10-02        |                | FALSE          |                           |
| 32177    | 2019-10-03 20:11:31.497 | 2019-10-02        | 2020-10-02     | FALSE          | order-1-name-gold         |

Note: The column `subscription_name_slugify` has been anonymised.

This order exemplifies perfectly what is happening in the table `customers_db_orders`. When the order starts, 17th Aug, 2019, it is a trial. That means that the flag `order_is_trial` is set to TRUE. But it doesn't have either a subscription_id or a subscription_name (`subscription_name_slugify` is null). When it converts, 2nd Nov, 2019, the `order_is_trial` flag is set to `FALSE`, the order_start_date (and order_end_date) is changed and a `subscription_name` and `subscription_id` are set! (last row of the table)


{% enddocs %}

{% docs prep_ping_instance_date_flags %}

A table to calculate the window functions over all of the `prep_ping_instance` data.  These calculations were separated from the `dim_ping_instance` table to improve efficiency of the model.

{% enddocs %}

{% docs prep_ramp_subscription %}

A table to show the ramps booked - union of the legacy and new ramp structure.

{% enddocs %}

{% docs prep_repo_storage_ratio_daily %}

**prep_repo_storage_ratio_daily**

    Mission: Map daily repository storage usage to Profit & Loss categories.
    Objective: Provide daily repository storage usage data by pl_category for cost analysis and reporting.
    Granularity: Daily
    Inputs: GitLab API: repository storage usage per day and per namespace plan type
    Accuracy rating: Very High
    Completeness rating: Very High

    snapshot_day: date - Date of the snapshot
    finance_pl_category: varchar - Profit & Loss category
    repo_size_gb: float - Repository size in GB
    percent_repo_size_gb: float - Percentage of repository size

{% enddocs %}

{% docs prep_container_registry_ratio_daily %}

**prep_container_registry_ratio_daily**

    Mission: Map daily container registry usage to Profit & Loss categories.
    Objective: Provide daily container registry usage data by pl_category for cost analysis and reporting.
    Granularity: Daily
    Inputs: GitLab API: Container registry usage per namespace per day
    Accuracy rating: Very High
    Completeness rating: Very High

    snapshot_day: date - Date of the snapshot
    finance_pl_category: varchar - Profit & Loss category
    container_registry_gb: float - Container registry size in GB
    percent_container_registry_size: float - Percentage of container registry size

{% enddocs %}

{% docs prep_ci_runner_ratio_daily %}

**prep_ci_runner_ratio_daily**

    Mission: Map daily CI runner usage to Profit & Loss categories.
    Objective: Provide daily CI runner usage data by pl_category for cost analysis and reporting.
    Granularity: Daily
    Inputs: GitLab API: ci_minutes consumption per type of customer and plan over time
    Accuracy rating: Very High
    Completeness rating: Very High

    reporting_day: date - Date of the report
    mapping: varchar - Mapping data
    finance_pl_category: varchar - Profit & Loss category
    total_ci_minutes: number - Total CI minutes used
    pct_ci_minutes: number - Percentage of CI minutes used

{% enddocs %}

{% docs prep_ci_build_artifact_ratio_daily %}

**prep_ci_build_artifact_ratio_daily**

    Mission: Map daily build artifacts usage to Profit & Loss categories.
    Objective: Provide daily build artifacts usage data by pl_category for cost analysis and reporting.
    Granularity: Daily
    Inputs: GitLab Storage per namespace statistics, namespace_pl_daily
    Accuracy rating: Very High
    Completeness rating: Very High

    snapshot_day: date - Date of the snapshot
    finance_pl_category: varchar - Profit & Loss category
    build_artifacts_gb: float - Build artifacts size in GB
    percent_build_artifacts_size: float - Percentage of build artifacts size

{% enddocs %}

{% docs prep_gitlab_dotcom_project_statistics_daily_snapshot %}

Daily snapshot of statitics related to a GitLab project

{% enddocs %}
