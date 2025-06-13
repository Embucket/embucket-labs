{% docs map_bizible_marketing_channel_path %}
 Intermediate table to expose the mapped marketing channel data.
{% enddocs %}

{% docs map_bizible_campaign_grouping %}
 Mapping table for Bizible marketing campaigns which groups the campaigns based on the touchpoint type, ad campaign name, parent campaign id, among other attributes. This generates the `bizible_itegrated_campaign_grouping`, `integrated_campaign_grouping`, `touchpoint_segment`, and `gtm_motion` mapping.
{% enddocs %}

{% docs map_crm_account %}
 Mapping table for dimension keys related to crm accounts so they can be reused in fact tables containing account ids.
{% enddocs %}

{% docs map_crm_opportunity %}
 Mapping table for dimension keys related to opportunities so they can be reused in fact tables containing quotes.
{% enddocs %}

{% docs map_gitlab_dotcom_xmau_metrics %}
 Mapping table that maps events from the Legacy Usage Data Model to the Common Data model. It also maps events to stages, groups, sections, and xMAU designations. 
{% enddocs %}

{% docs map_ip_to_country %}
Table for mapping ip address ranges to location ids.
{% enddocs %}

{% docs map_license_subscription_account%}
Table with mapping keys for license_md5 to subscription_id to crm (Salesforce) account ids
{% enddocs %}

{% docs map_merged_crm_account%}

Table mapping current crm account ids to accounts merged in the past.

{% enddocs %}

{% docs map_moved_duplicated_issue %}

Table mapping issues to the latest issue they were moved and / or duplicated to.

Example:

`Issue A` is moved to `Issue B`, `Issue B` is closed as duplicate of `Issue C`, `Issue C` is moved to `Issue D`

Then in our mapping table we would have:

| issue_id | last_moved_duplicated_issue_id | dim_issue_sk |
| -- | -- | -- |
| Issue A | Issue D | Surrogate Key for Issue D |
| Issue B | Issue D | Surrogate Key for Issue D |
| Issue C | Issue D | Surrogate Key for Issue D |
| Issue D | Issue D | Surrogate Key for Issue D |

{% enddocs %}

{% docs map_product_tier %}

 Table for mapping Zuora Product Rate Plans to Product Tier, Delivery Type, and Ranking.

{% enddocs %}

{% docs map_namespace_internal %}

This View contains the list of ultimate parent namespace ids that are internal to gitlab. In the future this list should be sourced from an upstream data sources or determined based on billing account in customer db if possible.

{% enddocs %}

{% docs map_team_member_bamboo_gitlab_dotcom_gitlab_ops %}
Table for mapping GitLab team members across bambooHR, GitLab.com Postgres DB, and GitLab Ops

{% enddocs %}

{% docs map_ci_runner_project %}
Table for mapping GitLab.com CI Runner to a specific project.

More info about [CI Runners here](https://docs.gitlab.com/ee/ci/runners/)
{% enddocs %}


{% docs map_subscription_opportunity %}

The distinct combination of subscriptions and opportunities generated through the rules defined in `prep_subscription_opportunity_mapping`. A flag has been created to indicate the subscription-opportunty mappings filled in by taking the most recent opportunity_id associated with a version of the subscription with the same subscription_name which we believe to have the lowest level of fidelity.

{% enddocs %}



{% docs map_team_member_user %}

This table is the distinct combination of the Gitlab team members and there Gitlab user IDs.  The two values are connected using the Gitlab user names that is recorded in the HRIS system

{% enddocs %}

{% docs map_latest_subscription_namespace_monthly %}

This table contains the most recent subscription version associated with each namespace in each month, and represents the most complete namespace <> subscription mapping we have. It prefers the Zuora namespace <> subscription mappings, then fills in any nulls with bridge logic. The end objective is to backfill Zuora with all mappings so that `dim_subscription` can be the SSOT for namespace <> subscription relationships.

Although in the prep data, namespaces can be associated with multiple `dim_subscription_id`s and/or multiple `dim_subscription_id_original`s in a single month, we use a `QUALIFY` statement in this table to limit down to **one** subscription per namespace per month (the most recently created subscription).

{% enddocs %}


{% docs map_alternative_lead_demographics %}

This tables creates an [alterntive mapping](https://about.gitlab.com/handbook/marketing/strategy-performance/marketing-metrics/#alternative-method-for-account-demographics-fields-on-leads) for GEO and Segment values for leads based on data from data enrichment services.

{% enddocs %}

{% docs map_project_internal %}

This View contains the list of projects that are under ultimate parent namespace ids that are internal to gitlab. This mapping should be used to filter entities such as Issues and Merge requests when only internal GitLab data is needed.

{% enddocs %}

{% docs map_epic_internal %}

This View contains the list of epics that are under ultimate parent namespace IDs and the namespace IDs that are internal to gitlab. This mapping should be used to filter epics when only internal GitLab data is needed.

{% enddocs %}

{% docs snowflake_user_name %}
The user name of the snowflake user.  This is different than the user email and is used connect to query activity.
{% enddocs %}

{% docs map_namespace_subscription_product %}

This model maps namespaces to their most recent paid GitLab.com (SaaS) subscription product, excluding one-time charges and specific product offerings.

It provides a daily snapshot of active subscription product(s) for each namespace by sourcing data from the unified `prep_charge_mrr_daily_with_namespace_and_installation` model which expands the subscription-product across the effective dates of the associated charge.

{% enddocs %}

{% docs map_installation_subscription_product %}

This model maps installations to their most recent paid Self-Managed or Dedicated subscription product, excluding one-time charges and specific product offerings.

It provides a daily snapshot of active subscription product(s) for each installation by sourcing data from the unified `prep_charge_mrr_daily_with_namespace_and_installation` model which expands the subscription-product across the effective dates of the associated charge.

{% enddocs %}