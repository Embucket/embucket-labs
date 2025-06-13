{% docs gitlab_dotcom_groups %}

This is the base model for Gitlab.com groups. It is a subset of the namespaces table which includes both individual namespaces (when a user is created, a personal namespace is created) and groups (and subgroups) which is a collaborative namespace where several users can collaborate on specific projects.

{% enddocs %}


{% docs gitlab_dotcom_namespace_lineage %}

This model has one row for each namespace in the namespaces base model. This model adds extra information about all of the upstream parents associated with the namespace. This model can be thought of as an intermediate model in between the gitlab_dotcom_namespaces base model and the gitlab_dotcom_namespaces_xf transformed model. 

The `upstream_lineage` column is an array with the namespaces's entire geneology, ordered from young to old (self, parent, grandparent).  

Since groups can be nested up to 21 levels deep, this model provides an `ultimate_parent_id` column which does the work of finding the top-level namespace for each namespace.  This column is always the first item of `upstream_lineage`.  

This model also adds some helper columns related to the **plan** associated with each namespace and its parents. For both the namespace and the namespace's ultimate parent, there are columns to represent:
* the **current** plan_id (4)
* the **current** plan_title (gold)
* whether or not the **current** plan is paid (TRUE)

{% enddocs %}


{% docs gitlab_dotcom_gitlab_subscriptions_source %}

Base model for Gitlab.com gitlab_subscriptions. These are the plan subscriptions for the GitLab.com product, as opposed to the `subscriptions` table (no prefix) which deals with subscriptions in the notification sense, related to issues and merge requests.

Note: the primary_key in this model is `namespace_id`.

This table has some weird overriding mechanisms that need to be explained. Most of the times (but not always), the app overwrites data. For example, if a gitlab_subscription is started with a Gold Trial, if the trial expires and is not converted, the following fields are updated:

* `gitlab_subscription_start_date` (when the trial expires)
* `gitlab_subscription_end_date` is turned to NULL
* `plan_id` is turned to 34 (free plan)

Here is an example (namespace_id won't be shown because PII):

| GITLAB_SUBSCRIPTION_ID | GITLAB_SUBSCRIPTION_START_DATE | GITLAB_SUBSCRIPTION_END_DATE | GITLAB_SUBSCRIPTION_TRIAL_ENDS_ON | PLAN_ID | IS_TRIAL |
|------------------------|--------------------------------|------------------------------|-----------------------------------|---------|----------|
| 350446                 | 2019-11-15                     | NULL                         | 2019-11-15                        | 34      | FALSE    |
| 350446                 | 2019-10-16                     | 2019-11-15                   | 2019-11-15                        | 4       | TRUE     |

{% enddocs %}


{% docs scd_type_two_documentation %}
<br/>
### Type 2 Slowly Changing Dimension
This base model is modelled as a Type 2 Slowly Changing Dimension. This means 3 columns have been added as metadata to track row-level changes over time. These columns are `valid_from`, `valid_to` and `is_currently_valid`. One implication of this is that the primary key column in this table is *not* unique. There can be multiple rows per primary_key, but only a maximum of one will have `is_currently_vaild` set to TRUE. 

Read the documentation for the SCD Type 2 Macro [here](https://gitlab.com/gitlab-data/analytics/blob/master/transform/snowflake-dbt/macros/README.md#scd_type_2).

{% enddocs %}


{% docs gitlab_dotcom_events_source %}

Base model for Gitlab.com events. Events are documented [here](https://docs.gitlab.com/ee/api/events.html).
We do one transformation in this base model where we map an `action_type_id` to an `action_type` thanks to the macro `action_type`.

{% enddocs %}


{% docs visibility_documentation %}
This content will be masked for privacy in one of the following conditions:
 * If this is an issue, and the issue is set to `confidential`
 * If the namespace or project visibility level is set to "internal" (`visibility_level` = 10) or "private" (`visibility_level` = 0).
    * The visibility values can be validated by going to the [project navigation](https://gitlab.com/explore) and using the keyboard shortcut "pb" to show how the front-end queries for visibility.
 * Public projects are defined with a `visibility_level` of 20   
 * In all the above cases,  the content will *not* be masked if the namespace_id is in:
   * 6543: gitlab-com
   * 9970: gitlab-org
   * 4347861: gitlab-data  

{% enddocs %}

{% docs gitlab_dotcom_resource_label_events_source %}

Base model for Gitlab.com resource label events. Explanation [here](https://docs.gitlab.com/ee/api/resource_label_events.html). 

We map the `action_type_id` column to an `action_type` using the macro `resource_event_action_type`

{% enddocs %}
