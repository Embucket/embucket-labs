{% docs gitlab_dotcom_user_custom_attributes %}

This table contains all [Custom Attributes](https://docs.gitlab.com/ee/api/custom_attributes.html) associated with a User Namespace.

{% enddocs %}

{% docs gitlab_dotcom_environments_xf %}

This model anonymizes three fields: `environment_name`, `slug`, `external_url` based on the visibility of the projects the environments are associated to 

{% enddocs %}

{% docs gitlab_dotcom_gitlab_user_requests %}

This model enables product managers to surface which issue has been requested by potential prospects and current customers. The final model creates a table where each row is unique tuple of a `issue_id` and a `sfdc_account_id`.

It extends the models `gitlab_dotcom_notes_linked_to_sfdc_account_id` and `gitlab_dotcom_issues_linked_to_sfdc_account_id` by joining it to SFDC account metadata through the `account_id`. We add then the following metrics:

* `total_tcv`
* `count_licensed_users`

We also join the model `gitlab_dotcom_notes_linked_to_sfdc_account_id` and `gitlab_dotcom_issues_linked_to_sfdc_account_id` to `gitlab_dotcom_issues`, `gitlab_dotcom_projects` and `gitlab_dotcom_namespaces_xf` to add more metadata about issues, projects and namespaces.

{% enddocs %}



{% docs gitlab_dotcom_internal_notes_xf %}

This model is a subset of `gitlab_dotcom_notes` model which selects only notes coming from projects in Gitlab Namespaces.

It adds a few columns to the base `gitlab_dotcom_notes` model:

* `project_name`
* `namespace_id`
* `namespace_name`
* `namespace_type`

{% enddocs %}


{% docs gitlab_dotcom_labels_xf %}

Masks the label description based on privacy of the project that it is on.

A CTE will find projects that don't have visibility set to public and then joined to the labels in order to build a CASE statement to mask the content.

{% enddocs %}

{% docs gitlab_dotcom_groups_xf %}

This model includes all columns from the groups base model and adds the count of members and projects associated with the groups.
It also adds 2 columns based on subscription inheritance (as described [here](https://about.gitlab.com/handbook/marketing/product-marketing/enablement/dotcom-subscriptions/#common-misconceptions)):

* `groups_plan_is_paid`
* `groups_plan_id`

{% enddocs %}

{% docs gitlab_dotcom_memberships %}

This model unions together all of the other models that represent a user having (full or partial) access to a namespace, AKA "membership". 

There are 5 general ways that a user can have access to a group G:
* Be a **group member** of group G.
* Be a **group member** of G2, where G2 is a descendant (subgroup) of group G.
* Be a **project member** of P, where P is owned by G or one of G's descendants.
* Be a group member of X or a parent group of X, where X is invited to a project underneath G via [project group links](https://docs.gitlab.com/ee/user/group/#sharing-a-project-with-a-group).
* Be a group member of Y or a parent group of Y, where Y is invited to G or one of G's descendants via [group group links](https://docs.gitlab.com/ee/user/group/#sharing-a-group-with-another-group).

An example of these relationships is shown in this diagram:

<div style="width: 720px; height: 480px; margin: 10px; position: relative;"><iframe allowfullscreen frameborder="0" style="width:720px; height:480px" src="https://app.lucidchart.com/documents/embeddedchart/9f529269-3e32-4343-9713-8eb311df7258" id="WRFbB73aKeB3"></iframe></div>

Additionally, this model calculates the field `is_billable` - i.e. if a member should be counted toward the seat count for a subscription (note: this also applies to namespaces without a subscription for the convenience of determining seats in use). To determine the number of seats in use for a given namespace, a simple query such as the following will suffice: 

```
SELECT COUNT(DISTINCT user_id)
FROM legacy.gitlab_dotcom_memberships
WHERE is_billable = TRUE
  AND ultimate_parent_id = 123456
```  

{% enddocs %}

{% docs gitlab_dotcom_events_monthly_active_users%}

For each day, this model counts the number of active users from the previous 28 days. The definiton of an active user is completing one or more audit events within the timeframe. This model includes the referenced date as part of the 28-day window. So for example, the window on January 31th would be from the start of January 4th to the end of January 31 (inclusive).

This model includes one row for every day, but MAU for a given month will typically be reported as the MAU on the **last day of the month**.

{% enddocs %}


{% docs gitlab_dotcom_merge_request_assignment_events %}

This model contains the history of assignments, unassignments, and reassignments for merge requests within internal namespaces. From `gitlab_dotcom_internal_notes_xf`, notes of type `MergeRequest` are queried. Notes are stemmed down to referenced usernames, tokenized, and flattened so that for each event (assign, unassign, reassign) a row is created for each referenced username in the order it appears in the note. Finally, usernames are replaced with user id's by joining to `gitlab_dotcom_users`. Usernames that do not have an associated user_id (if the user was deleted or changed usernames) are not included in this model so as to not misattribute assignee changes.

{% enddocs %}


{% docs gitlab_dotcom_namespaces_xf %}

Includes all columns from the namespaces base model.  
The plan columns here (plan_id, plan_title, plan_is_paid) reference the plan that is inheritted from the namespace's ultimate parent.
Adds the count of members and projects associated with the namespace.  
Also adds boolean column `namespaces_plan_is_paid` to provide extra context.  

{% enddocs %}


{% docs gitlab_dotcom_projects_xf %}

Includes all columns from the projects base model.
Adds the count of members associated with the project.
Adds a boolean column, `namespaces_plan_is_paid`, to provide extra context.
Adds additional information about the associated namespace (name and path).

{% enddocs %}


{% docs gitlab_dotcom_saml_providers_xf %}

XF model that joins the base model `gitlab_dotcom_saml_providers` to the `gitlab_dotcom_identities` model in order to get additional metrics:

* number of users using a specific saml_provider for a specific group 
* first time a user of a specific group started using the saml_provider. This could be `NULL` if no one is using it.

{% enddocs %}

{% docs gitlab_dotcom_secure_stage_ci_jobs %}

This table is meant to isolate all ci_build jobs used to create the AMAU calculation for secure stage as described in [this handbook page](https://about.gitlab.com/handbook/product/metrics/#stage-monthly-active-users-smau).

This table is populated to try to catch customized setups. It leverages tables gitlab_dotcom_ci_job_artifacts, gitlab_dotcom_projects_xf, and gitlab_dotcom_ci_builds.

{% enddocs %}

{% docs gitlab_dotcom_users_xf%}
This model extends the base model `gitlab_dotcom_users` and adds several other dimensions

### Age cohorts
This model adds account age cohorts to the users table, the defined cohorts are:

1-  1 day or less  
2-  2 to 7 days  
3-  8 to 14 days  
4-  15 to 30 days  
5-  31 to 60 days  
6-  Over 60 days  

The CTE does this by comparing the time of the dbt run with `created_at` in the users table.

### Highest inherited subscription

This model documents the highest subscription a user inherits from. Rules around inheritance are a bit complicated, as stated in the handbook [here](https://about.gitlab.com/handbook/marketing/product-marketing/enablement/dotcom-subscriptions/#common-misconceptions),

>>>
Reality: GitLab.com subscriptions are scoped to a namespace, and individual users could participate in many groups with different subscription types. For example, they might have personal projects on a Free subscription type, participate in an open-source project that has Gold features (because it's public) while their company has a Silver subscription.
>>>

A user inherits from a subscription when:
* They are a member of a group/sub-group that has a paid subscription.
* They are a member of a project which belongs to a group with a paid subscription
* They have a personal subscription attached to their personal namespace.

Some gotchas:
* If a user is part of a public open-source (or edu) group/project, they will not inherit from the Gold subscription of the group/project.
* If a user is part of a project created by another user's personal namespace, they won't inherit from the owner's namespace subscription.

We then know for each user: what's the highest plan they inherit from and where they inherit it from.

If a user inherits from 2+ subscriptions with the same plan, we choose one subscription over the other based on the inheritance source: First, user, then groups, then projects.

### Subscription Portal (customers.gitlab.com) data 

This model surfaces also if a user has created an account or not in the subscription portal by joining with the `customers_db_customers` table. It also informs us if a specific user has already started a trial and if so when. 

### Misc

A `days_active` column is added by comparing `created_at` with `last_activity_on`

{% enddocs %}

{% docs gitlab_dotcom_users_blocked_xf%}
This model extends the base model `gitlab_dotcom_users` and adds several other dimensions as well a filter out active users

### Age cohorts
This model adds account age cohorts to the users table, the defined cohorts are:

1-  1 day or less  
2-  2 to 7 days  
3-  8 to 14 days  
4-  15 to 30 days  
5-  31 to 60 days  
6-  Over 60 days  

The CTE does this by comparing the time of the dbt run with `created_at` in the users table.

### Highest inherited subscription

This model documents the highest subscription a user inherits from. Rules around inheritance are a bit complicated, as stated in the handbook [here](https://about.gitlab.com/handbook/marketing/product-marketing/enablement/dotcom-subscriptions/#common-misconceptions),

>>>
Reality: GitLab.com subscriptions are scoped to a namespace, and individual users could participate in many groups with different subscription types. For example, they might have personal projects on a Free subscription type, participate in an open-source project that has Gold features (because it's public) while their company has a Silver subscription.
>>>

A user inherits from a subscription when:
* They are a member of a group/sub-group that has a paid subscription.
* They are a member of a project which belongs to a group with a paid subscription
* They have a personal subscription attached to their personal namespace.

Some gotchas:
* If a user is part of a public open-source (or edu) group/project, they will not inherit from the Gold subscription of the group/project.
* If a user is part of a project created by another user's personal namespace, they won't inherit from the owner's namespace subscription.

We then know for each user: what's the highest plan they inherit from and where they inherit it from.

If a user inherits from 2+ subscriptions with the same plan, we choose one subscription over the other based on the inheritance source: First, user, then groups, then projects.

### Subscription Portal (customers.gitlab.com) data 

This model surfaces also if a user has created an account or not in the subscription portal by joining with the `customers_db_customers` table. It also informs us if a specific user has already started a trial and if so when. 

### Misc

A `days_active` column is added by comparing `created_at` with `last_activity_on`

{% enddocs %}

{% docs xf_visibility_documentation %}

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

{% docs namespace_plan_id_at_creation %}

This column represents the gitlab_subscription plan_id (2, 3, 4, 34 or 'trial') of the namespace at the time that the object (issue, project, merge request) was created. 

{% enddocs %}
