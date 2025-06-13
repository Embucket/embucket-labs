{% docs wk_rpt_event_namespace_plan_monthly %}

**Description:**

This model captures the last plan available from mart_event_valid for a namespace during a 
given calendar month. This is the same logic used to attribute the namespace's usage for PI 
reporting (ex: [`rpt_event_xmau_metric_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_event_xmau_metric_monthly)).

**Data Grain:**
* event_calendar_month
* dim_ultimate_parent_namespace_id

**Intended Usage**

This model is intended to be JOINed to the fct_event lineage in order to determine how 
a namespace's usage will be attributed during monthly reporting. It can also be leveraged 
to track how many plans a namespace has during a calendar month.

**Filters & Business Logic in this Model:**

* This model inherits all filters and business logic from [`mart_event_valid`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.mart_event_valid#description).
* The current month is excluded.

**Important Nuance:**

This model looks at the entire calendar month where the "official" PI reporting models only 
look at the last 28 days of the month. Please apply the filter `WHERE has_event_during_reporting_period = TRUE` 
in order to have this model tie out to the other reporting models. 

Example: Namespace xyz has events on July 1-2, 2022, but nothing for the remainder of the month. 
Namespace xyz will have a record in this model where `has_event_during_reporting_period = FALSE` 
because it did not have any events during the last 28 days of the month.  Namespace xyz's usage 
will _not_ count in the other reporting models (ex: [`rpt_event_xmau_metric_monthly`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.rpt_event_xmau_metric_monthly)), 
so the `has_event_during_reporting_period` flag needs to be used in order for the models to tie out.

{% enddocs %}

{% docs wk_rpt_gitlab_registered_users_monthly %}

**Description:**

This model captures the count of total, paid, and free users by month, delivery type, and deployment type.

**Data Grain:**

* reporting_month
* delivery_type
* deployment_type

**Filters Applied to this Model:**

* This model contains data going back to `2022-01-01`.
* The current month is excluded.
* Only paid seats from GitLab base products (i.e., normal tiers and not add-ons) are included in 
the paid user count. Removing this filter could lead to double-counting of users.
  * The exception is [Enterprise Agile Planning seats](https://docs.gitlab.com/ee/subscriptions/gitlab_com/#enterprise-agile-planning) (those _are_ included) since they are 
  incremental seats (and a base product license is not required to use them).
* Seats are limited to subscriptions with a status of `Active` or `Cancelled`.
* `Inherited` - Non-production GitLab.com usage is excluded.

**Business Logic in this Model:**

* `total_user_count` is defined using `instance_user_count` (aka [`active_user_count`](https://gitlab.com/gitlab-org/gitlab/-/blob/master/config/metrics/license/20210204124829_active_user_count.yml)) 
from the last ping of the month per installation. 
  * In this case "active" is referring to a user's state (ex. not blocked) as opposed to an indication of user activity with the product
* `paid_user_count` is defined using the count of paid seats for GitLab base products (i.e., not add-ons) for the given month
  * There are edge cases where `paid_user_count` is greater than `total_user_count` (ex: more Dedicated licenses were sold than there were registered Dedicated users). In this case, we set `paid_user_count` to equal `total_user_count`.
* `free_user_count` is defined as `total_user_count - paid_user_count`

**Callouts:**

* Even though Dedicated is a paid-only product, there are only free users in January-March 2022. 
This is because there were not yet active paid subscriptions for Dedicated.
* There are edge cases where `paid_user_count` is greater than `total_user_count` (ex: more Dedicated licenses were sold than there were registered Dedicated users). In this case, we set `paid_user_count` to equal `total_user_count`.

{% enddocs %}

{% docs wk_ping_installation_latest %}

**Description:**

This model contains the installation-level attributes from the latest ping for each installation.

**Data Grain:**
* dim_installation_id

**Filters in this Model:**

* This model inherits all filters from [`mart_ping_instance`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.mart_ping_instance#description).

**Business Logic in this Model:**

* Determine the first ping date for each installation.
* Retrieve various attributes from the most recent ping for each installation.
* Resulting table is a JOIN of the above two informations where each row represents a unique installation, containing both first ping date and latest ping attributes.
* The `latest_` prefix on the columns indicates the status as of the most recent ping.

{% enddocs %}

{% docs wk_cloud_connector_configuration %}

**Description:** This model contains detailed information about cloud connector configurations within GitLab data ecosystem.
- Data is extracted and cleaned from [cloud_connector.yml file](https://gitlab.com/gitlab-org/customers-gitlab-com/-/blob/main/config/cloud_connector.yml) in the underlying model multiple times per day.
- [GitLab Cloud Connector](https://about.gitlab.com/direction/cloud-connector/) is a way to access services common to multiple GitLab deployments, instances, and cells.

**Data Grain:**
- environment_name
- backend
- feature_name
- bundled_with_add_on_name
- unit_primitive_name
- valid_from
- valid_to

**Filters Applied to Model:**
- None - `ALL Data` at the Atomic (`lowest level/grain`) is brought through from the source, providing a complete view of all cloud connector configurations.

**Business Logic in this Model:**
In order to transform this data into a slowly-changing dimension, this model:

1. Creates a surrogate key based on the state of all the Cloud Connector configuration columns we want to track (`record_checksum`)
2. Finds the following information for each record:
    - `next_uploaded_at`: By id, the next time the id was uploaded
    - `lag_checksum`: The next settings loaded for the id
    - `checksum_group`: If the `record_checksum` is not the same as the next `record_checksum` (the `lag_checksum`), then assign it to a group number. This `checksum_group` is used to maintain the gaps and islands between groups of settings when a project returns to a previous state of settings. Without it, the SCD would show a continuous state for these settings from the first instance until the end of the last instance, with no break. With this `checksum_group`, we maintain the discrete time periods with the gaps between for other settings states.
3. Groups by the cloud_connector_configuration_snapshot_id, all columns in the `record_checksum`, and the `record_checksum` to find:
    - `uploaded_at`: The minumum `uploaded_at` for the group of settings
    - `valid_from`: The first time an `cloud_connector_configuration_snapshot_id` was in this settings state
    - `valid_to`: If the `next_uploaded_at` is null, then this will be null, otherwise it is the last time an `cloud_connector_configuration_snapshot_id` was in this settings state before it changed

On an incremental run, the model checks the following to determine which records to update:
1. In a CTE (`id_uploaded_date`), it find the distinct list of `project_ci_cd_settings_snapshot_id` and `valid_from` where `valid_to IS NULL`. These represent all of the records which could need to be updated because they are the latest version of the settings states.
2. This CTE is left joined to the `source` CTE (`RAW.raw.tap_postgres.gitlab_db_project_ci_cd_settings` where we create the `record_checksum`) based on the settings id and the uploaded_date/valid_from date. 
3. In an incremental filter on the `source` CTE, we filter for all newly uploaded records (`uploaded_at >= (SELECT MAX(uploaded_at) FROM {{ this }} )`) OR records in the `id_uploaded_date` CTE.

**Other Comments:**
- This model is crucial for understanding the features that differentiate Duo Pro and Duo Enterprise.
- Consider using this model in conjunction with usage metrics to gain insights into the utilization of different cloud services.

{% enddocs %}