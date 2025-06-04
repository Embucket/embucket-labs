{% docs mart_event_valid %}

**Description:** Enriched GitLab.com usage event data for valid events. This is an enhanced version of `fct_event_valid`, filtered to the last 24 months
- [Targets and Actions](https://docs.gitlab.com/ee/api/events.html) activity by Users and [Namespaces](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/namespace/) within the GitLab.com application are captured and refreshed periodically throughout the day.  Targets are objects ie. issue, milestone, merge_request and Actions have effect on Targets, ie. approved, closed, commented, created, etc.
- This data is enriched with additional user, namespace, and project attributes for ease of analysis

**Data Grain:**
- event_pk

**Filters Applied to Model:**
- `Inherited` - Include valid events for standard analysis and reporting:
  - Exclude events where the event created date < the user created date (`days_since_user_creation_at_event_date >= 0`)
    - These are usually events from projects that were created before the GitLab.com user and then imported after the user is created 
  - Exclude events from blocked users (based on the current user state)
- Rolling 24 months of data

**Business Logic in this Model:**
- `Inherited` - A namespace's plan information (ex: `plan_name_at_event_date`) is determined by the plan for the last event on a given day
- `Inherited` - The ultimate parent namespace's subscription, billing, and account information (ex: `dim_latest_subscription_id`) reflects the most recent available attributes associated with that namespace
- `Inherited` - `dim_latest_product_tier_id` reflects the _current_ product tier of the namespace
- `Inherited` - Not all events have a user associated with them (ex: 'milestones'), and not all events have a namespace associated with them (ex: 'users_created'). Therefore it is expected that `dim_user_sk` or `dim_ultimate_parent_namespace_id` will be NULL for these events
- `Inherited` - `section_name`, `stage_name`, `group_name`, and xMAU metric flags (ex: `is_gmau`) are based on the _current_ event mappings and may not match the mapping at the time of the event

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_event_namespace_daily %}

**Description:** Enriched GitLab.com usage event data for valid events, grouped by date, event name, and ultimate parent namespace. This is an enhanced version of `fct_event_namespace_daily`
- This data is enhanced with additional namespace attributes for ease of analysis

**Data Grain:**
- event_date
- event_name
- dim_ultimate_parent_namespace_id

**Filters Applied to Model:**
- `Inherited` - Include valid events for standard analysis and reporting:
  - Exclude events where the event created date < the user created date (`days_since_user_creation_at_event_date >= 0`)
    - These are usually events from projects that were created before the GitLab.com user and then imported after the user is created 
  - Exclude events from blocked users (based on the current user state)
- `Inherited` - Rolling 24 months of data
- `Inherited` - Exclude events not associated with a namespace (ex: 'users_created')

**Business Logic in this Model:**
- `Inherited` - A namespace's plan information (ex: `plan_name_at_event_date`) is determined by the plan for the last event on a given day
- `Inherited` - The ultimate parent namespace's subscription, billing, and account information (ex: `dim_latest_subscription_id`) reflects the most recent available attributes associated with that namespace
- `Inherited` - `dim_latest_product_tier_id` reflects the _current_ product tier of the namespace
- `Inherited` - `section_name`, `stage_name`, `group_name`, and xMAU metric flags (ex: `is_gmau`) are based on the _current_ event mappings and may not match the mapping at the time of the event

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_event_user_daily %}

**Description:** Enriched GitLab.com usage event data for valid events, grouped by date, user, ultimate parent namespace, and event name. This is an enhanced version of `fct_event_user_daily`
- This data is enhanced with additional user and namespace attributes for ease of analysis

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
- `Inherited` - Rolling 24 months of data
- `Inherited` - Exclude events not associated with a user (ex: 'milestones')

**Business Logic in this Model:**
- `Inherited` - A namespace's plan information (ex: `plan_name_at_event_date`) is determined by the plan for the last event on a given day
- `Inherited` - The ultimate parent namespace's subscription, billing, and account information (ex: `dim_latest_subscription_id`) reflects the most recent available attributes associated with that namespace
- `Inherited` - `dim_latest_product_tier_id` reflects the _current_ product tier of the namespace
- `Inherited` - `section_name`, `stage_name`, `group_name`, and xMAU metric flags (ex: `is_gmau`) are based on the _current_ event mappings and may not match the mapping at the time of the event

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_ping_instance_metric %}

**Description:** Enriched instance Service Ping data by ping and metric for all-time metrics. This is a UNIONED version of [`mart_ping_instance_metric_7_day`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.mart_ping_instance_metric_7_day), [`mart_ping_instance_metric_28_day`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.mart_ping_instance_metric_28_day), and [`mart_ping_instance_metric_all_time`](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.mart_ping_instance_metric_all_time)
- This data is enhanced with additional license, subscription, CRM account, and billing attributes for ease of analysis

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- `Inherited` - Exclude non-production SaaS installations (ex: `staging.gitlab.com`)
- `Inherited` - Exclude metrics with non-numeric or negative values (`TRY_TO_DECIMAL(metric_value::TEXT) >= 0`)
- `Inherited` - Include 7-day, 28-day, and all-time metrics (`time_frame IN ('7d', '28d', 'all')`)

**Business Logic in this Model:**
- `Inherited` - License / Subscription Logic:
  - `latest_subscription_id` reflects the most recent available subscription_id `WHERE subscription_status IN ('Active','Cancelled')`. This is not necessarily the subscription_id at the time of ping generation
  - `is_program_subscription` = TRUE `WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')`
  - `product_delivery_type = 'Self-Managed'`
  - `product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')`
  - `charge_type = 'Recurring'`
- `Inherited` - The installation's subscription information reflects the plan at time of ping generation
  - The exception is `latest_subscription_id` which reflects the most recent available subscription_id associated with the installation's subscription at time of ping generation
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `is_last_ping_of_month` = last ping created per calendar month per installation (`dim_installation_id`)

**Other Comments:**
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- `dim_instance_id` is synonymous with `uuid` in the source data
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- Sums, Counts and Percents of Usage (called metrics) is captured along with the Implementation Information at the Instance Level and sent to GitLab. The Instance Owner determines whether Service Ping data will be sent or not.
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).   
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_7_day %}

**Description:** Enriched instance Service Ping data by ping and metric for 7-day metrics. This is an enhanced version of `fct_ping_instance_metric_7_day` and is defined using the [`macro_mart_ping_instance_metric`](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.macro_mart_ping_instance_metric) macro. 
- This data is enhanced with additional license, subscription, CRM account, and billing attributes for ease of analysis

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- Exclude non-production SaaS installations (ex: `staging.gitlab.com`)
- Exclude metrics with non-numeric or negative values (`TRY_TO_DECIMAL(metric_value::TEXT) >= 0`)
- `Inherited` - Include 7-day metrics (`time_frame = '7d'`)

**Business Logic in this Model:**
- License / Subscription Logic:
  - `latest_subscription_id` reflects the most recent available subscription_id `WHERE subscription_status IN ('Active','Cancelled')`. This is not necessarily the subscription_id at the time of ping generation
  - `is_program_subscription` = TRUE `WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')`
  - `product_delivery_type = 'Self-Managed'`
  - `product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')`
  - `charge_type = 'Recurring'`
- The installation's subscription information reflects the plan at time of ping generation
  - The exception is `latest_subscription_id` which reflects the most recent available subscription_id associated with the installation's subscription at time of ping generation
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `is_last_ping_of_month` = last ping created per calendar month per installation (`dim_installation_id`)

**Other Comments:**
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- `dim_instance_id` is synonymous with `uuid` in the source data
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_28_day %}

**Description:** Enriched instance Service Ping data by ping and metric for 28-day metrics. This is an enhanced version of `fct_ping_instance_metric_28_day` and is defined using the [`macro_mart_ping_instance_metric`](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.macro_mart_ping_instance_metric) macro. 
- This data is enhanced with additional license, subscription, CRM account, and billing attributes for ease of analysis

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- Exclude non-production SaaS installations (ex: `staging.gitlab.com`)
- Exclude metrics with non-numeric or negative values (`TRY_TO_DECIMAL(metric_value::TEXT) >= 0`)
- `Inherited` - Include 28-day metrics (`time_frame = '28d'`)

**Business Logic in this Model:**
- License / Subscription Logic:
  - `latest_subscription_id` reflects the most recent available subscription_id `WHERE subscription_status IN ('Active','Cancelled')`. This is not necessarily the subscription_id at the time of ping generation
  - `is_program_subscription` = TRUE `WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')`
  - `product_delivery_type = 'Self-Managed'`
  - `product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')`
  - `charge_type = 'Recurring'`
- The installation's subscription information reflects the plan at time of ping generation
  - The exception is `latest_subscription_id` which reflects the most recent available subscription_id associated with the installation's subscription at time of ping generation
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `is_last_ping_of_month` = last ping created per calendar month per installation (`dim_installation_id`)

**Other Comments:**
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- `dim_instance_id` is synonymous with `uuid` in the source data
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_all_time %}

**Description:** Enriched instance Service Ping data by ping and metric for all-time metrics. This is an enhanced version of `fct_ping_instance_metric_all_time` and is defined using the [`macro_mart_ping_instance_metric`](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.macro_mart_ping_instance_metric) macro. 
- This data is enhanced with additional license, subscription, CRM account, and billing attributes for ease of analysis

**Data Grain:**
- dim_ping_instance_id
- metrics_path

**Filters Applied to Model:**
- Exclude non-production SaaS installations (ex: `staging.gitlab.com`)
- Exclude metrics with non-numeric or negative values (`TRY_TO_DECIMAL(metric_value::TEXT) >= 0`)
- `Inherited` - Include all-time metrics (`time_frame = 'all'`)

**Business Logic in this Model:**
- License / Subscription Logic:
  - `latest_subscription_id` reflects the most recent available subscription_id `WHERE subscription_status IN ('Active','Cancelled')`. This is not necessarily the subscription_id at the time of ping generation
  - `is_program_subscription` = TRUE `WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')`
  - `product_delivery_type = 'Self-Managed'`
  - `product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')`
  - `charge_type = 'Recurring'`
- The installation's subscription information reflects the plan at time of ping generation
  - The exception is `latest_subscription_id` which reflects the most recent available subscription_id associated with the installation's subscription at time of ping generation
- `Inherited` - Metrics that timed out (return -1) are set to a value of 0
- `Inherited` - `is_last_ping_of_month` = last ping created per calendar month per installation (`dim_installation_id`)

**Other Comments:**
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- `dim_instance_id` is synonymous with `uuid` in the source data
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance %}

**Description:** Enriched instance Service Ping data by ping. This is an enhanced version of `fct_ping_instance`. Metrics are not included in this data
- This data is enhanced with additional license, subscription, CRM account, and billing attributes for ease of analysis

**Data Grain:**
- dim_ping_instance_id

**Filters Applied to Model:**
- Exclude non-production SaaS installations (ex: `staging.gitlab.com`)

**Business Logic in this Model:**
- License / Subscription Logic:
  - `latest_subscription_id` reflects the most recent available subscription_id `WHERE subscription_status IN ('Active','Cancelled')`. This is not necessarily the subscription_id at the time of ping generation
  - `is_program_subscription` = TRUE `WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')`
  - `product_delivery_type = 'Self-Managed'`
  - `product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')`
  - `charge_type = 'Recurring'`
- The installation's subscription information reflects the plan at time of ping generation
  - The exception is `latest_subscription_id` which reflects the most recent available subscription_id associated with the installation's subscription at time of ping generation
- `Inherited` - `is_last_ping_of_month` = last ping created per calendar month per installation (`dim_installation_id`)

**Other Comments:**
- This model is built to have one record per Service Ping and therefore does not contain any metric-level data
- GitLab implementations can be Customer Hosted (Self-Managed), GitLab Hosted (referred to as SaaS or Dotcom data) or GitLab Dedicated Hosted (where each Installation is Hosted by GitLab but on Separate Servers).  
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_instance_id` is synonymous with `uuid` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_monthly %}

**Description:** Enriched instance Service Ping data for the last ping of the month per installation by ping and metric for 28-day and all-time metrics. This model is used for most monthly analysis and reporting. This is an enhanced version of `fct_ping_instance_metric_monthly`.
- This data is enhanced with additional license, subscription, CRM account, and billing attributes for ease of analysis

**Data Grain:**
- dim_installation_id
- metrics_path
- ping_created_date_month

**Filters Applied to Model:**
- Exclude non-production SaaS installations (ex: `staging.gitlab.com`)
- Exclude metrics with non-numeric or negative values (`TRY_TO_DECIMAL(monthly_metric_value::TEXT) >= 0`)
- `Inherited` - Exclude metrics that timed out during ping generation
- `Inherited` - Include 28-day and all-time metrics (`time_frame IN ('28d', 'all')`)
- `Inherited` - Include metrics from the 'Last Ping of the Month' pings
- Include only Self-Managed and Dedicated deployments, or the GitLab.com production installation (i.e., exclude any GitLab.com staging installations)

**Business Logic in this Model:**
- License / Subscription Logic:
  - `latest_subscription_id` reflects the most recent available subscription_id `WHERE subscription_status IN ('Active','Cancelled')`. This is not necessarily the subscription_id at the time of ping generation
  - `is_program_subscription` = TRUE `WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')`
  - `ping_deployment_type IN ('Self-Managed', 'Dedicated')`
  - `product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')`
  - `charge_type = 'Recurring'`
- The installation's subscription information reflects the plan at time of ping generation
  - The exception is `latest_subscription_id` which reflects the most recent available subscription_id associated with the installation's subscription at time of ping generation
- `Inherited` - `is_last_ping_of_month` = last ping created per calendar month per installation (`dim_installation_id`)

**Other Comments:**
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- `dim_instance_id` is synonymous with `uuid` in the source data
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_ping_instance_metric_weekly %}

**Description:** Enriched instance Service Ping data for the last ping of the month per installation by ping and metric for 7-day metrics. This is an enhanced version of `fct_ping_instance_metric_weekly` and is defined using the [`macro_mart_ping_instance_metric`](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.macro_mart_ping_instance_metric) macro. 
- This data is enhanced with additional license, subscription, CRM account, and billing attributes for ease of analysis

**Data Grain:**
- dim_installation_id
- metrics_path
- ping_created_date_week

**Filters Applied to Model:**
- Exclude non-production SaaS installations (ex: `staging.gitlab.com`)
- Exclude metrics with non-numeric or negative values (`TRY_TO_DECIMAL(metric_value::TEXT) >= 0`)
- `Inherited` - Exclude metrics that timed out during ping generation
- `Inherited` - Include 7-day metrics (`time_frame = '7d'`)
- `Inherited` - Include metrics from the 'Last Ping of the Week' pings

**Business Logic in this Model:**
- License / Subscription Logic:
  - `latest_subscription_id` reflects the most recent available subscription_id `WHERE subscription_status IN ('Active','Cancelled')`. This is not necessarily the subscription_id at the time of ping generation
  - `is_program_subscription` = TRUE `WHERE product_rate_plan_name LIKE ('%edu%' or '%oss%')`
  - `product_delivery_type = 'Self-Managed'`
  - `product_rate_plan_name NOT IN ('Premium - 1 Year - Eval')`
  - `charge_type = 'Recurring'`
- The installation's subscription information reflects the plan at time of ping generation. (The exception is `latest_subscription_id` which reflects the most recent available subscription_id associated with the installation's subscription at time of ping generation
- `Inherited` - `is_last_ping_of_week` = last ping created per calendar week per installation (`dim_installation_id`)

**Other Comments:**
- `dim_ping_instance_id` is the unique identifier for the service ping and is synonymous with `id` in the source data
- `dim_installation_id` is the unique identifier for the actual installation. It is a combination of `dim_instance_id` and `dim_host_id`. `dim_host_id` is required because there can be multiple installations that share the same `dim_instance_id` (ex: gitlab.com has several installations sharing the same dim_instance_id: gitlab.com, staging.gitlab.com, etc)
- `dim_instance_id` is synonymous with `uuid` in the source data
- Metric time frames are set in the metric definition yaml file and can be found in the [Service Ping Metrics Dictionary](https://metrics.gitlab.com/)
- The different types of Service Pings are shown here with the [Self-Managed Service Ping](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#self-managed-service-ping), [GitLab Hosted Implementation](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-service-ping-automation/#saas-service-ping).
- [GitLab Dedicated Implementation](https://docs.gitlab.com/ee/subscriptions/gitlab_dedicated/#gitlab-dedicated) service pings will function similar to Self-Managed Implementations.
- [Service Ping Guide](https://docs.gitlab.com/ee/development/service_ping/) shows a technical overview of the Service Ping data flow.

{% enddocs %}

{% docs mart_event_namespace_monthly %}

**Description:** Enriched GitLab.com usage event data for valid events, grouped by month, event name, and ultimate parent namespace. This is an enhanced version of `fct_event_namespace_monthly`
- This data is enhanced with additional namespace attributes for ease of analysis

**Data Grain:**
- event_calendar_month
- event_name
- dim_ultimate_parent_namespace_id

**Filters Applied to Model:**
- Exclude current month
- `Inherited` - Include valid events for standard analysis and reporting:
  - Exclude events where the event created date < the user created date (`days_since_user_creation_at_event_date >= 0`)
    - These are usually events from projects that were created before the GitLab.com user and then imported after the user is created 
  - Exclude events from blocked users (based on the current user state)
- `Inherited` - Rolling 36 months of data
- `Inherited` - Exclude events not associated with a namespace (ex: 'users_created')

**Business Logic in this Model:**
- `Inherited` - A namespace's plan information (ex: `plan_name_at_event_month`) is determined by the plan for the last event on a given month
- `Inherited` - The ultimate parent namespace's subscription, billing, and account information (ex: `dim_latest_subscription_id`) reflects the most recent available attributes associated with that namespace
- `Inherited` - `dim_latest_product_tier_id` reflects the _current_ product tier of the namespace
- `Inherited` - `section_name`, `stage_name`, `group_name`, and xMAU metric flags (ex: `is_gmau`) are based on the _current_ event mappings and may not match the mapping at the time of the event

**Other Comments:**
- Note about the `action` event: This "event" captures everything from the [Events API](https://docs.gitlab.com/ee/api/events.html) - issue comments, MRs created, etc. While the `action` event is mapped to the Manage stage, the events included actually span multiple stages (plan, create, etc), which is why this is used for UMAU. Be mindful of the impact of including `action` during stage adoption analysis.

{% enddocs %}

{% docs mart_behavior_structured_event %}

**Description:** Enriched Snowplow table for the analysis of structured events. This is an enhanced version of `fct_behavior_structured_event` containing only production events (staging events are excluded).

**Data Grain:** behavior_structured_event_pk

This ID is generated using `event_id` from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) 

**Filters Applied to Model:**
- `Inherited` - This model only includes Structured events (when `event=struct` from `dim_behavior_event`)
- Exclude staging events (`is_staging_event = FALSE`)

**Tips for use:**
- There is a cluster key on `behavior_at::DATE`. Using `behavior_at` in a WHERE clause or INNER JOIN will improve query performance.
- Join this model to `dim_behavior_website_page` using `dim_behavior_website_page_sk` in order to pull in information about the page URL
- Join this model to `dim_behavior_operating_system` using `dim_behavior_operating_system_sk` in order to pull in information about the user OS 
- Join this model to `dim_behavior_browser` using `dim_behavior_browser_sk` in  order to pull in information about the user browser 

**Other Comments:**
- Structured events are custom events implemented with five parameters: event_category, event_action, event_label, event_property and event_value. Snowplow documentation on [types of events](https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/).
- There is information on some Snowplow structured events in the [Snowplow event dictionary](https://metrics.gitlab.com/snowplow)

{% enddocs %}

{% docs mart_behavior_structured_event_code_suggestion %}

**Description:** Enriched Snowplow table for the analysis of Code Suggestions-related structured events. This model is limited to events carrying the `code_suggestions_context`, in addition to other filters (listed below). It enhances `fct_behavior_structured_event` and includes fields from the `code_suggestions_context` and `ide_extension_version` contexts.

**Data Grain:** behavior_structured_event_pk

This ID is generated using `event_id` from [prep_snowplow_unnested_events_all](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.prep_snowplow_unnested_events_all) 

**Filters Applied to Model:**
- Include events containing the `code_suggestions_context`
- Include events from the following app_ids: `gitlab_ai_gateway`, `gitlab_ide_extension`
- Exclude IDE events from VS Code extension version 3.76.0. These are excluded by using both `ide_name` and `extension_version` values.
  - Note: The Gateway did not send duplicate events from that extension version, so it is okay to let those flow through
- `Inherited` - This model only includes Structured events (when `event=struct` from `dim_behavior_event`)

**Tips for use:**
- There is a cluster key on `behavior_at::DATE`. Using `behavior_at` in a WHERE clause or INNER JOIN will improve query performance.
- All events will carry the `code_suggestions_context`, but only a subset will contain the `ide_extension_version` context
- `app_id = 'gitlab_ai_gateway'` 
  - These events originate from the AI gateway and cannot be blocked
  - There is only one event per suggestion (upon the request), which carries an `event_action` of `suggestions_requested` or `suggestion_requested`. Therefore these events can only be used to get a counts of users, etc, not acceptance rate.
  - These events do not carry the suggestion identifier in `event_label`
  - These events carry the `code_suggestions_context`, but not the `ide_extension_version` context
- `app_id = 'gitlab_ide_extension'`
  - These events originate from the IDEs, but can be blocked by the user (e.g. via disabling tracking)
  - There can be multiple events per suggestion, all with different `event_action` values (ex: `suggestion_requested`, `suggestion_loaded`, `suggestion_shown`, etc). Therefore these events can be used to calculate acceptance rate, etc.
  - These events carry a unique suggestion identifier in `event_label`. This can be joined across multiple events to calculate acceptance rate, etc.
  - These events carry both the `code_suggestions_context` and the `ide_extension_version` context

**Other Comments:**
- Schema for `code_suggestions_context` [here](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/code_suggestions_context)
- Schema for `ide_extension_version` context [here](https://gitlab.com/gitlab-org/iglu/-/tree/master/public/schemas/com.gitlab/ide_extension_version)
- A visual representation of the different Snowplow events coming from the IDE extension can be found [here](https://gitlab.com/gitlab-org/editor-extensions/gitlab-lsp/-/blob/main/docs/telemetry.md)
- Structured events are custom events implemented with five parameters: event_category, event_action, event_label, event_property and event_value. Snowplow documentation on [types of events](https://docs.snowplow.io/docs/understanding-tracking-design/out-of-the-box-vs-custom-events-and-entities/).
- There is information on some Snowplow structured events in the [Snowplow event dictionary](https://metrics.gitlab.com/snowplow)

{% enddocs %}