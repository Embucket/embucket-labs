{% docs mart_ci_runner_activity_monthly %}

Mart table containing quantitative data related to CI runner activity on GitLab.com.

These metrics are aggregated at a monthly grain per `dim_namespace_id`.

Additional identifier/key fields - `dim_ci_runner_id`, `dim_ci_pipeline_id`, `dim_ci_stage_id` have been included for Reporting purposes.

Only activity since 2020-01-01 is being processed due to the high volume of the data.

{% enddocs %}

{% docs mart_product_usage_free_user_metrics_monthly %}
This table unions the sets of all Self-Managed and SaaS **free users**. The data from this table will be used for  Customer Product Insights by Sales team.

The grain of this table is namespace || uuid-hostname per month.

Information on the Enterprise Dimensional Model can be found in the [handbook](https://about.gitlab.com/handbook/business-ops/data-team/platform/edw/)

{% enddocs %}

{% docs mart_ci_runner_activity_daily %}

Mart table containing quantitative data related to CI runner activity on GitLab.com.

These metrics are aggregated at a daily grain per `dim_project_id`.

Additional identifier/key fields - `dim_ci_runner_id`, `dim_ci_pipeline_id`, `dim_ci_stage_id` have been included for Reporting purposes.

Only activity since 2020-01-01 is being processed due to the high volume of the data.

{% enddocs %}

{% docs mart_ping_instance_metric_health_score_self_managed %}

**Description:** Joins together facts and dimensions related to Self-Managed and SaaS Dedicated Service Pings, and does a simple aggregation to pivot out and standardize metric values. The data from this table will be used for customer product insights. Most notably, this data is pumped into Gainsight and aggregated into customer health scores for use by TAMs.

**Data Grain:**
- Service Ping Payload

**Filters:**
- Only includes Service Ping metrics that have been added via the "wave" process.
- Only includes pings that have a license associated with them.

{% enddocs %}

{% docs mart_ping_namespace_metric_health_score_saas %}

**Description:** Joins together facts and dimensions related to SaaS Namespace Service Pings, and does a simple aggregation to pivot out and standardize metric values. The data from this table will be used for customer product insights. Most notably, this data is pumped into Gainsight and aggregated into customer health scores for use by TAMs.

**Data Grain:**
- Namespace
- Subscription
- Month

**Filters:**
- Only includes Service Ping metrics that have been added via the "wave" process.
- Only includes pings that have a license associated with them.

**Business Logic in this Model:**
- Resolves a one-to-many relationship between namespaces and instance types by prioritizing production instances above other instance types
- Limits down to last ping of the month for each namespace-subscription
- Currently, bridges uses the order to bridge from subscription to namespace. In the future, we will use Zuora subscription tables to get the namespace directly from the subscription.

{% enddocs %}

{% docs rpt_namespace_onboarding %}

**Description:**

This model is aggregated at the ultimate parent namespaces level and contains a wide variety of namesapace onboarding behaviors and attributes. This model contains one row per ultimate_parent_namespace_id and is designed to meet most of the primary analytics use cases for the Growth team. 

**Data Grain:**
* ultimate_parent_namespace_id
* namespace_created_at

**Intended Usage**

This model is intended to be used as a reporting model for the Growth Section and any other teams at GitLab that are interested in ultimate parent namespace level onboarding behaviors and attributes.

**Filters & Business Logic in this Model:**

* This model filters out internal ultimate parent namespaces, ultimate parent namespaces whose creator is blocked, and is aggregated at the ultimate parent namespace level meaning that sub-groups and projects are not included in this model.

{% enddocs %}

{% docs mart_behavior_structured_event_service_ping_metrics %}

This model is for analysing SaaS product usage data at the namespace level. Snowplow events that contain the Service Ping Context are joined with the bridge table containing metric name, redis event name.
It is limited to events carrying the `service_ping_context`, in addition to other filters.

**Data Grain:** The combination of a Snowplow event containing a Service Ping event, and every Service Ping metric that Service Ping event joins to. (The relationship between Service Ping events and Service Ping metrics is many-to-many).

This ID is generated using `behavior_structured_event_pk` from [fct_behavior_structured_event_service_ping](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.fct_behavior_structured_event_service_ping) and `metrics_path` from [bdg_metrics_redis_events](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.bdg_metrics_redis_events).

**Filters Applied to Model:**
- Include events containing the `service_ping_context`
- `Inherited` - This model only includes Structured events (when `event=struct` from `dim_behavior_event`)

{% enddocs %}


{% docs mart_behavior_structured_event_service_ping_metrics_7d %}

Filtered model `mart_behavior_structured_event_service_ping_metrics` for last `7` days. Used for the `internal_events_metrics_calculation`.
Use filter:
```sql
  WHERE time_frame     = '7d'
    AND metrics_status = 'active'
    AND data_source    IN ('internal_events', 'redis', 'redis_hll')
```

{% enddocs %}

{% docs mart_behavior_structured_event_service_ping_metrics_28d %}

Filtered model `mart_behavior_structured_event_service_ping_metrics` for last `28` days. Used for the `internal_events_metrics_calculation`.
Use filter:
```sql
  WHERE time_frame     = '28d'
    AND metrics_status = 'active'
    AND data_source    IN ('internal_events', 'redis', 'redis_hll')
```

{% enddocs %}