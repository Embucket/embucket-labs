{% docs user_group %}

Type of user based on the ultimate parent namespace's plan on the last event of the month (total, free, paid). Trial namespaces are considered to be free

{% enddocs %}

{% docs event_name_array %}

Array containing the event_names included in the xMAU metric

{% enddocs %}

{% docs first_major_minor_version_id_with_counter %}

The first (minimum/earliest) major_minor_version_id that sent a ping containing the metric

{% enddocs %}

{% docs first_major_minor_version_with_counter %}

The first (minimum/earliest) major_minor_version that sent a ping containing the metric, easily joined to `dim_gitlab_releases`

{% enddocs %}

{% docs first_major_version_with_counter %}

The first (minimum/earliest) major_version that sent a ping containing the metric

{% enddocs %}

{% docs first_minor_version_with_counter %}

The first (minimum/earliest) minor_version that sent a ping containing the metric

{% enddocs %}

{% docs last_major_minor_version_id_with_counter %}

The last (maximum/most recent) major_minor_version_id that sent a ping containing the metric

{% enddocs %}

{% docs last_major_minor_version_with_counter %}

The last (maximum/most recent) major_minor_version that sent a ping containing the metric, easily joined to `dim_gitlab_releases`

{% enddocs %}

{% docs last_major_version_with_counter %}

The last (maximum/most recent) major_version that sent a ping containing the metric

{% enddocs %}

{% docs last_minor_version_with_counter %}

The last (maximum/most recent) minor_version that sent a ping containing the metric

{% enddocs %}

{% docs dim_installation_count %}

A count of installations (`COUNT(DISTINCT dim_installation_id)`) that ever sent a ping containing the metric

{% enddocs %}

{% docs licensed_user_count_rpt_model %}

Count of licensed users (seats) associated with the subscription, calculated using `fct_charge`

{% enddocs %}

{% docs ping_count %}

The count of pings sent by the installation that month

{% enddocs %}

{% docs has_sent_pings %}

Boolean flag set to True if installation sent at least 1 ping that month

{% enddocs %}

{% docs is_missing_charge_subscription %}

Boolean flag set to True if a ping associated with the subscription was received during the month, but there is not a corresponding record in `fct_charge`. This can be True in cases of delayed renewals, etc.

{% enddocs %}

{% docs reporting_count_estimation_model %}

The number of subscriptions or seats that meet the condition described in the estimation_grain. This is either a subscription reporting a metric or a subscription sending a ping from a version with that metric.

Examples: 
- If `estimation_grain = 'metric/version check - subscription based estimation'`, then this is a count of subscriptions that sent a ping from a version with the metric instrumented
- If `estimation_grain = 'reported metric - seat based estimation'`, then this is a count of seats that reported the metric

{% enddocs %}

{% docs not_reporting_count_estimation_model %}

The number of subscriptions or seats that meet the condition described in the estimation_grain. This is either a subscription not sending a ping that month, not sending that particular metric, or sending a ping from a version without that metric (ex. an older version). This is defined as `total_count - reporting_count`

Examples: 
- If `estimation_grain = 'metric/version check - subscription based estimation'`, then this is a count of subscriptions that sent a ping from a version with the metric instrumented (they could be opted out of sending Service Ping or on an older version)
- If `estimation_grain = 'reported metric - seat based estimation'`, then this is a count of seats that did not report the metric (they could be opted out of sending Service Ping, on an older version, or opted out of sending optional metrics)

{% enddocs %}

{% docs total_count_estimation_model %}

The total number of active subscriptions or seats that month, based on the estimation_grain.

Examples:
- If `estimation_grain = 'metric/version check - subscription based estimation'`, then this is a count of active subscriptions that month
- If `estimation_grain = 'reported metric - seat based estimation'`, then this is a count of seats associated with active subscriptions that month

{% enddocs %}

{% docs percent_reporting_estimation_model %}

The percent of total subscriptions or seats that meet the condition described in the estimation_grain. This is calculated using the [`pct_w_counters` macro](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.pct_w_counters) and is defined as `reporting_count / (reporting_count + not_reporting_count)`

{% enddocs %}

{% docs estimation_grain %}

The estimation methodology being used. **The "official" methodology used for xMAU/PI reporting is "metric/version check - subscription based estimation".** This value provides the context to understand `total_count`, `reporting_count`, `not_reporting_count`, and `percent_reporting`. 

estimation_grain is made up of two components: a condition and a measure (`condition - measure`). There are two conditions, `metric_version_check` (whether a subscription sent a ping from a version of GitLab with the metric instrumented) and `reported_metric` (whether a subscription reported the metric), and two measures `subscription based` (count of subscriptions) and `seat based` (count of seats).

Examples:
- `estimation_grain = 'metric/version check - subscription based estimation'`: This is a methodology based on the count of subscriptions that sent a ping from on a version with the metric instrumented
- `estimation_grain = 'reported metric - seat based estimation'`: This methodology is based on the count of seats that reported the metric

{% enddocs %}

{% docs total_usage_with_estimate %}

Total usage, including the estimate. This is calculated using the [`usage_estimation` macro](https://dbt.gitlabdata.com/#!/macro/macro.gitlab_snowflake.usage_estimation).

{% enddocs %}

{% docs estimated_usage %}

Estimated usage based on recorded usage and estimation grain/methodology. This is defined as `total_usage_with_estimate - recorded_usage`.

{% enddocs %}

{% docs recorded_usage_estimation_model %}

Actual recorded usage

{% enddocs %}

{% docs total_subscription_count_on_versions %}

Count of subscriptions sending a ping from a version of GitLab with the metric instrumented

{% enddocs %}

{% docs total_licensed_users_on_versions %}

Count of licensed users (seats) associated with subscriptions sending a ping from a version of GitLab with the metric instrumented

{% enddocs %}

{% docs zoekt_rollout_categories %}

The zoekt_rollout_categories column categorizes namespaces based on their indexing and search capabilities within the GitLab platform. It reflects the rollout status of the "Zoekt" search feature.

**The values in the zoekt_rollout_categories column are determined based on specific criteria related to the indexing and search status of namespaces:**
- `not indexed`: Indicates namespaces that are not yet indexed for search purposes.
- `indexed` - search disabled': Represents namespaces that are indexed but have search functionality disabled.
- `indexed` - search enabled': Denotes namespaces that are indexed and have search functionality enabled.

{% enddocs %}