{% docs attribution_id %}
A unique ID for the attribution comprises of the query id and the warehouse_metering_start_at.
{% enddocs %}

{% docs query_id %}
The snowflake unique id for the query.
{% enddocs %}

{% docs query_start_at %}
The timestamp for when the query started.
{% enddocs %}

{% docs query_execution_start_at %}
The timestamp for when the execution of the query started. It is derived from the `query_end_at` and `execution_time` fields.
{% enddocs %}

{% docs query_end_at %}
The timestamp for when the query ended.
{% enddocs %}

{% docs execution_time %}
Execution time (in milliseconds)
{% enddocs %}

{% docs total_elapsed_time %}
Elapsed time (in milliseconds).
{% enddocs %}

{% docs start_during_end_after %}
A flag to indicate if a query started during a spend window and ended after the spend window ended.  Used to calculate the `query_spend_duration`.
{% enddocs %}

{% docs start_before_end_after %}
A flag to indicate if a query started before a spend window and ended after the spend window ended.  Used to calculate the `query_spend_duration`.
{% enddocs %}

{% docs start_before_end_during %}
A flag to indicate if a query started before a spend window and ended during the spend window ended.  Used to calculate the `query_spend_duration`.
{% enddocs %}

{% docs start_during_end_during %}
A flag to indicate if a query started during a spend window and ended during the spend window ended.  Used to calculate the `query_spend_duration`.
{% enddocs %}

{% docs query_spend_duration %}
The duration of the query that happened in the spend window.
{% enddocs %}

{% docs total_query_duration %}
The summation of all of the `query_spend_duration` within the spend window.  Used to calculate the `query_spend_fraction`.
{% enddocs %}

{% docs query_spend_fraction %}
The fraction of the sum of all execution times for all all queries that happened during the spend window. 
{% enddocs %}

{% docs attributed_query_credits %}
The amount of `credits_used_total` attributed to the query for the spend window.  Calculated using `query_spend_fraction`.
{% enddocs %}