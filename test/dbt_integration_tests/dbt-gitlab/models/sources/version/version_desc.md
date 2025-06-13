{% docs version_raw_usage_data_source %}
Data source for usage ping [page](https://docs.gitlab.com/ee/development/telemetry/usage_ping.html) contains full unprocessed usage ping payloads.
The explanation for `version_db` timestamp columns as it is vital to fully understand their meaning:
1. `recorded_at` its time when ServicePing was generated on the client side, we receive [usage_data.rb](https://gitlab.com/gitlab-org/gitlab/-/blob/master/lib/gitlab/usage_data.rb#L51-51) it with payload
2. `created_at` and `updated_at` are standard Rails datetime columns. In the case of table `usage_data` and `raw_usage_data` will always hold the same values, as we don't upsert record, always create new and reflect the timestamp when the payload was received.
{% enddocs %}

{% docs version_raw_usage_data_source_uploaded_at %}
Column `uploaded_at` (`TIMESTAMP` data type) represent the moment WHEN the record is ingested into Snowflake. 
The main motivation for introducing this column is for a few reasons:
1. Be able to track back the exact date and time of data ingesting _(this information wasn't known to us)_
1. Improving incremental load using `uploaded_at` column 
1. Support "late_arriving" ping automatically, without the need to full-refresh a full lineage

{% enddocs %}