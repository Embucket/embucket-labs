{% set filter_date = (run_started_at - modules.datetime.timedelta(days=31)).strftime('%Y-%m-%d') %}
{% set temp_suffix = '_records_to_delete' %}
{% set tmp_table_statement %}
  CREATE TEMPORARY TABLE {% raw %}{{ this }}{% endraw %}{{ temp_suffix }} AS (
  SELECT ping_instance_metric_id
  FROM {% raw %}{{ this }}{% endraw %}
  WHERE ping_created_at > '{{ filter_date }}'
  QUALIFY ROW_NUMBER() OVER (PARTITION BY dim_installation_id, metrics_path, ping_created_date_week ORDER BY ping_created_at DESC) !=1)
{% endset %}
{% set delete_statement %}
  DELETE FROM {% raw %}{{ this }}{% endraw %} WHERE ping_instance_metric_id in (
  SELECT ping_instance_metric_id
  FROM {% raw %}{{ this }}{% endraw %}{{ temp_suffix }})
{% endset %}

{{ config(
    tags=["product", "mnpi_exception"],
    post_hook=[tmp_table_statement,delete_statement]
) }}

{{ macro_mart_ping_instance_metric('fct_ping_instance_metric_weekly') }}
