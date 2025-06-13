The following tables are moved from incremental to full (SCD) load and moved under the `\scd` folder:

| Table (PostgresG) | Primary_key | dbt model |
| ------ | ------ |------|
| `path_locks` | `ID` | [gitlab_dotcom_path_locks_dedupe_source](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.gitlab_dotcom_path_locks_dedupe_source)|
| `lfs_file_locks` | `ID` | [gitlab_dotcom_lfs_file_locks_dedupe_source](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.gitlab_dotcom_lfs_file_locks_dedupe_source) |
| `bulk_import_entities` | `ID` | [gitlab_dotcom_bulk_import_entities_dedupe_source](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.gitlab_dotcom_bulk_import_entities_dedupe_source) |
| `clusters_integration_prometheus` | `CLUSTER_ID` | [gitlab_dotcom_clusters_integration_prometheus_dedupe_source](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.gitlab_dotcom_clusters_integration_prometheus_dedupe_source) |
| `group_import_states` | `GROUP_ID` | [gitlab_dotcom_group_import_states_dedupe_source](https://dbt.gitlabdata.com/#!/model/model.gitlab_snowflake.gitlab_dotcom_group_import_states_dedupe_source) |

Model name should be **not** changed as per `instance_sql_metrics` algorithm based on the `*_dedupe_source` table names.

For this purpose, `scd_latest_state` macro is used instead of the incremental load and expose the snapshot of data based on the `_task_instance` column.