{% docs gitlab_dotcom_project_ci_cd_settings_snapshots_base %}

The source model `raw.tap_postgres.gitlab_db_project_ci_cd_settings` snapshots the state of each projects' CI CD settings twice per day. When the settings change, there is no `updated_at` date provided to indicate when the settings were adjusted.

In order to transform this data into a slowly-changing dimension, this model:

1. Creates a surrogate key based on the state of all the settings columns we want to track (`record_checksum`)
2. Finds the following information for each record:
    - `next_uploaded_at`: By id, the next time the id was uploaded
    - `lag_checksum`: The next settings loaded for the id
    - `checksum_group`: If the `record_checksum` is not the same as the next `record_checksum` (the `lag_checksum`), then assign it to a group number. This `checksum_group` is used to maintain the gaps and islands between groups of settings when a project returns to a previous state of settings. Without it, the SCD would show a continuous state for these settings from the first instance until the end of the last instance, with no break. With this `checksum_group`, we maintain the discrete time periods with the gaps between for other settings states.
3. Groups by the id, project_id, all columns in the `record_checksum`, and the `record_checksum` to find:
    - `uploaded_at`: The minumum `uploaded_at` for the group of settings
    - `valid_from`: The first time an `id` was in this settings state
    - `valid_to`: If the `next_uploaded_at` is null, then this will be null, otherwise it is the last time an `id` was in this settings state before it changed

On an incremental run, the model checks the following to determine which records to update:
1. In a CTE (`id_uploaded_date`), it find the distinct list of `project_ci_cd_settings_snapshot_id` and `valid_from` where `valid_to IS NULL`. These represent all of the records which could need to be updated because they are the latest version of the settings states.
2. This CTE is left joined to the `source` CTE (`RAW.raw.tap_postgres.gitlab_db_project_ci_cd_settings` where we create the `record_checksum`) based on the settings id and the uploaded_date/valid_from date. 
3. In an incremental filter on the `source` CTE, we filter for all newly uploaded records (`uploaded_at >= (SELECT MAX(uploaded_at) FROM {{ this }} )`) OR records in the `id_uploaded_date` CTE.

`
LEFT JOIN id_uploaded_date
    ON project_ci_cd_settings.id =  id_uploaded_date.project_ci_cd_settings_snapshot_id
      AND TO_TIMESTAMP(project_ci_cd_settings._uploaded_at::INT) = id_uploaded_date.valid_from
  WHERE uploaded_at >= (SELECT MAX(uploaded_at) FROM {{ this }} )
   OR id_uploaded_date.project_ci_cd_settings_snapshot_id IS NOT NULL
`

{% enddocs %}

{% docs gitlab_dotcom_pipl_users_snapshots_source %}

Snapshot model of `pipl_users` to track users being blocked for PIPL compliance reasons over time.

{% enddocs %}