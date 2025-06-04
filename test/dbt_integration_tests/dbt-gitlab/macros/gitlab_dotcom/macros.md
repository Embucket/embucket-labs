{% docs action_type %}
This macro maps action type ID to the action type.
{% enddocs %}

{% docs filter_out_blocked_users %}
This macro takes in the name of the table and column that contain GitLab user ids. This macro creates the SQL filter for filtering out users blocked by GitLab.

The SQL filter returned does not include a `WHERE`, `AND`, or `OR` so it can flexibly be used as any part of the `WHERE` clause.  For example, to filter out blocked users from a table named `users` with a column named `user_id`, the dbt model would look like

```
{% raw %}
SELECT *
FROM users
WHERE {{ filter_out_blocked_users('users', 'user_id') }}
{% endraw %}
```

This macro should be used downstream of source models, in models where activities of blocked users may introduce noise to metrics.  For example, this macro is used in `pump_marketing_contact_namespace_detail` to only keep relevant users.

{% enddocs %}

{% docs filter_out_active_users %}
This macro takes in the name of the table and column that contain GitLab user ids. This macro creates the SQL filter for filtering out users with an active account status on GitLab.

The SQL filter returned does not include a `WHERE`, `AND`, or `OR` so it can flexibly be used as any part of the `WHERE` clause.  For example, to filter out active users from a table named `users` with a column named `user_id`, the dbt model would look like

```
{% raw %}
SELECT *
FROM users
WHERE {{ filter_out_active_users('users', 'user_id') }}
{% endraw %}
```

This macro should be used downstream of source models, in models where we are isolating the activities of blocked or deactivated users from active users.

{% enddocs %}

{% docs get_internal_parent_namespaces %}
Returns a list of all the internal GitLab parent namespaces, enclosed in round brackets. This is useful for filtering an analysis down to external users only.

{% enddocs %}

{% docs map_state_id %}
This macro maps state_ids to english state names (opened, closed, etc).
{% enddocs %}


{% docs resource_event_action_type %}
This macro maps action type ID to the action type for the `resource_label_events` table.
{% enddocs %}


{% docs user_role_mapping %}
This macro maps "role" values (integers) from the user table into their respective string values.

For example, user_role=0 maps to the 'Software Developer' role.

{% enddocs %}

{% docs dedupe_source %}
This macro speed up data deduplication for the `PREP.GITLAB_DOTCOM` layer. This is a follow-up on `RAW.TAP_POSTGRES` schema data.
For instance, calling the macro with the command:

{% raw %}
```sql
{{ dedupe_source(source_table='workspaces') }}
```
{% endraw %}

will be translated as:
```sql
SELECT *

FROM RAW.tap_postgres.gitlab_db_workspaces

QUALIFY ROW_NUMBER() OVER (PARTITION BY id ORDER BY updated_at DESC) = 1
```
and this is handy as the files for data deduplication as smaller and unified.

{% enddocs %}

{% docs visibility_level_name %}

Maps visibility level ids to their corresponding names (Public, Internal, Private).

{% enddocs %}

{% docs user_type_mapping %}

Maps user type ids to their corresponding names based on the enum mapping in this [source](https://gitlab.com/gitlab-org/gitlab/-/blob/master/app/models/concerns/has_user_type.rb).

{% enddocs %}