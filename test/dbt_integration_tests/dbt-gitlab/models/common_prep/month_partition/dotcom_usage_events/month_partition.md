{% docs prep_event %}

prep table to build the fct_dotcom_usage_events table. The grain of the table is one row per event.

The primary key of this table is the `event_id`.

This table is actually a long event list table with the following foreign keys:

- `dim_project_id`: in which project a specific event has been generated. For some events, there is no such data like boards which are at the group-level
- `ultimate_parent_namespace_id`: the top-level namespace in which the event has been generated. This is ALWAYS filled
- `dim_user_id`: the user who generated the event (for example the user who triggered a CI Pipeline)
- `dim_plan_id`: ID of the plan of the ultimate parent namespace when the event got created
- `dim_date_id`: ID of the date when the event got created.

Then other metadata available in the model are :

- Time metadata
  - user_created_at
  - namespace_created_at
  - days_since_namespace_creation
  - days_since_user_creation
  - days_since_project_creation
- Project specific metadata:
  - project_is_learn_gitlab
  - project_is_imported

A specific handbook page has been created for this table [here](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-product-events-data/). This has more information on how to add events and some analysis that can be run with this model.

When making changes to this model, you will have to use a specific DAG to backfill the partitions. Details [here](https://handbook.gitlab.com/handbook/enterprise-data/data-catalog/saas-product-events-data/#backfill-the-data).

{% enddocs %}
