{% docs gitlab_dotcom_dedupe_source_scd %}

Dedupe model SCD is used to create a copy of the 
Gitlab Dotcom postgres db without any transformation, but with keeping the latest state of data only.
Data is filtered per additional metadata from `Postgres` pipeline, in this case, it is column `_task_instance`.


{% enddocs %}
