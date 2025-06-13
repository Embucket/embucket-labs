{% docs gitlab_dotcom_dedupe_docs %}

Dedupe model is used to create a copy of the Gitlab Dotcom postgres db without any transformation. The model deduplicates rows stored in the `RAW` database based on Primary Key (usually, it is `id` column) and a timestamp (`updated_at` if existing, or `created_at`)

{% enddocs %}
