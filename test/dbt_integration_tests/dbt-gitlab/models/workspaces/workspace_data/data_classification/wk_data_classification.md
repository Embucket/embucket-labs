{% docs wk_sensitive_queries_source_doc %}

Source for sensitive queries for:
* `PII`
* `MNPI`

data. Combining tags with views:

1. [QUERY_HISTORY](https://docs.snowflake.com/en/sql-reference/functions/query_history) 
1. [ACCESS_HISTORY](https://docs.snowflake.com/en/sql-reference/account-usage/access_history)

to get the data can be treated as not allowed or suspicious action. 

More details about the technical implementation can be found in the [**/analytics**](https://gitlab.com/gitlab-data/analytics/-/blob/master/extract/data_classification/README.md) repo.

{% enddocs %}

{% docs wk_sensitive_queries_details_doc %}

Details about the tagging.

{% enddocs %}

{% docs wk_classification_type_doc %}

This column can have values:
- `PII` - Personally identifiable information (PII) is any type of data that can be used to identify someone, from their name and address to their phone number, passport information, and Social Security numbers. This information is frequently a target for identity thieves, especially over the internet.
- `MNPI` - Material Nonpublic Information (MNPI) is information that has not been made available to the public in any form, and is not available through proper channels of inquiry.

{% enddocs %}

{% docs wk_query_id_doc %}

The ID of a specified query in the current session and explain under which query SQL was executed.

{% enddocs %}

{% docs wk_start_time_doc %}

When query execution was started.

{% enddocs %}

{% docs wk_end_time_doc %}

When query execution was ended.

{% enddocs %}

{% docs wk_uploaded_at_doc %}

Moment when the record was inserted into table.
Represent in the TIMESTAMP (`YYY-MM-DD HH24:MI:SS`) format.

{% enddocs %}

{% docs wk_tag_name_doc %}

Name of the tag (PII, MNPI...).

{% enddocs %}

{% docs wk_tag_value_doc %}

Value of the tag (example: `MNPI=Yes`).

{% enddocs %}

{% docs wk_sensitive_queries_tags_doc %}

Set of `MNPI` and `PII` tags we want to track.

{% enddocs %}