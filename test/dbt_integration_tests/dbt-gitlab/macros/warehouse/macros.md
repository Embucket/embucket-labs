{% docs alter_warehouse %}
This macro turns on or off a Snowflake warehouse.
{% enddocs %}


{% docs backup_to_gcs %}
This macro fetches all relevant tables in the specified database and schema for backing up into GCS. This macro should NOT be used outside of a `dbt run-operation` command.
{% enddocs %}


{% docs get_backup_table_command %}
This macro is called by `backup_to_gcs` so that the actual `copy into` command can be generated. This macro should NOT be referenced outside of the `backup_to_gcs` macro.
{% enddocs %}


{% docs grant_usage_to_schemas %}
This macro...
{% enddocs %}


{% docs gdpr_delete %}
This macro is intended to be run as a dbt run-operation. The command to do this at the command line is:

`dbt run-operation gdpr_delete --args '{email_sha: your_sha_here, run_queries: True}'`

The sha can be generated separately on the command line as well by doing the following:

***Make sure the email is in lowercase to match the `LOWER()` function used in the deletion operation.***

`echo -n email@redacted.com | shasum -a 256`

The macro gathers all of the columns within the RAW database that match `email` anywhere in the name and delete the rows if they're not in the `snapshots` schema. If the columns are in the snapshot schema it will first update the columns that don't match `email` and also don't match `id`. It will then update all of the email columns to the sha256 value as well.

The output from the run operation can be stored in a file by appending `>> file.txt` on the command line:

`dbt run-operation gdpr_delete --args '{email_sha: your_sha_here}' >> file.txt`

The output can also be split between standard out and a file by using the `tee` command. If you're running this command multiple times in a row, adding `--partial-parse` to the operation will help it run faster so everything won't have to compile each time. A complete example command:

`dbt --partial-parse run-operation gdpr_delete --args '{email_sha: your_sha_here, run_queries: True}' | tee file.txt`

Output will look like this:

DELETE FROM db.schema.table WHERE SHA2(TRIM(LOWER("COLUMNAME"))) =  'your_sha_here';
            
| number of rows de... |
| -------------------- |
|                    0 |
{% enddocs %}

{% docs gdpr_delete_gitlab_dotcom %}
This macro is intended to be run as a dbt run-operation and only applies on GitLab.com data sources. The command to do this at the command line is:

`dbt run-operation gdpr_delete_gitlab_dotcom --args '{email_sha: your_sha_here, run_queries: True}'`

The sha can be generated separately on the command line as well by doing the following:

`echo -n email@redacted.com | shasum -a 256`

The macro gathers all of the columns within the RAW database that match `email` anywhere in the name and delete the rows if they're not in the `snapshots` schema. If the columns are in the snapshot schema it will first update the columns that don't match `email` and also don't match `id`. It will then update all of the email columns to the sha256 value as well.

The output from the run operation can be stored in a file by appending `>> file.txt` on the command line:

`dbt run-operation gdpr_delete_gitlab_dotcom --args '{email_sha: your_sha_here}' >> file.txt`

The output can also be split between standard out and a file by using the `tee` command. If you're running this command multiple times in a row, adding `--partial-parse` to the operation will help it run faster so everything won't have to compile each time. A complete example command:

`dbt --partial-parse run-operation gdpr_delete_gitlab_dotcom --args '{email_sha: your_sha_here, run_queries: True}' | tee file.txt`

Output will look like this:

DELETE FROM db.schema.table WHERE SHA2(TRIM(LOWER("COLUMNAME"))) =  'your_sha_here';
            
| number of rows de... |
| -------------------- |
|                    0 |
{% enddocs %}
{% docs gdpr_bulk_delete %}
This macro performs bulk GDPR deletions. 

The input for this macro is a Driveload file containing the e-mail address and deletion type, 
The macro then runs either the gdpr_delete_gitlab_dotcom or gdpr_delete macro depending on the deletion type. 

After running processing an e-mail from the driveload file the input e-mail is then removed from the driveload file to ensure continued GDPR compliance.

{% enddocs %}