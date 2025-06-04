{% docs mask_model %}
This macro is designed do be applied as a post-hook on a table or view model.  It will pull the policy information from identified columns and create and apply the masking policy.  This needs to be done after the table or view has been created as it calls for information about the table or view to know the data types of the columns to be masked.

{% enddocs %}

{% docs get_columns_to_mask %}
This macro is designed to collect the masking policies of a model or source table.  It can be used for a single table or for an entire resource type.


It can take 2 parameters:
* `resource_type`: the dbt resource type to get he masking policies for. Acceptable values are `source` and `model`
* `table`: Optional, the name of the dbt object to retrieve the policy information for.


Output:

* A list of dictionaries with the elements of a fully qualified column name and the masking policy for that column.

{% enddocs %}

{% docs apply_masking_policy %}
This macro is designed to collect the column data types of a specific table and use that information with passed masking policies to create and apply masking policies to all of the columns in a table.


It takes 4 parameters:
* `database`: The qualified database name of the table to apply masking policies to.
* `schema`: The qualified schema name of the table to apply masking policies to.
* `table`: The qualified table name of the table to apply masking policies to.
* `column_policies`: A list of dictionaries that must contain the `COLUMN_NAME` and `COLUMN_POLICY` keys each column to have a masking policy applied to it.


Output:

* Passed list of columns will have the passed masking policy created and applied to it.

{% enddocs %}

{% docs get_mask %}
This macro is designed to return the text of the policy and data type specific mask for a masking policy. Additional custom masking [examples](https://docs.snowflake.com/en/user-guide/security-column-ddm-use.html#additional-masking-policy-examples).


It can take 2 parameters:
* `data_type`: The Snowflake data type of the column to be masked.
* `policy`: Optional, The name of the policy with specific masking requirements.



Output:

* The text of the mask to be applied for a specific policy and data type.

{% enddocs %}

{% docs create_masking_policy %}
This macro is designed to create or replace a database, schema, policy, and data type specific masking policy.


It takes 4 parameters:
* `database`: The qualified database name for the masking policy.
* `schema`: The qualified schema name for the masking policy.
* `data_type`: The Snowflake data type for the masking policy.
* `policy`: The name of the policy. Must match a Data Masking Role



Output:

* A masking policy in the specified database and schema with a name specific to the Data Masking Role and data type

{% enddocs %}

{% docs set_masking_policy %}
This macro is designed to set the masking policy of a column


It takes 7 parameters:
* `database`: The qualified database name for the masking policy and table to apply the policy to.
* `schema`: The qualified schema name for the masking policy and table to apply the policy to.
* `table`: The qualified table name for the masking policy and table to apply the policy to.
* `table_type`: The Snowflake table type, `view` or `table`, for the policy to be applied to.
* `column_name`: The qualified column name for the masking policy to apply the policy to.
* `data_type`: The Snowflake data type for the masking policy and the column to apply the policy to.
* `policy`: The name of the policy. Must match a Data Masking Role

Output:

* The specific masking policy applied to the specific column

{% enddocs %}

{% docs create_latest_version_view %}

This macro creates or replaces a view for the latest version of a model, ensuring it always points to the most current data.

- Latest Version Check: Only operates if the model is versioned and set as the latest; otherwise, it performs no action (no-op).
- View Creation: If the model is the latest, it creates or replaces a view for that model. It will drop any existing relation that isn’t a view to avoid conflicts.

{% enddocs %}
