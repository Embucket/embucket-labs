{% docs case_when_boolean_int %}
This macro returns a 1 if some value is greater than 0; otherwise, it returns a 0.
{% enddocs %}


{% docs coalesce_to_infinity %}
This macro expects a timestamp or date column as an input. If a non-null value is inputted, the same value is returned. If a null value is inputted, a large date representing 'infinity' is returned. This is useful for writing `BETWEEN` clauses using date columns that are sometimes NULL.
{% enddocs %}

{% docs convert_variant_to_boolean_field %}
This macro takes in either a variant or varchar field, converts it to a varchar field and then to a boolean field with lower case values. 
{% enddocs %}

{% docs create_snapshot_base %}
This macro creates a base model for dbt snapshots. A single entry is generated from the chosen start date through the current date for the specified primary key(s) and unit of time.
{% enddocs %}


{% docs current_date_schema %}
Returns the schema name based on the run start time. Returns `base_yyyy_mm`.
{% enddocs %}

{% docs dbt_audit %}
Used to append audit columns to a model.

This model assumes that the final statement in your model is a `SELECT *` from a CTE. The final SQL will still be a `SELECT *` just with 6 additional columns added to it. Further SQL DML can be added after the macro call, such as ORDER BY and GROUP BY.

There are two internally calculated date values based on when the table is created and, for an incremental model, when data was inserted.

```sql
WITH my_cte AS (...)
{% raw %}
{{ dbt_audit(
    cte_ref="my_cte", 
    created_by="@gitlab_user1", 
    updated_by="@gitlab_user2", 
    created_date="2019-02-12", 
    updated_date="2020-08-20"
) }}
{% endraw %}
ORDER BY updated_at
```

{% enddocs %}

{% docs distinct_source %}
This macro is used for condensing a `source` CTE into unique rows only. Our ETL runs quite frequently while most rows in our source tables don't update as frequently. So we end up with a lot of rows in our RAW tables that look the same as each other (except for the metadata columns with a leading underscore). This macro takes in a `source_cte` and looks for unique values across ALL columns (excluding airflow metadata.)

This macro **is specific** to pgp tables (gitlab_dotcom, version, license) and should not be used outside of those. Specifically, it makes references to 2 airflow metadata columns:
* `_uploaded_at`: we only want the *minimum* value per unique row ... AKA "when did we *first* see this unique row?" This macros calls this column `valid_from` (to be used in the SCD Type 2 Macro)
* `_task_instance`: we want to know the *maximum* task instance (what was the last task when we saw this row?). This is used later to infer whether a `primary_key` is still present in the source table (as a roundabout way to track hard deletes)

{% enddocs %}


{% docs get_column_values_ordered %}
This macro retrieves the column values for a references table and allows you to order the column values once fetched.
{% enddocs %}

{% docs get_date_id %}
This creates a conformed date_id for use in with the date dimension in common. This macro should always be used when the output for a column is meant to join with date_id in the date dimension. This macro does not include an alias so an alias must always be applied. 
{% enddocs %}

{% docs get_date_pt_id %}
The same as get_date_id, but includes a conversion to the Pacific timezone for use in facts. 
{% enddocs %}

{% docs get_keyed_nulls %}

This macro generates a key for facts with missing dimensions so when the fact table is joined to the dimension it joins to a record that says it's unknown as in

```sql
SELECT * 
FROM DIM_GEO_AREA 
WHERE DIM_GEO_AREA_ID = MD5(-1);
```

which has:

```
***************************[ 1 ]***************************
DIM_GEO_AREA_ID    | 6bb61e3b7bce0931da574d19d1d82c88
GEO_AREA_NAME      | Missing geo_area_name
```

Generally this should be used when creating and keying on new dimensions that might not be fully representing in the referencing tables
{% enddocs %}

{% docs monthly_change %}
This macro calculates differences for each consecutive usage ping by uuid.
{% enddocs %}

{% docs hash_diff %}

Built for use in data pumps, this macro is inserted at the end of the model, before the `dbt_audit` macro, and adds two columns to the model. 

1. `prev_hash` - the hashed value from designated columns using `dbt_utils.generate_surrogate_key()` from the last dbt run
2. `last_changed` - the timestamp of the last dbt run where the new hashed values didn't match the previous hashed values

In order to do this it requires three arguments

1. the source cte name
2. a cte name to return (usually to use in the `dbt_audit macro`)
3. a **list** of columns to hash and compare for changes

Example: 

```sql
WITH my_cte AS (...)
{% raw %}
{{ hash_diff(
  cte_ref="my_cte",
  return_cte="final",
  columns=[
    'col1',
    'col2',
    'col3'
    ]
) }}
{% endraw %}
```

In the above example this macro would query the `test_data` cte in the referencing model, create and compare a hash for `col1`, `col2`, and `col3`, and name the resulting cte `final` for reference in the `dbt_audit` macro.

{% enddocs %}


{% docs monthly_is_used %}
This macro includes the total counts for a given feature's usage cumulatively.
{% enddocs %}

{% docs null_negative_numbers %}
This macro takes in either a number or varchar field, converts it to a number, and then NULLs out the value if it is less than zero or shows the original value if it is greater than zero. 
{% enddocs %}

{% docs query_comment %}
Defines the format for how comments are added to queries. See [dbt documentation](https://docs.getdbt.com/docs/building-a-dbt-project/dbt-projects/configuring-query-comments/).
{% enddocs %}

{% docs scd_latest_state %}
This macro pick up the latest state when data is ingested as SCD type. For this purpose, to ensure we load only the latest state of data, will use `_task_instance` column as a criteria (this can be redefined). Currently we do a `max` on the defined `_task_instance` column, so its important to take a proper column to select.
Example, if we have data records in the `RAW` layer like:

| ID | DATE |
| ---- | ---- |
|1| `2022-01-01`|
|1| `2022-02-02`|
|1| `2022-03-28`|

using this macro (and treating `DATE` as the `_task_instance`) should reflect and show in the `PREP` layer as:

| ID | DATE |
| ---- | ---- |
|1| `2022-03-28`|

as this record is the recent one. We need this approach for some specific use case.

{% enddocs %}

{% docs scd_type_2 %}
This macro inserts SQL statements that turn the inputted CTE into a [type 2 slowly changing dimension model](https://en.wikipedia.org/wiki/Slowly_changing_dimension#Type_2:_add_new_row). According to [Orcale](https://www.oracle.com/webfolder/technetwork/tutorials/obe/db/10g/r2/owb/owb10gr2_gs/owb/lesson3/slowlychangingdimensions.htm), "a Type 2 SCD retains the full history of values. When the value of a chosen attribute changes, the current record is closed. A new record is created with the changed data values and this new record becomes the current record. Each record contains the effective time and expiration time to identify the time period between which the record was active."

In particular, this macro adds 3 columns: `valid_from`, `valid_to`, and `is_currently_valid`. It does not alter or drop any of the existing columns in the input CTE.
* `valid_from` will never be null
* `valid_to` can be NULL for up to one row per ID. It is possible for an ID to have 0 currently active rows (implies a "Hard Delete" on the source db)
* `is_currently_active` will be TRUE in cases where `valid_to` is NULL (for either 0 or 1 rows per ID)

The parameters are as follows:
  * **primary_key_renamed**: The primary key column from the `casted_cte`. According to our style guide, we usually rename primary keys to include the table name ("merge_request_id")
  * **primary_key_raw**: The same column as above, but references the column name from when it was in the RAW schema (usually "id")
  * **source_cte**: (defaults to '`distinct_source`). This is the name of the CTE with all of the unique rows from the raw source table. This will always be `distinct_source` if using the `distinct_source` macro.
  * **casted_cte**: (defaults to `renamed`). This is the name of the CTE where all of the columns have been casted and renamed. Our internal convention is to call this `renamed`. This CTE needs to have a column called `valid_from`.

This macro does **not** reference anything specific to the pgp data sources, but was built with them in mind. It is unlikely that this macro will be useful to anything outside of pgp data sources as it was built for a fairly specific problem. We would have just used dbt snapshots here except for the fact that they currently don't support hard deletes. dbt snapshots should be satisfactory for most other use cases.

This macro was built to be used in conjunction with the distinct_source macro.

{% enddocs %}


{% docs schema_union_all %}
This macro takes a schema prefix and a table name and does a UNION ALL on all tables that match the pattern. The exclude_part parameter defaults to 'scratch' and all schemas matching that pattern will be ignored. Optionally, an integer representing a limit on the number of days to be included can be passed to limit the schemas that are included in the union.  This only limits to the month of the day represented by the integer, to filter to the precise day limit use `schema_union_limit` macro.

If the user would like to test their changes with local versions of the unioned tables (i.e. those in their `USER_PREP`/`USER_PROD` schemas), they can apply a variable at run time in the terminal to pull the data from there instead of production. The variable defined in this macro is `local` will pull from local copies if it is not null when added in the terminal. Ex. `dbt run --m your_model_name --vars '{"local": "yes"}'`.
{% enddocs %}


{% docs schema_union_limit %}
This macro takes a schema prefix, a table name, a column name, and an integer representing days. It returns a view that is limited to the last 30 days based on the column name. Note that this also calls schema union all which can be a heavy call.
{% enddocs %}

{% docs simple_cte %}
Used to simplify CTE imports in a model.

A large portion of import statements in a SQL model are simple `SELECT * FROM table`. Writing pure SQL is verbose and this macro aims to simplify the imports.

The macro accepts once argument which is a list of tuples where each tuple has the alias name and the table reference.

Below is an example and the expected output:

```sql
{% raw %}
{{ simple_cte([
    ('map_merged_crm_account','map_merged_crm_account'),
    ('zuora_account','zuora_account_source'),
    ('zuora_contact','zuora_contact_source')
]) }}

, excluded_accounts AS (

    SELECT DISTINCT
      account_id
    FROM {{ref('zuora_excluded_accounts')}}

)
{% endraw %}
```

```sql
WITH map_merged_crm_account AS (

    SELECT * 
    FROM "PROD".common.map_merged_crm_account

), zuora_account AS (

    SELECT * 
    FROM "PREP".zuora.zuora_account_source

), zuora_contact AS (

    SELECT * 
    FROM "PREP".zuora.zuora_contact_source

)

, excluded_accounts AS (

    SELECT DISTINCT
      account_id
    FROM "PROD".legacy.zuora_excluded_accounts

)
```

{% enddocs %}

{% docs generate_single_field_dimension %}
Convenience macro created to assist in the creation of new Dimensions based off a single source field. This returns the compiled SQL for selecting from the Source model   
{% enddocs %}

{% docs convert_variant_to_number_field %}
This macro takes in a variant field, converts it to a varchar field and then to a Number field with lower case values. 
{% enddocs %}

{% docs rolling_window_delete %}
This macro standardizes the syntax for adding a `DELETE` post-hook for rolling window models. 
{% enddocs %}

{% docs only_force_full_refresh %}
This macro is for setting the `full_refresh` property of an incremental model to only allow forced full refreshes and is usually combined with an `on_schema_change` configuration.  A forced full refresh requires an additional variable, `full_refresh_force` to be passed at run time in addition to the `--full-refresh` flag.

```
{% raw %}
{{
  config(
    materialized = 'incremental', 
    unique_key = 'unique_key',
    on_schema_change='append_new_columns',
    full_refresh=only_force_full_refresh()
  )
}}
{% endraw %}
```


```
dbt run -s my_model
→ incremental
dbt run -s my_model --full-refresh
→ incremental
dbt run -s my_model --full-refresh --vars '{"full_refresh_force": true}'
→ full refresh
```
{% enddocs %}



{% docs orphaned_db_table_check %}

The operation will output, to the log file and terminal window, a list of database tables that do not match to model or source in the dbt code base.

Example Usage:
dbt run-operation orphaned_db_table_check
dbt run-operation orphaned_db_table_check --args "{databases: ['raw']}"

Arguments:
  databases: The list of databases to retrieve the table names from

Known exceptions and not yet implemented source tables were identified from observation.

{% enddocs %}

{% docs missing_member_column %}

This macro will generate a 'missing member' record which can be added to any dimension to handle late arriving dimensions in fact tables. It constructs an 'Unknown' record with null or unknown dimension attributes to maintain join integrity to facts models.

{% enddocs %}

{% docs union_tables %}

The `union_tables` macro is derived from the `dbt_utils.union_relations` macro. It is designed to handle unions of tables with unequal schemas, supporting WHERE clauses and individual table filters, then combining all results using UNION. It constructs SQL SELECT statements for each specified table, ensuring correct column alignment and handling missing columns by filling them with NULL values. It allows for optional column overrides, inclusion or exclusion of specific columns, and the addition of a source column to identify the originating table. The macro supports WHERE clauses and individual table filters. Example usage below:

    union_tables(
        relations=[
            ref('sales_jan'),
            ref('sales_feb'),
            ref('sales_mar')
        ],
        column_override={'sales_amount': 'FLOAT'},
        include=['sale_id', 'product_id', 'sales_amount', 'sale_date'],
        exclude=['discount_code'],
        source_column_name='_source_table',
        where='is_active = true',
        filters={
            'sales_jan': 'region = "North"',
            'sales_feb': 'region = "South"'
        }
    )

This macro also casts numeric columns to NUMBER to avoid overflow issues.

{% enddocs %}
