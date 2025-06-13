{% docs samples %}
This macro contains a yml list of model tables to be sampled, it returns that list as a python dictionary.  This macro is used in conjunction with the `create_sample_tables` operation and an override in the `ref` function to create and use sample tables.

Definitions:
  - samples_dict: An output variable of data type dictionary

Example usage
{% raw %}
```yml
samples:
  - name: date_details_source
    clause: '{{ sample_table(3) }}'
  - name: date_details
    clause: "WHERE date_actual >= DATEADD('day', -30, CURRENT_DATE())"
```
{% endraw %}

{% enddocs %}

{% docs sample_table %}
This macro returns a consistent `SAMPLE` command that will yield deterministic results based on the input percent of table to sample

Definitions:
  - percent: An input of data type NUMBER. And represents the percent of the table to be returned in the sample

Example usage
{% raw %}
```sql
SELECT *
FROM {{ ref('dim_date') }} {{ sample_table(3) }}
```
{% endraw %}

{% raw %}
```yml
samples:
  - name: date_details_source
    clause: '{{ sample_table(3) }}'
```
{% endraw %}
{% enddocs %}

{% docs is_table_sampled %}
This macro tests the conditions for sampling a table.  The conditions are: is the target not a production environment, is the model in the sample list, and is the sample variable set.

Definitions:
  - model_name: An input of data type string that is the name of the relation to be sampled.

{% enddocs %}

{% docs get_sample_relation %}
This macro returns the sample relation name.

Definitions:
  - model_name: An input of data type string that is the name of the relation to be sampled.
  - sample_relation: An output of data type relation.

{% enddocs %}

{% docs create_sample_tables %}
The macro is to be run as an operation to construct sample tables based on the list defined in the `samples` macro.  It loops the the sample list and calls the `generate_sample_table_sql` macro to construct the SQL which is then executed.

{% enddocs %}

{% docs generate_sample_table_sql %}
This macro generates the SQL to constrict a sample table based ont the configuration nin the `samples` macro.

{% enddocs %}