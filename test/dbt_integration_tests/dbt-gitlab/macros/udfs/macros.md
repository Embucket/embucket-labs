{% docs crc32 %}
UDF stands for User-defined functions. They let you extend the system to perform operations that are not available through the built-in, system-defined functions provided by Snowflake. Snowflake currently supports two types of UDFs: SQL and JavaScript.

This runs 32-bit cyclic redundancy check.  This is required because the Flipper gem's feature flag makes rollouts based on CRC32.
{% enddocs %}

{% docs create_udfs %}
UDF stands for User-defined functions. They let you extend the system to perform operations that are not available through the built-in, system-defined functions provided by Snowflake. Snowflake currently supports two types of UDFs: SQL and JavaScript.

This macro is inspired by [this discourse post](https://discourse.getdbt.com/t/using-dbt-to-manage-user-defined-functions-redshift/18) on using dbt to manager UDFs.
{% enddocs %}

{% docs regexp_substr_to_array %}
UDF stands for User-defined functions. They let you extend the system to perform operations that are not available through the built-in, system-defined functions provided by Snowflake. Snowflake currently supports two types of UDFs: SQL and JavaScript.

This UDF returns the all substrings that match a regular expression in an array  ("regex_text") within a string ("input_text") If no match is found, returns an empty array.
{% enddocs %}

{% docs sfdc_id_15_to_18 %}
UDF stands for User-defined functions. They let you extend the system to perform operations that are not available through the built-in, system-defined functions provided by Snowflake. Snowflake currently supports two types of UDFs: SQL and JavaScript.

This converts SFDC account ids that are 15 characters long to 18 characters.
{% enddocs %}