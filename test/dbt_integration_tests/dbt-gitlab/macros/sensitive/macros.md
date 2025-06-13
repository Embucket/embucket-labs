{% docs get_meta_columns %}
This macro will fetch and return the column names which have the passed `meta_key` equal to True. It reads the graph and can fetch column data from models and sources. 
{% enddocs %}

{% docs hash_of_column %}
This macro hashes the column passed in with an obscured salt that it derives from the `get_salt` macro.  The final salt is calculated by encrypting the return value of `get_salt` with a secret password and a constant initialization vector.  This encryption enables the true salt to be hidden in query logs while still being consistent between tables.
{% enddocs %}

{% docs hash_of_column_in_view %}
This macro hashes the column passed in with an obscured salt that it derives from the `get_salt` macro.  This only differs from the `hash_of_column` macro because it puts the final salt in plain-text in the SQL.  Using `encrypt_raw` was making views unusable, so using this macro is the work around.  This should only be used in secure views to not expose the salt in query text.
{% enddocs %}

{% docs hash_sensitive_columns %}
This macro calls the `get_meta_columns` macro to fetch the columns to be hashed. By default it is looking for the key `sensitive`. It iterates through the columns and performs a sha2 hash on them using the `hash_of_column` macro. It then selects the remaining columns from the provided source table and does a select star without the hashed columns.

This is meant to be used in parallel with `nohash_sensitive_columns`.
{% enddocs %}


{% docs nohash_sensitive_columns %}
This macro calls the `get_meta_columns` macro to fetches the columns sensitive columns. It takes the `join_key` passed and hashes it with sha2 using the `hash_of_column` macro. This is so data can be joined when `hash_sensitive_columns` is used. It then iterates through the sensitive columns and prints them out with no change.
{% enddocs %}

{% docs get_salt %}
This macro returns a salt from the environment variables based on the name of the column provided.
{% enddocs %}
