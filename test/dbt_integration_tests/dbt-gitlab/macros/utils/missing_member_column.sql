{%- macro missing_member_column(primary_key, referential_integrity_columns=[], not_null_test_cols=[]) -%}
    {%- if not execute -%}
        {{ return('') }}
    {% endif %}

    {%- set columns = adapter.get_columns_in_relation(this) -%}
    {%- set referential_integrity_columns = referential_integrity_cols|list -%}
    {%- set not_null_test_columns = not_null_test_cols|list -%}
    {%- set target = this -%}

    {%- set source %}
    (
        SELECT DISTINCT
          {%- for col in columns %}
            {% if col.name|lower == primary_key %}
            MD5('-1') AS {{ col.name|lower }}
            -- if the column is part of a relationship test, set it to -1 so this test does not fail
            {% elif col.name|lower in referential_integrity_columns|lower %}
            MD5('-1') AS {{ col.name|lower }}
            -- if the column is part of a not null test, set it to 0 so this test does not fail
            {% elif col.name|lower in not_null_test_columns|lower %}
            '0' AS {{ col.name|lower }}
            -- if the column contains "_ID" this indicates it is an id so assign it "-1" or the hash of -1
            {% elif ('_ID' in col.name|string and col.data_type.startswith('character varying')) %}
            MD5('-1') AS {{ col.name|lower }}
            {% elif ('_ID' in col.name|string and col.data_type.startswith('numeric')) %}
            -1 AS {{ col.name|lower }}
            {% elif col.name|string == 'IS_DELETED' %}
            '0' AS {{ col.name|lower }}
            -- boolean
            {% elif col.data_type == 'BOOLEAN' %}
            NULL AS {{ col.name|lower }}
            -- strings
            {% elif (col.data_type.startswith('character varying') and col.string_size() >= ('Missing '+ col.name)|length) %}
            'Missing ' || '{{ col.name|lower }}' AS {{ col.name|lower }}
            {% elif (col.data_type.startswith('character varying') and col.string_size() < ('Missing '+ col.name)|length) %}
            'Missing' AS {{ col.name|lower }}
            -- dates
            {% elif col.data_type == 'DATE' %}
            '9999-01-01' AS {{ col.name|lower }}
            {% elif col.data_type == 'DATETIME' %}
            '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }}
            {% elif col.data_type == 'TIMESTAMP_TZ' %}
            '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }}
            {% elif col.data_type == 'TIMESTAMP_NTZ' %}
            '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }}
            {% elif col.data_type == 'TIMESTAMP' %}
            '9999-01-01 00:00:00 +0000' AS {{ col.name|lower }}
            {% elif col.data_type == 'TIMESTAMP_LTZ' %}
            '9999-01-01 00:00:00.000 +0000' AS {{ col.name|lower }}
            -- numeric
            {% elif col.data_type.startswith('NUMBER') %}
            NULL AS {{ col.name|lower }}
            -- catch all for everything else
            {% else %}
            NULL AS {{ col.name|lower }}
            {%- endif %}
            {%- if not loop.last %}
            ,
            {%- endif %}
          {%- endfor %}
      FROM {{ this }}
      LIMIT 1
    )
    {%- endset %}

  {{ get_merge_sql(target, source, primary_key, columns, predicates=none) }}

{%- endmacro -%}
