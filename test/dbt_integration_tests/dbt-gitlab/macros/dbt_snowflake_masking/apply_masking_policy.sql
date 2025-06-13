{%- macro apply_masking_policy(database, schema, table, column_policies) -%}

{% if column_policies is not iterable %}
  {% do exceptions.raise_compiler_error('"column_policies" must be a list')%}
{% endif %}

{% if database is not string or schema is not string or table is not string %}
  {% do exceptions.raise_compiler_error('"database", "schema", and "table" must be a string')%}
{% endif %}

{#
Masking policies are data type specific so the Snowfake data type
must be retrieved to properly constrict the masking policy.
#}
{%- set column_data_type_query -%}
SELECT
  t.table_catalog,
  t.table_schema,
  t.table_name,
  CASE t.table_type
    WHEN 'BASE TABLE' THEN 'TABLE'
    ELSE t.table_type
  END AS table_type,
  c.column_name,
  c.data_type
FROM "{{ database }}".information_schema.tables t
INNER JOIN "{{ database }}".information_schema.columns c
  ON c.table_schema = t.table_schema
  AND c.table_name = t.table_name
WHERE t.table_catalog =  '{{ database.upper() }}' 
  AND t.table_type IN ('BASE TABLE', 'VIEW')
  AND t.table_schema = '{{ schema.upper() }}' 
  AND t.table_name = '{{ table.upper() }}'
;

{%- endset -%}

{% set column_info = dict()  %}


{%- if execute -%}

  {# {% do log(column_policies, info=true) %} #}

  {% if column_policies %}

    {%- set result = run_query(column_data_type_query) %}

    {%- for policy in column_policies  -%}

      {%- for row in result.rows if row['COLUMN_NAME'] == policy['COLUMN_NAME'] -%} 

        {{ create_masking_policy(row['TABLE_CATALOG'], row['TABLE_SCHEMA'], row['DATA_TYPE'], policy['POLICY_NAME']) }}

        {{ set_masking_policy(row['TABLE_CATALOG'], row['TABLE_SCHEMA'], row['TABLE_NAME'], row['TABLE_TYPE'], row['COLUMN_NAME'], row['DATA_TYPE'], policy['POLICY_NAME']) }}

      {%- endfor -%}    

    {%- endfor -%}

  {% endif %}

{%- endif -%}

{%- endmacro -%}
