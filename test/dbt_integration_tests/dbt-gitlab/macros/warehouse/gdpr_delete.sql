{% macro gdpr_delete(email_sha, run_queries=False) %}


        {% set data_types = ('BOOLEAN', 'TIMESTAMP_TZ', 'TIMESTAMP_NTZ', 'FLOAT', 'DATE', 'NUMBER') %}
        {% set exclude_columns = ('dbt_scd_id', 'dbt_updated_at', 'dbt_valid_from', 'dbt_valid_to', '_task_instance', '_uploaded_at', '_sdc_batched_at', '_sdc_extracted_at',
           '_sdc_received_at', '_sdc_sequence', '_sdc_table_version') %}

        {% set set_sql %}
        SET email_sha = '{{email_sha}}';
        {% endset %}
        {{ log(set_sql, info = True) }}

{# DELETE FROM EVERYTHING THAT'S NOT SNAPSHOTS#}
    {%- call statement('gdpr_deletions', fetch_result=True) %}

        WITH email_columns AS (
        
            SELECT 
                LOWER(table_catalog)||'.'||LOWER(table_schema)||'."'||UPPER(table_name)||'"' AS fqd_name,
                LISTAGG(column_name,',') AS email_column_names
            FROM "RAW"."INFORMATION_SCHEMA"."COLUMNS"
            WHERE LOWER(column_name) LIKE '%email%'
                AND table_schema NOT IN ('SNAPSHOTS','SHEETLOAD')
                AND data_type NOT IN {{data_types}}
            GROUP BY 1
        
        )

        SELECT
          fqd_name, 
          email_column_names
        FROM email_columns;

    {%- endcall -%}

    {%- set value_list = load_result('gdpr_deletions') -%}

    {%- if value_list and value_list['data'] -%}

      {%- set values = value_list['data'] %}

      {% for data_row in values %}

        {% set fqd_name = data_row[0] %}
        {% set email_column_list = data_row[1].split(',') %}
      
        {% for email_column in email_column_list %}

            {% set delete_sql %}
                DELETE FROM {{fqd_name}} WHERE SHA2(TRIM(LOWER("{{email_column}}"))) =  '{{email_sha}}';
            {% endset %}
            {{ log(delete_sql, info = True) }}

            {% if run_queries %}
                {% set results = run_query(delete_sql) %}
                {% set rows_deleted = results.print_table() %}
            {% endif %}

        {% endfor %}

      {% endfor %}
    
    {%- endif -%}


{# UPDATE SNAPSHOTS #}
    {%- call statement('update_snapshots', fetch_result=True) %}

        WITH email_columns AS (
        
            SELECT 
                LOWER(table_catalog)||'.'||LOWER(table_schema)||'."'||UPPER(table_name)||'"' AS fqd_name,
                LISTAGG(column_name,',') AS email_column_names
            FROM "RAW"."INFORMATION_SCHEMA"."COLUMNS"
            WHERE LOWER(column_name) LIKE '%email%'
                AND table_schema IN ('SNAPSHOTS')
                AND data_type NOT IN {{data_types}}
                AND LOWER(column_name) NOT IN {{exclude_columns}}
                AND LOWER(column_name) NOT LIKE '%\_at'
                AND LOWER(column_name) NOT LIKE '% %'
            GROUP BY 1
        
        ), non_email_columns AS (

            SELECT 
              LOWER(table_catalog)||'.'||LOWER(table_schema)||'."'||UPPER(table_name)||'"' AS fqd_name,
              LISTAGG(column_name,',') AS non_email_column_names
            FROM "RAW"."INFORMATION_SCHEMA"."COLUMNS" AS a
            WHERE LOWER(column_name) NOT LIKE '%email%'
              AND table_schema IN ('SNAPSHOTS')
              AND data_type NOT IN {{data_types}}
              AND LOWER(column_name) NOT IN {{exclude_columns}}
              AND NOT (LOWER(column_name) LIKE ANY ('%id%', '%\_at', '% %'))
              AND LOWER(column_name) NOT IN {{exclude_columns}}
            GROUP BY 1

        )

        SELECT
          a.fqd_name, 
          a.email_column_names, 
          b.non_email_column_names
        FROM email_columns a
        LEFT JOIN non_email_columns b ON a.fqd_name = b.fqd_name
        where b.non_email_column_names is not null;

    {%- endcall -%}

    {%- set value_list = load_result('update_snapshots') -%}

    {%- if value_list and value_list['data'] -%}

      {%- set values = value_list['data'] %}

      {% for data_row in values %}

        {% set fqd_name = data_row[0] %}
        {% set email_column_list = data_row[1].split(',') %}
        {% set non_email_column_list = data_row[2].split(',') %}
      
        {% for email_column in email_column_list %}

            {% set sql %}
                UPDATE {{fqd_name}} SET
                {% for non_email_column in non_email_column_list -%}
                    {{non_email_column}} =  'GDPR Redacted'{% if not loop.last %}, {% endif %}
                {% endfor %}
                WHERE SHA2(TRIM(LOWER("{{email_column}}"))) =  '{{email_sha}}';

            {% endset %}
            {{ log(sql, info = True) }}

            {% if run_queries %}
                {% set results = run_query(sql) %}
                {% set rows_updated = results.print_table() %}
            {% endif %}

        {% endfor %}

        {% for email_column in email_column_list %}

            {% set email_sql %}
                UPDATE {{fqd_name}} SET
                {% for email_column_inner in email_column_list -%}
                    {{email_column_inner}} =  '{{email_sha}}'{% if not loop.last %}, {% endif %}
                {% endfor %}
                WHERE SHA2(TRIM(LOWER("{{email_column}}"))) =  '{{email_sha}}';

            {% endset %}
            {{ log(email_sql, info = True) }}

            {% if email_sql %}
                {% set results = run_query(email_sql) %}
                {% set rows_updated = results.print_table() %}
            {% endif %}

        {% endfor %}

      {% endfor %}
    
    {%- endif -%}

    {{ log("Removal Complete!", info = True) }}

{%- endmacro -%}
