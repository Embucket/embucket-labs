{%- macro schema_union_all(schema_part, table_name, exclude_part='scratch', database_name=none, day_limit=none, boolean_filter_statement=none, excluded_col=[]) -%}

{% set local = var('local', 'no') %}

  {%- if local != 'no' and database is not none -%}

    {%- set database = generate_database_name(database_name) -%}

  {%- elif database_name is not none-%}

    {%- set database = database_name -%}

  {%- else -%}

    {%- set database = target.database -%}

  {%- endif -%}

 {%- set sql_statement -%}

  SELECT DISTINCT 
    '"' || table_schema || '"."' || table_name || '"'                                 AS qualified_name,
    table_schema                                                                      AS table_schema,
    table_name                                                                        AS table_name,
    CASE
      WHEN REGEXP_INSTR(table_name,'\\d{4}_\\d{2}_\\d{2}') > 0 
        THEN 'YYYY_MM_DD'
      WHEN REGEXP_INSTR(table_name,'\\d{4}_\\d{2}') > 0 
        THEN 'YYYY_MM'
    END                                                                               AS table_date_format,
    CASE
      WHEN REGEXP_INSTR(table_schema,'\\d{4}_\\d{2}_\\d{2}') > 0 
        THEN 'YYYY_MM_DD'
      WHEN REGEXP_INSTR(table_schema,'\\d{4}_\\d{2}') > 0 
        THEN 'YYYY_MM'
    END                                                                               AS schema_date_format,
    TO_DATE(REGEXP_SUBSTR(table_name,'\\d{4}_\\d{2}_?\\d{0,2}'),table_date_format)    AS table_date,
    TO_DATE(REGEXP_SUBSTR(table_schema,'\\d{4}_\\d{2}_?\\d{0,2}'),schema_date_format) AS schema_date,
    LISTAGG(column_name, ',') WITHIN GROUP (ORDER BY column_name)                     AS column_names
  FROM "{{ database }}".information_schema.columns
  WHERE table_schema ILIKE '%{{ schema_part }}%'
    AND table_schema NOT ILIKE '%{{ exclude_part }}%'
    AND table_name ILIKE '{{ table_name }}'
    {%- if day_limit %}
    AND COALESCE(table_date,schema_date) >= DATE_TRUNC('month',DATEADD('day',-{{ day_limit }}, CURRENT_DATE()))
    {%- endif -%}
  {{ dbt_utils.group_by(n=7) }}
  ORDER BY 1

  {%- endset -%}

  {%- set value_list = dbt_utils.get_query_results_as_dict(sql_statement) -%}
              
    {%- if value_list and value_list['QUALIFIED_NAME'] -%}

        {%- if excluded_col -%}

          {%- set excluded_fields = [] -%}

          {%- for col in excluded_col -%}
        
            {%- set excluded_fields = excluded_fields.append(col|upper) -%}

          {% endfor %}

        {%- endif -%}
          
        {% for qualified_name, column_names in zip(value_list['QUALIFIED_NAME'], value_list['COLUMN_NAMES']) -%} 

            {%- set columns_names_list = column_names.split(',') -%}

            {%- set columns_without_exclusions =  (columns_names_list | reject('in', excluded_fields) | list) | join(', ')   -%}

              SELECT {{columns_without_exclusions}}
              FROM "{{ database }}".{{ qualified_name }}
              {%- if boolean_filter_statement %}
              WHERE {{ boolean_filter_statement }}
              {%- endif -%}
              {% if not loop.last %}
              UNION ALL
              {% endif -%}
        {% endfor %}

            
    {%- else -%}


        {{ return("/* No models found to union in schema_union_all */") }}

    {%- endif %}


{%- endmacro -%}
