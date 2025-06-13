{% macro orphaned_db_table_check(databases=['prod','prep']) %}

{% if (databases is not string and databases is not iterable) or databases is mapping or databases|length <=0  %}
  {% do exceptions.raise_compiler_error('databases must be a list') %}
{% endif %}
  

  {% call statement('get_tables', fetch_result=True) %}
    SELECT
      table_catalog,
      table_schema,
      table_name,
      table_type
    FROM (
      {%- for database in databases -%}
      SELECT
        t.table_catalog,
        t.table_schema,
        t.table_name,
        t.table_type
      FROM {{ database }}.information_schema.tables t
      WHERE t.table_type IN ('BASE TABLE', 'VIEW')
      {% if not loop.last %}
      UNION
      {% endif %}
      {%- endfor -%}
    )
    WHERE LOWER(table_name) NOT IN (
      {%- for node in graph.nodes.values() | selectattr("resource_type", "equalto","model") | list
        + graph.nodes.values() | selectattr("resource_type", "equalto","seed") | list
        + graph.sources.values() | selectattr("resource_type", "equalto","source") | list
      %}
      '{{ node.identifier or node.alias or node.name }}'{% if not loop.last %}, {% endif %}        
      {%- endfor -%}
    )
    -- Known exceptions
    AND table_schema NOT LIKE 'SNOWPLOW_%'
    AND table_schema NOT LIKE 'DOTCOM_USAGE_EVENTS_%'
    AND table_schema != 'INFORMATION_SCHEMA'
    AND table_schema != 'BONEYARD'
    AND table_schema != 'TDF'
    AND table_schema != 'CONTAINER_REGISTRY'
    AND table_schema != 'FULL_TABLE_CLONES'
    AND table_schema != 'QUALTRICS_MAILING_LIST'
    -- Not yet implemented models
    AND table_schema != 'NETSUITE_FIVETRAN'
    ORDER BY 1,2,3
  {% endcall %}

  {% set output = [] %}

  {% for to_check in load_result('get_tables')['data'] %}
    {% set check_relation = adapter.get_relation(
      database=to_check[0],
      schema=to_check[1],
      identifier=to_check[2]
    ) %}
    {% if check_relation %}
      {#% do log( check_relation, info=true ) %#}
      {% do output.append(check_relation | string) %}
    {% endif %}   
  {% endfor %}

  {% do log("Number of Orphaned Tables: " ~ output | length, info=True ) %}
  {% do log("Orphaned Tables:\n" ~ toyaml(output), info=True ) %}

{% endmacro %} 
