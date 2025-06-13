{%- macro get_columns_to_mask(resource_type, table=none) -%}

{# 

dbt run-operation get_columns_to_mask --args "{resource_type: 'source', table: 'test_source_table'}" 
dbt run-operation get_columns_to_mask --args "{resource_type: 'source'}" 
dbt run-operation get_columns_to_mask --args "{resource_type: 'model', table: 'test_table'}" 

#}

{% if not (resource_type == 'source' or resource_type == 'model') %}
  {% do exceptions.raise_compiler_error('"resource_type" must be "source" or "model"')%}
{% endif %}

{# 
The models and sources have a different structure in the graph object that
needs to be accounted for in the selection of the proper objects.
#}

{% if resource_type == 'source' %}
  {% set search_path = graph.sources.values() %}
  {% set table_key = 'identifier' %}
{% elif resource_type == 'model' %}
  {% set search_path = graph.nodes.values() %}
  {% set table_key = 'alias' %}
{% endif %}

{% if table %}
  {% set name_test =  'equalto' %}
  {% set name_match =  table.lower() %} {# does this work both source and models? #}
{% else %}
  {% set name_test =  'ne'  %} {# not equal to #}
  {% set name_match =  none  %}
{% endif %}
 
{% set column_policies = []  %}
{% set column_info = dict()  %}

{%- if execute -%}

  {%- for node in search_path
     | selectattr("resource_type", "equalto", resource_type)
     | selectattr("name", name_test, name_match )
 
  -%}
  
     {# {% do log(node.name, info=true) %}  #}

    {%- for column in node.columns.values()
     | selectattr("meta")
    -%}
        {# {% do log(column.meta, info=true) %} #}

        {%- if column.meta['masking_policy'] -%}
        
          
          {% set column_info = ({
            "DATABASE" : node.database.upper(),
            "SCHEMA" : node.schema.upper(),
            "TABLE" : node[table_key].upper(),
            "COLUMN_NAME" : column.name.upper(), 
            "POLICY_NAME" : column.meta['masking_policy'].upper()  
          }) %}
          {% do column_policies.append(column_info) %}

        {%- endif -%}

    {%- endfor -%}
  
  {%- endfor -%}

  {# {% do log(column_policies, info=true) %} #}

  {{ return(column_policies) }}


{%- endif -%}



{%- endmacro -%}
