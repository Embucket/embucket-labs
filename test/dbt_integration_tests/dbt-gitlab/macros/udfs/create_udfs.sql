{% macro create_udfs() %}

{%- set production_targets = production_targets() -%}
{%- set db_prep = env_var("SNOWFLAKE_PREP_DATABASE") -%}
{%- set db_prod = env_var("SNOWFLAKE_PROD_DATABASE") -%}
{%- set production_databases = [db_prep, db_prod] -%}

{%- if target.name in production_targets -%}

    {% for db in production_databases %}
        create schema if not exists "{{ db | trim }}".{{target.schema}};
    {% endfor %}
    {{sfdc_id_15_to_18()}}
    {{regexp_substr_to_array()}}
    {{crc32()}}
    {{parse_bigquery_object_array()}}

{%- else -%}
    
    {# Need to create analytics for gitlab_dotcom models #}
    {% for db in production_databases %}
        create schema if not exists "{{ target.database | trim }}_{{ db | trim}}".{{target.schema}};
    {% endfor %}
    {{sfdc_id_15_to_18()}}
    {{regexp_substr_to_array()}}
    {{crc32()}}
    {{parse_bigquery_object_array()}}

{%- endif -%}


{% endmacro %}
