{% macro parse_bigquery_object_array() %}

{%- set production_targets = production_targets() -%}
{%- set db_prep = env_var("SNOWFLAKE_PREP_DATABASE") -%}
{%- set db_prod = env_var("SNOWFLAKE_PROD_DATABASE") -%}
{%- set production_databases = [db_prep, db_prod] -%}

{% for db in production_databases %}
    {%- if target.name in production_targets -%}

    CREATE OR REPLACE FUNCTION "{{ db | trim }}".{{target.schema}}.parse_bigquery_object_array(V VARIANT)

    {%- else -%}

    CREATE OR REPLACE FUNCTION "{{ target.database | trim }}_{{ db | trim }}".{{target.schema}}.parse_bigquery_object_array(V VARIANT)

    {% endif %}
    RETURNS variant
    LANGUAGE JAVASCRIPT
    AS 
    $$
    var result = {};
    for (const x of V) {
    if (x.value){
        for (const [key, value] of Object.entries(x.value)) {
        if ( key != 'set_timestamp_micros') {
            result[x.key] = value;
        }
        }
    } else {
        result[x.key] = null;
    }
    }
    return result
    $$;

    {% endfor %}
{% endmacro %}
