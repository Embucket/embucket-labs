-- Will write to custom schemas not on prod

{% macro generate_schema_name(custom_schema_name, node) -%}

    {%- set production_targets = production_targets() -%}

    {#
        Definitions:
            - custom_schema_name: schema provided via dbt_project.yml or model config
            - target.name: name of the target (dev for local development, prod for production, etc.)
            - target.schema: schema provided by the target defined in profiles.yml
        
        This macro is hard to test, but here are some test cases and expected output.
        (custom_schema_name, target.name, target.schema) = <output>

        In all cases it will now write to the same schema. The database is what's 
        different. See generate_database_name.sql

        (legacy, prod, preparation) = legacy
        (legacy, ci, preparationprod) = legacy
        (legacy, dev, preparation) = legacy
        
        (zuora, prod, preparation) = zuora
        (zuora, ci, preparation) = zuora
        (zuora, dev, preparation) = zuora

    #}

    {%- if custom_schema_name is none -%}

        {{ target.schema.lower() | trim }}

    {%- else -%}

        {{ custom_schema_name.lower() | trim }}

    {%- endif -%}
    
{%- endmacro %}
