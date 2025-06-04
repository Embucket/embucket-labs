{% macro generate_warehouse_name(custom_warehouse_size, node) -%}

    {%- set production_targets = production_targets() -%}
    {%- set is_backfill = var('is_backfill', none) -%}

    {#
        Definitions:
            - custom_warehouse_size: size of the warehsoue provided at the time of macro call
            - target.name: name of the target (dev for local development, prod for production, etc.)
            - target.warehouse: warehouse provided by the target defined in profiles.yml

        Assumptions:
            - dbt users will have access to the DEV colletion of warehouses

        This macro is hard to test, but here are some test cases and expected output.
        (custom_warehouse_size, target.name, target.warehouse) = <output>


        (XL, prod, TRANSFORMING_XS) = TRANSFORMING_XL
        (XL, ci, DEV_XS) = DEV_XL
        (XL, dev, DEV_XS) = DEV_XL
    #}

    {%- if custom_warehouse_size is none or is_backfill is not none -%}

        {{ target.warehouse }}

    {%- else -%}

        {%- set prefix = 'TRANSFORMING_' if target.name in ('prod','docs') else 'DEV_' -%}

        {{ prefix }}{{ custom_warehouse_size | trim }}

    {%- endif -%}

{%- endmacro %}
