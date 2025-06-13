{%- macro dedupe_source(source_table, source_schema='gitlab_dotcom', condition_column='updated_at', unique_key='id') -%}

{% set source_table_name = source_table %}
{% set source_schema_name = source_schema %}
{% set unique_key_column = unique_key %}

{{ config.set('materialized', 'incremental') }}
{{ config.set('unique_key', unique_key_column) }}

SELECT *
FROM {{ source(source_schema_name, source_table_name) }}
{% if is_incremental() %}

WHERE {{ condition_column }} > (SELECT MAX({{ condition_column }}) FROM {{this}})

{% endif %}
QUALIFY ROW_NUMBER() OVER (PARTITION BY {{ unique_key }} ORDER BY {{ condition_column }} DESC) = 1


{%- endmacro -%}