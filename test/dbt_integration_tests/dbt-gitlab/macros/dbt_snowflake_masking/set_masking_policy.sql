
{%- macro set_masking_policy(database, schema, table_name, table_type, column_name, data_type, policy ) -%}

ALTER {{table_type}} "{{database}}".{{schema}}.{{table_name}} 
MODIFY COLUMN {{ column_name }} 
SET MASKING POLICY "{{database}}".{{schema}}.{{ policy }}_{{ data_type }};
        
{%- endmacro -%}