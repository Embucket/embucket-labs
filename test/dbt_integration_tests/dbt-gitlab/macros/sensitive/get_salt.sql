{% macro get_salt(column_name) %}

    {%- if 'name' in column_name -%}
        {{ return( env_var("SALT_NAME") ) }}
    {%- elif 'email' in column_name -%}
        {{ return( env_var("SALT_EMAIL") ) }}
    {%- elif 'ip' in column_name -%}
        {{ return( env_var("SALT_IP") ) }}
    {%- else -%}
        {{ return( env_var("SALT") ) }}
    {%- endif -%}

{% endmacro %}