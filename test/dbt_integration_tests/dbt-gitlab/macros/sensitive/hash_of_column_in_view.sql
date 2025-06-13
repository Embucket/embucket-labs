{% macro hash_of_column_in_view(column) %}

    {% set salt_query %}
        select ENCRYPT_RAW(
            to_binary('{{ get_salt(column|lower) }}', 'utf-8'), 
            to_binary('{{ env_var("SALT_PASSWORD") }}', 'HEX'), 
            to_binary('416C736F4E637265FFFFFFAB', 'HEX')
        )['ciphertext']::VARCHAR
    {% endset %}

    {% set results = run_query(salt_query) %}

    sha2(
        TRIM(
            LOWER(
                {{column|lower}} ||  '{{results.columns[0].values()[0]}}'
            )
        )
    ) AS {{column|lower}}_hash,

{% endmacro %}