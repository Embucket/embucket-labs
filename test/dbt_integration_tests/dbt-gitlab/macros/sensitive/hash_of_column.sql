{% macro hash_of_column(column) %}

    sha2(
        TRIM(
            LOWER(
                {{column|lower}} || 
                ENCRYPT_RAW(
                  to_binary('{{ get_salt(column|lower) }}', 'utf-8'), 
                  to_binary('{{ env_var("SALT_PASSWORD") }}', 'HEX'), 
                  to_binary('416C736F4E637265FFFFFFAB', 'HEX')
                )['ciphertext']::VARCHAR 
            )
        )
    ) AS {{column|lower}}_hash,

{% endmacro %}