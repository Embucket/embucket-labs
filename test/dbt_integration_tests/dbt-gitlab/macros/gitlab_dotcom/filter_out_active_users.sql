{%- macro filter_out_active_users(table_to_filter, user_id_column_name) -%}

    NOT EXISTS (

        SELECT 1
        FROM {{ ref('gitlab_dotcom_users_source') }} users_source
        WHERE users_source.state = 'active' 
          AND users_source.user_id = {{table_to_filter}}.{{user_id_column_name}}

    )

{%- endmacro -%}

