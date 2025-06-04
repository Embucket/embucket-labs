{% macro gdpr_bulk_delete() %}

{%- call statement('gdpr_deletions', fetch_result=True) %}
    select SHA2(TRIM(LOWER(email_address))), delete_type
    from {{ source('driveload', 'gdpr_delete_requests') }}
{%- endcall -%}

    {%- set value_list = load_result('gdpr_deletions') -%}

    {%- if value_list and value_list['data'] -%}

      {%- set values = value_list['data'] %}

      {% for data_row in values %}
        {% if data_row[1] == 'full' %}
        {{ gdpr_delete(data_row[0], run_queries='True')}}
        {% endif %}
        {% if data_row[1] == 'gitlab' %}
        {{ gdpr_delete_gitlab_dotcom(data_row[0], run_queries='True')}}
        {% endif %}

        {%- call statement('remove_data', fetch_result=True) %}
            DELETE FROM {{ source('driveload', 'gdpr_delete_requests') }}
            WHERE  SHA2(TRIM(LOWER(email_address))) = '{{data_row[0]}}'
        {%- endcall -%}

      {% endfor %}
    {%- endif -%}

{%- endmacro -%}
