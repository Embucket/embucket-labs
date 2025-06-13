{% macro zuora_excluded_accounts() %}

    SELECT DISTINCT account_id
    FROM {{ref('zuora_excluded_accounts')}}
    WHERE account_id IS NOT NULL

 {% endmacro %}
