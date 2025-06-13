{% macro get_personal_email_domain_list(type_of_match) %}

{% set personal_email_domain_query %}
select distinct
personal_email_domain
from {{ref('personal_email_domains')}}
where type_of_match = '{{type_of_match}}'
{% endset %}

{% set results = run_query(personal_email_domain_query) %}

{% if execute %}
{# Return the first column #}
{% set results_list = results.columns[0].values() %}
{% else %}
{% set results_list = [] %}
{% endif %}

{{ return(results_list) }}

{% endmacro %}
