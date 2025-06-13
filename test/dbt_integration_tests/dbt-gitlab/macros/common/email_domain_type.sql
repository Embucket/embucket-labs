{% macro email_domain_type(email_domain, lead_source) %}

{%- set personal_email_domains_partial_match = get_personal_email_domain_list('partial_match') -%}
{%- set personal_email_domains_full_match = get_personal_email_domain_list('full_match') -%}


CASE
  WHEN {{lead_source}} IN ('DiscoverOrg', 'Zoominfo', 'Purchased List', 'GitLab.com') THEN 'Bulk load or list purchase or spam impacted'
  WHEN TRIM({{email_domain}}) IS NULL THEN 'Missing email domain'

  WHEN {{email_domain}} LIKE ANY (
    {%- for personal_email_domain in personal_email_domains_partial_match -%}
    '%{{personal_email_domain}}%' {%- if not loop.last -%}, {% endif %}
    {% endfor %}
  )

  OR {{email_domain}} IN (
    {%- for personal_email_domain in personal_email_domains_full_match -%}
    '{{personal_email_domain}}' {%- if not loop.last -%}, {% endif %}
    {% endfor %}
  )

  THEN 'Personal email domain'
  ELSE 'Business email domain'
END

{% endmacro %}
