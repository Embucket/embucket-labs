{% docs get_opportunity_date_fields %}

This macro generates standardized date-related fields and their corresponding joins for a list of date_id columns from the fct_crm_opportunity table (v2).
For each date_id field provided, the macro generates:

5 standardized date fields (date, month, fiscal quarter date, fiscal quarter name, fiscal year)
The corresponding LEFT JOIN to the dim_date table
Proper SQL formatting and comma handling

Arguments:
date_columns: List of date_id column names from fct_crm_opportunity table


{% enddocs %}

{% docs get_opportunity_flag_fields %}
This macro retrieves the list of columns used for purposes of generating a surrogate key so as to join dim_crm_opportunity_flags.
{% enddocs %}

{% docs get_opportunity_command_plan_fields %}
This macro retrieves the list of command plan fields used for generating a surrogate key to join dim_crm_command_plan. Fields include various command plan attributes like champion, close plan, decision criteria, and value drivers.
{% enddocs %}

{% docs get_opportunity_deal_fields %}
This macro retrieves the list of deal-related fields used for generating a surrogate key to join dim_crm_opportunity_deal. Fields include deal path, size, category, and related deal attributes.
{% enddocs %}

{% docs get_opportunity_source_and_path_fields %}
This macro retrieves the list of source and path-related fields used for generating a surrogate key to join dim_crm_opportunity_source_and_path. Fields include campaign sources, lead sources, sales path, and qualification attributes.
{% enddocs %}

{% docs get_opportunity_partner_fields %}
This macro retrieves the list of partner-related fields used for generating a surrogate key to join dim_crm_opportunity_partner. Fields include partner deal types, tracks, registrations, and various partner relationship attributes.
{% enddocs %}