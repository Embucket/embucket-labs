{%- macro alliance_partner_short_current(fulfillment_partner_name, partner_account_name, resale_partner_track, partner_track, deal_path, is_focus_partner) -%}

CASE
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%google%' OR LOWER({{ partner_account_name }}) LIKE '%google%'
    THEN 'GCP' 
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE ANY ('%aws%', '%amazon%') OR LOWER({{ partner_account_name }}) LIKE ANY ('%aws%', '%amazon%')
    THEN 'AWS'
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%ibm (oem)%' OR LOWER({{ partner_account_name }}) LIKE '%ibm (oem)%'
    THEN 'IBM'
  WHEN NOT EQUAL_NULL({{ resale_partner_track }}, 'Technology') AND NOT EQUAL_NULL({{ partner_track }}, 'Technology') AND {{ deal_path }} = 'Partner'
    THEN 'Channel Partners'
  WHEN ( {{ fulfillment_partner_name }} IS NOT NULL OR {{ partner_account_name }} IS NOT NULL )
    THEN 'Non-Alliance'
  WHEN {{ is_focus_partner }} = TRUE 
    THEN 'Channel Focus Partner'
  ELSE 'Other Alliance'
END

{%- endmacro -%}
