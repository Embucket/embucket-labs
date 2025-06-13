{%- macro alliance_partner(fulfillment_partner_name, partner_account_name, close_date, resale_partner_track, partner_track, deal_path, is_focus_partner) -%}

CASE
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%google%' OR LOWER({{ partner_account_name }}) LIKE '%google%'
    THEN 'Google Cloud' 
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE ANY ('%aws%', '%amazon%') OR LOWER({{ partner_account_name }}) LIKE ANY ('%aws%', '%amazon%')
    THEN 'Amazon Web Services'
  WHEN LOWER({{ fulfillment_partner_name }}) LIKE '%ibm (oem)%' OR LOWER({{ partner_account_name }}) LIKE '%ibm (oem)%'
    THEN 'IBM (OEM)'
  WHEN {{ close_date }} >= '2022-02-01' AND NOT EQUAL_NULL({{ resale_partner_track }}, 'Technology') AND NOT EQUAL_NULL({{ partner_track }}, 'Technology') AND {{ deal_path }} = 'Partner'
    THEN 'Channel Partners'
  WHEN {{ close_date }} < '2022-02-01' AND ( {{ fulfillment_partner_name }} IS NOT NULL OR {{ partner_account_name }} IS NOT NULL )
    THEN 'Non-Alliance Partners'
  WHEN {{ is_focus_partner }} = TRUE 
    THEN 'Channel Focus Partner'
  ELSE 'Other Alliance Partners'
END

{%- endmacro -%}
