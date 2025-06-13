{%- macro partner_marketing_task_subject_cleaning(column_1) -%}

  CASE 
    WHEN contains({{ column_1 }}, 'Microsite Program') 
      THEN 'Microsite Program'
    WHEN contains({{ column_1 }}, 'Free Trial Program')
      THEN 'Free Trial Program'
    WHEN trim({{ column_1 }}) = 'GitLab Dedicated Landing Pages'
      THEN 'GitLab Dedicated Landing Pages'
    WHEN trim({{ column_1 }}) = 'Partner Concierge Program'
      THEN 'Partner Concierge Program'
    ELSE NULL
  END  

{%- endmacro -%}