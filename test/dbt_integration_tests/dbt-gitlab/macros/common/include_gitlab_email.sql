{%- macro include_gitlab_email(column_name) -%}

    CASE
      WHEN {{ column_name }}  IS NULL THEN 'Exclude'
      WHEN {{ column_name }} LIKE '%-%'        THEN 'Exclude'   -- removes any emails with special character - 
      WHEN {{ column_name }} LIKE '%~%'        THEN 'Exclude'    -- removes emails with special character ~
      WHEN {{ column_name }} LIKE '%+%'        THEN 'Exclude'    -- removes any emails with special character + 
      WHEN {{ column_name }} LIKE '%admin%'    THEN 'Exclude'-- removes records with the word admin
      WHEN {{ column_name }} LIKE '%hack%'     THEN 'Exclude'-- removes hack accounts
      WHEN {{ column_name }} LIKE '%xxx%'      THEN 'Exclude'-- removes accounts with more than three xs
      WHEN {{ column_name }} LIKE '%gitlab%'   THEN 'Exclude'-- removes accounts that have the word gitlab
      WHEN {{ column_name }} LIKE '%test%'     THEN 'Exclude'-- removes accounts with test in the name
      WHEN {{ column_name }}  IN (           -- removes duplicate emails 
                                'mckai.javeion',
                                'deandre',
                                'gopals',
                                'kenny',
                                'jason'
                              )             THEN 'Exclude'
      ELSE 'Include' END                                             

{%- endmacro -%}

